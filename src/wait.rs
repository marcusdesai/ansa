//! Provides strategies used by handles when waiting for sequences on the ring buffer.
//!
//! Also provided are three traits for implementing your own wait logic.
//!
//! # Performance
//!
//! Each strategy provides a tradeoff between latency and CPU use, but this is not the only aspect
//! to consider when optimizing performance. The size of the strategy is also an important factor.
//!
//! Each handle struct includes its own instance of the wait strategy used by a single disruptor.
//! Hence, the size of the strategy can have an effect on performance if, for example, its size
//! means the handle may no longer fit in a single cache line.
//!
//! Cache lines are commonly `32`, `64`, or `128` bytes. With `64` bytes being the most common.
//!
//! Presently, when the wait strategy is zero-sized, each handle has the following size (in bytes).
//! It is very likely that these sizes will remain the same, but it is **not** guaranteed.
//!
//! | Handle                                                     | size |
//! |------------------------------------------------------------|------|
//! | [`Consumer`](crate::handles::Consumer)                     | 32   |
//! | [`ExactConsumer`](crate::handles::ExactConsumer)           | 32   |
//! | [`Producer`](crate::handles::Producer)                     | 32   |
//! | [`ExactProducer`](crate::handles::ExactProducer)           | 32   |
//! | [`MultiProducer`](crate::handles::MultiProducer)           | 40   |
//! | [`ExactMultiProducer`](crate::handles::ExactMultiProducer) | 40   |
//!
//! And here are the minimum sizes of the provided strategies, again assuming nested wait
//! strategies are zero-sized. These sizes are also unlikely to change, but this is also **not**
//! guaranteed.
//!
//! | Strategy                      | size |
//! |-------------------------------|------|
//! | [`WaitBusy`]                  | 0    |
//! | [`WaitBusyHint`]              | 0    |
//! | [`WaitYield`]                 | 0    |
//! | [`WaitSleep`]                 | 8    |
//! | [`WaitBlocking`]              | 16   |
//! | [`WaitPhased<W>`](WaitPhased) | 16   |
//! | [`Timeout<W>`](Timeout)       | 8    |
//!
//! Both [`WaitPhased`] and [`Timeout`] can be considerably larger. For example:
//! ```
//! use ansa::wait::*;
//!
//! assert_eq!(size_of::<WaitPhased<WaitBlocking>>(), 32);
//! assert_eq!(size_of::<Timeout<WaitPhased<WaitSleep>>>(), 32);
//! ```
//!
//! By the by, various provided strategies are limited to wait durations of `u64::MAX` nanoseconds,
//! which is done in order to keep them small. Using [`Duration`] instead would double the minimum
//! size of the relevant strategies, which would be wasteful as wait durations are likely to be short.
//! ```
//! use std::time::Duration;
//!
//! assert_eq!(size_of::<Duration>(), 16);
//! assert_eq!(size_of::<u64>(), 8);
//! ```

use crate::handles::Barrier;
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

/// Implement to provide logic which will run inside a wait loop.
///
/// This trait is unsuitable when state needs to be held across loop iterations, since
/// [`waiting`](Waiting::waiting) cannot observe outside of the loop.
///
/// If state or fallibility is required, implement [`WaitStrategy`] or [`TryWaitStrategy`] instead.
///
/// If a type, `T`, implements `Waiting`, then `T` will implement `WaitStrategy`, and
/// `Timeout<T>` will implement `TryWaitStrategy`.
///
/// As a consequence of the above blanket impls, do _not_ implement `Waiting` for a type if you
/// want to provide your own implementations of `WaitStrategy` for `T`, or `TryWaitStrategy`
/// for `Timeout<T>`.
///
/// # Examples
/// ```
/// use ansa::wait::Waiting;
///
/// struct MyWaiter;
///
/// impl Waiting for MyWaiter {
///     fn waiting(&self) {
///         // stuff that will happen on each loop iteration
///     }
/// }
/// ```
pub trait Waiting {
    /// Called on every iteration of the wait loop.
    fn waiting(&self);
}

/// Implement to provide a wait loop which runs as a handle waits for a sequence.
///
/// If a wait strategy does not require either control over loop behaviour, or carrying state
/// across loop iterations, then prefer implementing [`Waiting`] instead, as it provides a safe
/// interface.
///
/// # Safety
///
/// This trait is unsafe as there is no guard against invalid implementations of
/// [`wait`](WaitStrategy::wait) causing Undefined Behaviour. A valid implementation must ensure
/// that the following two conditions holds:
/// 1) `wait` may only return when `barrier sequence >= desired_seq` is true.
/// 2) `wait` must return the last read `barrier sequence`.
///
/// If `wait` does not abide by these conditions, then writes to the ring buffer may overlap with
/// other accesses, causing Undefined Behaviour due to mutable aliasing.
///
/// # Examples
/// ```
/// use ansa::{Barrier, wait::WaitStrategy};
///
/// /// Prints the count of wait loop iterations after waiting.
/// struct CountIters;
///
/// // SAFETY: wait returns once barrier_seq >= desired_seq, with barrier_seq itself
/// unsafe impl WaitStrategy for CountIters {
///     fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
///         let mut counter = 0;
///         let mut barrier_seq = barrier.sequence();
///         while barrier_seq < desired_seq {
///             barrier_seq = barrier.sequence();
///             counter += 1;
///         }
///         println!("looped: {} times", counter);
///         barrier_seq
///     }
/// }
/// ```
///
/// The following example shows only _some_ of the possible implementation mistakes that will
/// cause UB.
/// ```
/// use ansa::{Barrier, wait::WaitStrategy};
///
/// struct BadWait;
///
/// // ** NOT SAFE **
/// unsafe impl WaitStrategy for BadWait {
///     fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
///         let mut barrier_seq = barrier.sequence();
///
///         // VERY BAD: we've changed only one character from `<` to `>`, but this makes
///         // it possible for waiting to end before the barrier has advanced. Could cause
///         // mutable aliasing, and thus UB.
///         while barrier_seq > desired_seq {
///             barrier_seq = barrier.sequence();
///         }
///         // VERY BAD: we return a sequence unrelated to the barrier, possibly leaving
///         // the disruptor in an inconsistent, non-recoverable state if a handle uses
///         // the value. Could cause mutable aliasing, and thus UB.
///         10
///     }
/// }
/// ```
pub unsafe trait WaitStrategy {
    /// Runs the wait loop.
    ///
    /// `desired_seq` represents a value which the barrier must exceed before the wait loop can end.
    /// Call [`Barrier::sequence`] to views updates of the barrier position.
    ///
    /// The following two conditions must hold:
    /// 1) May only return when `barrier sequence >= desired_seq` is true.
    /// 2) Must return the last read `barrier sequence`.
    ///
    /// The return value may be used by handles to determine which buffer elements to access, but
    /// this is not guaranteed.
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64;
}

// SAFETY: use of `wait_loop` guarantees correct implementation
unsafe impl<W> WaitStrategy for W
where
    W: Waiting,
{
    #[inline]
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
        wait_loop(desired_seq, barrier, || self.waiting())
    }
}

/// Implement to provide a fallible wait loop which runs as a handle waits for a sequence.
///
/// If a wait strategy is not fallible, or does not require either control over loop behaviour, or
/// carrying state across loop iterations, then prefer implementing [`Waiting`] instead, as it
/// provides a safe interface.
///
/// # Safety
///
/// This trait is unsafe as there is no guard against invalid implementations of
/// [`try_wait`](TryWaitStrategy::try_wait) causing Undefined Behaviour. A valid implementation
/// must ensure that the following two conditions holds:
/// 1) `try_wait` may only return when `barrier sequence >= desired_seq` is true.
/// 2) `try_wait`, if successful, must return the last read `barrier sequence`.
///
/// If `try_wait` does not abide by these conditions, then writes to the ring buffer may overlap
/// with other accesses, causing Undefined Behaviour due to mutable aliasing.
///
/// Note that there are no conditions limiting when `try_wait` can return an error.
///
/// # Examples
/// ```
/// use ansa::{Barrier, wait::TryWaitStrategy};
///
/// /// Wait until `max` iterations of the wait loop.
/// struct MaxIters {
///     max: usize
/// }
///
/// struct MaxItersError;
///
/// // SAFETY: only successful if barrier_seq >= desired_seq; returns barrier_seq
/// unsafe impl TryWaitStrategy for MaxIters {
///     type Error = MaxItersError;
///
///     fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
///         let mut iters = 0;
///         let mut barrier_seq = barrier.sequence();
///         while barrier_seq < desired_seq {
///             if iters >= self.max {
///                 return Err(MaxItersError)
///             }
///             barrier_seq = barrier.sequence();
///             iters += 1;
///         }
///         Ok(barrier_seq)
///     }
/// }
/// ```
/// Implementing a no wait strategy is also possible.
/// ```
/// use ansa::{Barrier, wait::TryWaitStrategy};
///
/// struct NoWait;
///
/// struct NoWaitError;
///
/// // SAFETY: only successful if barrier_seq >= desired_seq; returns barrier_seq
/// unsafe impl TryWaitStrategy for NoWait {
///     type Error = NoWaitError;
///
///     fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
///         match barrier.sequence() {
///             barrier_seq if barrier_seq < desired_seq => Err(NoWaitError),
///             barrier_seq => Ok(barrier_seq),
///         }
///     }
/// }
/// ```
///
/// The following example shows only _some_ of the possible implementation mistakes that will
/// cause UB.
/// ```
/// use ansa::{Barrier, wait::TryWaitStrategy};
///
/// struct BadWait;
///
/// // ** NOT SAFE **
/// unsafe impl TryWaitStrategy for BadWait {
///     type Error = ();
///
///     fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
///         let mut barrier_seq = barrier.sequence();
///
///         // VERY BAD: we've changed only one character from `<` to `>`, but this makes
///         // it possible for waiting to end before the barrier has advanced. Could cause
///         // mutable aliasing, and thus UB.
///         while barrier_seq > desired_seq {
///             barrier_seq = barrier.sequence();
///         }
///         // VERY BAD: we return a sequence unrelated to the barrier, possibly leaving
///         // the disruptor in an inconsistent, non-recoverable state if a handle uses
///         // the value. Could cause mutable aliasing, and thus UB.
///         Ok(10)
///     }
/// }
/// ```
pub unsafe trait TryWaitStrategy {
    type Error;

    /// Runs the fallible wait loop.
    ///
    /// `desired_seq` represents a value which the barrier must exceed before the wait loop can end.
    /// Call [`Barrier::sequence`] to views updates of the barrier position.
    ///
    /// The following two conditions must hold:
    /// 1) May only return when `barrier sequence >= desired_seq` is true.
    /// 2) If successful, must return the last read `barrier sequence`.
    ///
    /// No conditions are placed on returning errors.
    ///
    /// The return value may be used by handles to read or write elements to the ring buffer, but
    /// this is not guaranteed.
    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error>;
}

#[inline]
fn wait_loop(desired: i64, barrier: &Barrier, mut waiting: impl FnMut()) -> i64 {
    loop {
        let barrier_seq = barrier.sequence();
        if barrier_seq >= desired {
            break barrier_seq;
        }
        waiting()
    }
}

#[inline]
fn wait_loop_timeout(
    desired: i64,
    barrier: &Barrier,
    duration: Duration,
    mut waiting: impl FnMut(),
) -> Result<i64, TimedOut> {
    let timer = Instant::now();
    loop {
        let barrier_seq = barrier.sequence();
        if barrier_seq >= desired {
            break Ok(barrier_seq);
        }
        if timer.elapsed() > duration {
            break Err(TimedOut);
        }
        waiting()
    }
}

/// Pure busy-wait.
///
/// # Performance
///
/// Offers the lowest possible wait latency at the cost of unrestrained processor use.
///
/// Suitable when CPU resource use is of no concern.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusy;

impl Waiting for WaitBusy {
    #[inline]
    fn waiting(&self) {} // do nothing
}

/// Busy-wait and signal that a spin loop is occurring.
///
/// See: [`spin_loop`](std::hint::spin_loop) docs for further details.
///
/// # Performance
///
/// The spin loop signal can optimise processor use with minimal cost to latency, but should offer
/// latencies similar to [`WaitBusy`].
///
/// Nonetheless, it is best used when CPU resource consumption is of little concern.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusyHint;

impl Waiting for WaitBusyHint {
    #[inline]
    fn waiting(&self) {
        std::hint::spin_loop()
    }
}

/// Busy-wait, but allow the current thread to yield to the OS.
///
/// See: [`yield_now`](std::thread::yield_now) docs for further details.
///
/// # Performance
///
/// Like [`WaitBusy`], processor use is unrestrained, but [`WaitYield`] is more likely to cede CPU
/// resources when those resources are contended by other threads.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitYield;

impl Waiting for WaitYield {
    #[inline]
    fn waiting(&self) {
        std::thread::yield_now()
    }
}

/// Busy-wait, but sleep the current thread on each iteration of the wait loop.
///
/// The duration of the sleep is limited to `u64::MAX` nanoseconds.
///
/// # Performance
///
/// Trades latency for CPU resource use, depending on the length of the sleep.
///
/// An important consideration is that the sleep cannot be interrupted, unlike with
/// [`WaitBlocking`] which will wake when notified. Note also that spurious wakes can occur.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitSleep {
    duration: u64,
}

impl WaitSleep {
    /// Construct a [`WaitSleep`] strategy which will sleep for the given `duration`.
    ///
    /// `duration` will be truncated to `u64::MAX` nanoseconds.
    #[inline]
    pub fn new(duration: Duration) -> Self {
        WaitSleep {
            duration: duration.as_nanos() as u64,
        }
    }
}

impl Waiting for WaitSleep {
    #[inline]
    fn waiting(&self) {
        std::thread::sleep(Duration::from_nanos(self.duration))
    }
}

/// Block on each iteration of the wait loop until signalled that the barrier ahead has moved.
///
/// Alternatively, will wake and check whether the barrier has moved after a given duration
/// (defaulted to `200` microseconds).
///
/// The blocking duration is limited to `u64::MAX` nanoseconds.
///
/// # Performance
///
/// High latency but, in contrast to [`WaitSleep`], will wake when new sequences are made visible.
///
/// Configuring the wake duration allows trading latency for CPU resource use.
#[derive(Clone, Debug)]
pub struct WaitBlocking {
    pair: Arc<(Condvar, Mutex<Empty>)>,
    duration: u64,
}

#[derive(Copy, Clone, Debug)]
struct Empty;

// All instances of WaitBlocking share the same (condvar, mutex) pair
static BLOCK_VAR: LazyLock<Arc<(Condvar, Mutex<Empty>)>> =
    LazyLock::new(|| Arc::new((Condvar::new(), Mutex::new(Empty))));

impl WaitBlocking {
    /// Construct an instance of [`WaitBlocking`] with the default wake duration (200 microseconds).
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        WaitBlocking {
            pair: Arc::clone(&BLOCK_VAR),
            duration: 200_000, // 200 micros in nanos
        }
    }

    /// Construct an instance of [`WaitBlocking`] with the given wake `duration`.
    ///
    /// `duration` will be truncated to `u64::MAX` nanoseconds.
    #[inline]
    pub fn wake_after(duration: Duration) -> Self {
        WaitBlocking {
            pair: Arc::clone(&BLOCK_VAR),
            duration: duration.as_nanos() as u64,
        }
    }
}

// SAFETY: use of `wait_loop` guarantees correct implementation
unsafe impl WaitStrategy for WaitBlocking {
    #[inline]
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
        let (condvar, mutex) = &*self.pair;
        let block_duration = Duration::from_nanos(self.duration);
        let barrier_seq = wait_loop(desired_seq, barrier, || {
            let _unused = condvar.wait_timeout(mutex.lock().unwrap(), block_duration);
        });
        condvar.notify_all();
        barrier_seq
    }
}

// SAFETY: use of `wait_loop_timeout` guarantees correct implementation
unsafe impl TryWaitStrategy for Timeout<WaitBlocking> {
    type Error = TimedOut;

    #[inline]
    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        let (condvar, mutex) = &*self.strategy.pair;
        let block_duration = Duration::from_nanos(self.strategy.duration);
        let timeout_duration = Duration::from_nanos(self.duration);
        let barrier_seq = wait_loop_timeout(desired_seq, barrier, timeout_duration, || {
            let _unused = condvar.wait_timeout(mutex.lock().unwrap(), block_duration);
        })?;
        condvar.notify_all();
        Ok(barrier_seq)
    }
}

/// Performs a phased back-off of strategies during the wait loop.
///
/// This strategy busy spins, then yields, and finally calls the fallback strategy.
///
/// Both durations for spinning and yielding are limited to `u64::MAX` nanoseconds.
///
/// # Performance
///
/// Highly dependent on the fallback strategy, but best used when low latency is not a priority
/// verses CPU resource use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitPhased<W> {
    spin_duration: u64,
    yield_duration: u64,
    fallback: W,
}

impl<W: Copy> Copy for WaitPhased<W> {}

impl<W> WaitPhased<W> {
    /// Construct a [`WaitPhased`] instance with the given fallback strategy.
    ///
    /// `spin_duration` sets the duration for which to busy-spin. `yield_duration` does the same
    /// for yielding to the OS.
    ///
    /// `spin_duration` and `yield_duration` are truncated to `u64::MAX` nanoseconds.
    pub fn new(spin_duration: Duration, yield_duration: Duration, fallback: W) -> Self {
        let spin_nanos = spin_duration.as_nanos() as u64;
        let yield_nanos = (spin_duration + yield_duration).as_nanos() as u64;
        WaitPhased {
            spin_duration: spin_nanos,
            yield_duration: yield_nanos,
            fallback,
        }
    }
}

// SAFETY: wait only returns once barrier_seq >= desired_seq, with barrier_seq itself
unsafe impl<W> WaitStrategy for WaitPhased<W>
where
    W: WaitStrategy,
{
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
        let timer = Instant::now();
        let spin_dur = Duration::from_nanos(self.spin_duration);
        let yield_dur = Duration::from_nanos(self.yield_duration);
        loop {
            let barrier_seq = barrier.sequence();
            if barrier_seq >= desired_seq {
                break barrier_seq;
            }
            match timer.elapsed() {
                dur if dur < spin_dur => (),
                dur if dur < yield_dur => std::thread::yield_now(),
                _ => return self.fallback.wait(desired_seq, barrier),
            }
        }
    }
}

// SAFETY: try_wait only returns once barrier_seq >= desired_seq, with barrier_seq itself
unsafe impl<W> TryWaitStrategy for WaitPhased<W>
where
    W: TryWaitStrategy,
{
    type Error = W::Error;

    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
        let timer = Instant::now();
        let spin_dur = Duration::from_nanos(self.spin_duration);
        let yield_dur = Duration::from_nanos(self.yield_duration);
        loop {
            let barrier_seq = barrier.sequence();
            if barrier_seq >= desired_seq {
                break Ok(barrier_seq);
            }
            match timer.elapsed() {
                dur if dur < spin_dur => (),
                dur if dur < yield_dur => std::thread::yield_now(),
                _ => return self.fallback.try_wait(desired_seq, barrier),
            }
        }
    }
}

/// Indicates that the waiting handle has timed out.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TimedOut;

/// Wrapper which provides timeout capabilities to strategies implementing `Waiting`.
///
/// If a type, `T`, implements [`Waiting`], then `Timeout<T>` implements [`TryWaitStrategy`].
///
/// This struct is not required for implementing [`TryWaitStrategy`], it is only a convenience
/// for automating implementations of this trait.
///
/// The length of the timeout is limited to `u64::MAX` nanoseconds.
///
/// # Examples
/// ```
/// use ansa::*;
/// use ansa::wait::*;
/// use std::time::Duration;
///
/// let wait_factory = || Timeout::new(Duration::from_millis(1), WaitBusy);
///
/// let _ = DisruptorBuilder::new(64, || 0)
///     .wait_strategy(wait_factory)
///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct Timeout<W> {
    duration: u64,
    strategy: W,
}

impl<W: Clone> Clone for Timeout<W> {
    fn clone(&self) -> Self {
        Timeout {
            duration: self.duration,
            strategy: self.strategy.clone(),
        }
    }
}

impl<W: Copy> Copy for Timeout<W> {}

impl<W> Timeout<W> {
    /// Construct a `Timeout` with the given strategy.
    ///
    /// `duration` sets the amount of time after which the strategy will time out. Timeout will not
    /// occur before `duration` elapses, but may not occur immediately after that point either.
    /// Timings should not be treated as exact.
    ///
    /// `duration` will be truncated to `u64::MAX` nanoseconds.
    pub fn new(duration: Duration, strategy: W) -> Self {
        Timeout {
            duration: duration.as_nanos() as u64,
            strategy,
        }
    }
}

// SAFETY: use of `wait_loop_timeout` guarantees correct implementation
unsafe impl<W> TryWaitStrategy for Timeout<W>
where
    W: Waiting,
{
    type Error = TimedOut;

    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
        let duration = Duration::from_nanos(self.duration);
        wait_loop_timeout(desired_seq, barrier, duration, || self.strategy.waiting())
    }
}
