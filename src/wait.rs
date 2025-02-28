//! Provides strategies used by handles when waiting for sequences on the ring buffer.
//!
//! Also provided are three traits for implementing your own wait logic.
//!
//! # Performance
//!
//! Each strategy provides a tradeoff between latency and CPU use, as detailed in their separate
//! docs, but this is not the only aspect to consider when optimizing performance. The size of the
//! strategy is also an important factor.
//!
//! Each handle struct includes a wait strategy. So the size of the strategy may impact performance
//! if, for example, that additional size prevents the handle from fitting into a single cache line.
//!
//! Cache lines are commonly `64`, or `128` bytes.
//!
//! When the wait strategy is zero-sized, each handle has the following size (in bytes, on a 64-bit
//! system). Changes in these sizes, if compiled for a single system, are considered breaking.
//!
//! | Handle                                           | size |
//! |--------------------------------------------------|------|
//! | [`Consumer`](crate::handles::Consumer)           | 32   |
//! | [`Producer`](crate::handles::Producer)           | 32   |
//! | [`MultiProducer`](crate::handles::MultiProducer) | 40   |
//!
//! And here are the minimum sizes of the provided strategies (on a 64-bit system), again assuming
//! nested strategies are zero-sized. Changes in these sizes are also considered breaking.
//!
//! | Strategy                      | size |
//! |-------------------------------|------|
//! | [`WaitBusy`]                  | 0    |
//! | [`WaitBusyHint`]              | 0    |
//! | [`WaitYield`]                 | 0    |
//! | [`WaitSleep`]                 | 8    |
//! | [`WaitPhased<W>`](WaitPhased) | 16   |
//! | [`Timeout<W>`](Timeout)       | 8    |
//!
//! The sizes of both [`WaitPhased`] and [`Timeout`] of course ultimately depend on their included
//! strategy.
//!
//! Various provided strategies are limited to wait durations of `u64::MAX` nanoseconds, which is
//! done in order to keep their sizes small. Storing a [`Duration`] instead would double the
//! minimum size of these strategies.
//! ```
//! use std::time::Duration;
//!
//! assert_eq!(size_of::<Duration>(), 16);
//! assert_eq!(size_of::<u64>(), 8);
//! ```

use crate::handles::Barrier;
use std::time::{Duration, Instant};

/// Implement to provide logic which will run inside a wait loop.
///
/// This trait is unsuitable when state needs to be held across loop iterations, since
/// [`waiting`](Waiting::waiting) cannot observe outside of the loop.
///
/// If state or fallibility is required, implement [`WaitStrategy`] or [`TryWaitStrategy`] instead.
///
/// If a type, `T`, implements `Waiting`, then `T` will implement `WaitStrategy`, and
/// [`Timeout<T>`](Timeout) will implement `TryWaitStrategy`.
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

    /// Construct a `Timeout` using this strategy.
    ///
    /// `duration` sets the amount of time after which the strategy will time out. Timeout will not
    /// occur before `duration` elapses, but may not occur immediately after that point either.
    /// Timings should not be treated as exact.
    ///
    /// `duration` will be truncated to `u64::MAX` nanoseconds.
    ///
    /// # Examples
    /// ```
    /// use std::time::Duration;
    /// use ansa::wait::Waiting;
    ///
    /// struct MyWaiter;
    ///
    /// impl Waiting for MyWaiter {
    ///     fn waiting(&self) {} // busy-spin
    /// }
    ///
    /// let timeout = MyWaiter.with_timeout(Duration::from_millis(1));
    /// assert_eq!(timeout.nanos(), 1_000_000); // duration in nanos
    /// ```
    fn with_timeout(self, duration: Duration) -> Timeout<Self>
    where
        Self: Sized,
    {
        Timeout::new(duration, self)
    }
}

/// Implement to provide a wait loop which runs as a handle waits for a sequence.
///
/// Then list rules...
///
/// If a wait strategy does not require either control over loop behaviour, or carrying state
/// across loop iterations, then prefer implementing [`Waiting`] instead, as it provides a safe
/// interface.
///
/// well-behaved, won't cause UB if not observed:
/// should return as soon as barrier >= desired
/// safety:
/// must not return while barrier < desired
/// must return with the last barrier seq value
///
/// # Safety
///
/// This trait is unsafe as there is no guard against invalid implementations of
/// [`wait`](WaitStrategy::wait) causing Undefined Behaviour. Valid implementations must satisfy
/// the following conditions:
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
    /// Call [`Barrier::sequence`] to view updates of the barrier's position.
    ///
    /// Implementations must satisfy the following conditions:
    /// 1) May only return when `barrier sequence >= desired_seq` is true.
    /// 2) Must return the last read `barrier sequence`.
    ///
    /// The return value may be used by handles to determine which buffer elements to access, but
    /// this behaviour is not guaranteed.
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64;
}

// SAFETY: wait returns once barrier_seq >= desired_seq, with barrier_seq itself
unsafe impl<W> WaitStrategy for W
where
    W: Waiting,
{
    #[inline]
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
        loop {
            let barrier_seq = barrier.sequence();
            if barrier_seq >= desired_seq {
                break barrier_seq;
            }
            self.waiting()
        }
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
/// [`try_wait`](TryWaitStrategy::try_wait) causing Undefined Behaviour. Valid implementations must
/// satisfy the following conditions:
/// 1) `try_wait` may only successfully return when `barrier sequence >= desired_seq` is true.
/// 2) `try_wait`, if successful, must return the last read `barrier sequence`.
///
/// If `try_wait` does not abide by these conditions, then writes to the ring buffer may overlap
/// with other accesses, causing Undefined Behaviour due to mutable aliasing.
///
/// Note that no conditions limit when `try_wait` can return an error.
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
///
/// Implementing a no wait strategy is also possible (though not necessary, see: `wait_range`).
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
    /// Call [`Barrier::sequence`] to view updates of the barrier's position.
    ///
    /// Implementations must satisfy the following conditions:
    /// 1) May only successfully return when `barrier sequence >= desired_seq` is true.
    /// 2) If successful, must return the last read `barrier sequence`.
    ///
    /// No conditions are placed on returning errors.
    ///
    /// The return value may be used by handles to read or write elements to the ring buffer, but
    /// this behaviour is not guaranteed.
    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error>;
}

/// Pure busy-spin waiting.
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

/// Sleep the current thread on each iteration of the wait loop.
///
/// Equivalent to polling available sequences at a fixed interval.
///
/// Default duration is 50 microseconds.
///
/// The duration of the sleep is limited to `u64::MAX` nanoseconds.
///
/// # Performance
///
/// Trades latency for CPU resource use, depending on the length of the sleep.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitSleep {
    nanos: u64,
}

impl WaitSleep {
    /// Construct a [`WaitSleep`] strategy which will sleep for the given `duration`.
    ///
    /// `duration` will be truncated to `u64::MAX` nanoseconds.
    #[inline]
    pub const fn new(duration: Duration) -> Self {
        WaitSleep {
            nanos: truncate_u128(duration.as_nanos()),
        }
    }
}

impl Default for WaitSleep {
    fn default() -> Self {
        WaitSleep { nanos: 50_000 } // 50 microseconds
    }
}

impl Waiting for WaitSleep {
    #[inline]
    fn waiting(&self) {
        std::thread::sleep(Duration::from_nanos(self.nanos))
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
    spin_nanos: u64,
    yield_nanos: u64,
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
    pub const fn new(spin_duration: Duration, yield_duration: Duration, fallback: W) -> Self {
        let spin_nanos = truncate_u128(spin_duration.as_nanos());
        let yield_nanos = spin_nanos.saturating_add(truncate_u128(yield_duration.as_nanos()));
        WaitPhased {
            spin_nanos,
            yield_nanos,
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
        let spin_dur = Duration::from_nanos(self.spin_nanos);
        let yield_dur = Duration::from_nanos(self.yield_nanos);
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

// SAFETY: Before yield_dur exceeded, try_wait only returns once barrier_seq >= desired_seq, with
// barrier_seq. When exceeded, calls fallback try_wait, which is expected to be validly implemented.
unsafe impl<W> TryWaitStrategy for WaitPhased<W>
where
    W: TryWaitStrategy,
{
    type Error = W::Error;

    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
        let timer = Instant::now();
        let spin_dur = Duration::from_nanos(self.spin_nanos);
        let yield_dur = Duration::from_nanos(self.yield_nanos);
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
/// This struct is not required for implementing `TryWaitStrategy`, it is only a convenience
/// for automating implementations of timeouts.
///
/// The length of the timeout is limited to `u64::MAX` nanoseconds.
///
/// # Examples
/// ```
/// use ansa::*;
/// use ansa::wait::*;
/// use std::time::Duration;
///
/// let strategy = Timeout::new(Duration::from_millis(1), WaitBusy);
///
/// let _ = DisruptorBuilder::new(64, || 0)
///     .wait_strategy(strategy)
///     .add_handle(0, Handle::Consumer, Follows::LeadProducer)
///     .build()
///     .unwrap();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Timeout<W> {
    nanos: u64,
    strategy: W,
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
    ///
    /// # Examples
    /// ```
    /// use ansa::wait::{Timeout, WaitBusy};
    /// use std::time::Duration;
    ///
    /// // duration of way more than u64::MAX nanoseconds
    /// let duration = Duration::from_secs(u64::MAX);
    ///
    /// let timeout = Timeout::new(duration, WaitBusy);
    /// // duration till timeout truncated to u64::MAX nanoseconds
    /// assert_eq!(timeout.nanos(), u64::MAX);
    /// ```
    pub const fn new(duration: Duration, strategy: W) -> Self {
        Timeout {
            nanos: truncate_u128(duration.as_nanos()),
            strategy,
        }
    }

    /// Returns the duration till timeout, in nanoseconds.
    pub const fn nanos(&self) -> u64 {
        self.nanos
    }
}

// SAFETY: try_wait only successfully returns with barrier_seq once barrier_seq >= desired_seq
unsafe impl<W> TryWaitStrategy for Timeout<W>
where
    W: Waiting,
{
    type Error = TimedOut;

    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
        let duration = Duration::from_nanos(self.nanos);
        let timer = Instant::now();
        loop {
            let barrier_seq = barrier.sequence();
            if barrier_seq >= desired_seq {
                break Ok(barrier_seq);
            }
            if timer.elapsed() > duration {
                break Err(TimedOut);
            }
            self.strategy.waiting()
        }
    }
}

#[inline]
const fn truncate_u128(n: u128) -> u64 {
    if n > u64::MAX as u128 {
        return u64::MAX;
    }
    n as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    // Check that sizes don't accidentally change. If size change is found and intended, just
    // change the values in this test.
    #[test]
    fn sizes() {
        assert_eq!(size_of::<WaitBusy>(), 0);
        assert_eq!(size_of::<WaitBusyHint>(), 0);
        assert_eq!(size_of::<WaitYield>(), 0);
        assert_eq!(size_of::<WaitSleep>(), 8);
        assert_eq!(size_of::<WaitPhased<WaitBusy>>(), 16);
        assert_eq!(size_of::<Timeout<WaitBusy>>(), 8);
    }

    #[test]
    fn test() {
        let duration = Duration::from_secs(u64::MAX);
        assert_eq!(truncate_u128(duration.as_nanos()), u64::MAX);
    }
}
