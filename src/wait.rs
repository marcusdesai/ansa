//! todo Wait strategies ordered by best latency

use crate::handles::Barrier;
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

/// Implement to provide logic which will run inside a wait loop.
///
/// This trait is unsuitable when state needs to be held across loop iteration, since the
/// [`Waiting::waiting`] method cannot observe outside the loop. If state is required, Implement
/// [`WaitStrategy`] or [`TryWaitStrategy`] instead.
///
/// If a type, `T`, implements [`Waiting`], then `T` will implement [`WaitStrategy`], and
/// `Timeout<T>` will implement [`TryWaitStrategy`].
///
/// As a consequence of the above blanket impls, do _not_ implement [`Waiting`] for a type if you
/// want to provide your own implementations of [`WaitStrategy`] or [`TryWaitStrategy`].
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

/// todo docs
/// Implement if you want control over the loop.
///
/// must return final barrier sequence
/// must wait until barrier sequence >= `desired_seq`
///
/// todo: does this need to be unsafe, since it's so easy to cause undefined behaviour with it?
///
/// # Examples
/// ```
/// use ansa::{Barrier, wait::WaitStrategy};
///
/// /// Counts and prints iterations of the wait loop.
/// struct CountIters;
///
/// impl WaitStrategy for CountIters {
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
pub trait WaitStrategy {
    /// Wait for `barrier` sequence to reach `desired_seq`.
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64;
}

impl<W> WaitStrategy for W
where
    W: Waiting,
{
    #[inline]
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
        wait_loop(desired_seq, barrier, || self.waiting())
    }
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

/// Pure busy-wait.
///
/// Offers the lowest possible wait latency at the cost of unrestrained processor use.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusy;

impl Waiting for WaitBusy {
    #[inline]
    fn waiting(&self) {} // do nothing
}

/// Busy-wait and signal that such a spin loop is occurring.
///
/// The spin loop signal can optimise processor use with minimal cost to latency.
///
/// See: [`spin_loop`](std::hint::spin_loop) docs for further details.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusyHint;

impl Waiting for WaitBusyHint {
    #[inline]
    fn waiting(&self) {
        std::hint::spin_loop()
    }
}

/// Busy-wait and allow the current thread to yield to the OS.
///
/// See: [`yield_now`](std::thread::yield_now) docs for further details.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitYield;

impl Waiting for WaitYield {
    #[inline]
    fn waiting(&self) {
        std::thread::yield_now()
    }
}

/// Busy-wait and sleep the current thread on each iteration of the wait loop.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitSleep {
    duration: Duration,
}

impl WaitSleep {
    /// Construct a [`WaitSleep`] strategy which will sleep for the given [`Duration`].
    #[inline]
    pub fn new(duration: Duration) -> Self {
        WaitSleep { duration }
    }
}

impl Waiting for WaitSleep {
    #[inline]
    fn waiting(&self) {
        std::thread::sleep(self.duration)
    }
}

/// Block on each iteration of the wait loop until signalled that the barrier ahead has moved.
///
/// Alternatively, will wake and check whether the barrier has moved after a given duration
/// (defaulted to `200` microseconds).
#[derive(Clone, Debug)]
pub struct WaitBlocking {
    pair: Arc<(Condvar, Mutex<Empty>)>,
    duration: Duration,
}

#[derive(Copy, Clone, Debug)]
struct Empty;

// All handles share the same condvar
static BLOCK_VAR: LazyLock<Arc<(Condvar, Mutex<Empty>)>> =
    LazyLock::new(|| Arc::new((Condvar::new(), Mutex::new(Empty))));

impl WaitBlocking {
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        WaitBlocking {
            pair: Arc::clone(&BLOCK_VAR),
            duration: Duration::from_micros(200),
        }
    }

    #[inline]
    pub fn with_timeout(duration: Duration) -> Self {
        WaitBlocking {
            pair: Arc::clone(&BLOCK_VAR),
            duration,
        }
    }
}

impl WaitStrategy for WaitBlocking {
    #[inline]
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
        let (condvar, mutex) = &*self.pair;
        let barrier_seq = wait_loop(desired_seq, barrier, || {
            let _unused = condvar.wait_timeout(mutex.lock().unwrap(), self.duration);
        });
        condvar.notify_all();
        barrier_seq
    }
}

impl TryWaitStrategy for Timeout<WaitBlocking> {
    type Error = TimedOut;

    #[inline]
    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        let (condvar, mutex) = &*self.strategy.pair;
        let barrier_seq = wait_loop_timeout(desired_seq, barrier, self.duration, || {
            let _unused = condvar.wait_timeout(mutex.lock().unwrap(), self.strategy.duration);
        })?;
        condvar.notify_all();
        Ok(barrier_seq)
    }
}

/// todo docs
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitPhased<W> {
    spin_duration: Duration,
    yield_duration: Duration,
    fallback: W,
}

impl<W: Copy> Copy for WaitPhased<W> {}

impl<W> WaitPhased<W> {
    pub fn new(spin_duration: Duration, yield_duration: Duration, fallback: W) -> Self {
        WaitPhased {
            spin_duration,
            yield_duration: spin_duration + yield_duration,
            fallback,
        }
    }
}

impl<W: WaitStrategy> WaitStrategy for WaitPhased<W> {
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
        let timer = Instant::now();
        loop {
            let barrier_seq = barrier.sequence();
            if barrier_seq >= desired_seq {
                break barrier_seq;
            }
            match timer.elapsed() {
                dur if dur < self.spin_duration => (),
                dur if dur < self.yield_duration => std::thread::yield_now(),
                _ => return self.fallback.wait(desired_seq, barrier),
            }
        }
    }
}

impl<W: TryWaitStrategy> TryWaitStrategy for WaitPhased<W> {
    type Error = W::Error;

    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
        let timer = Instant::now();
        loop {
            let barrier_seq = barrier.sequence();
            if barrier_seq >= desired_seq {
                break Ok(barrier_seq);
            }
            match timer.elapsed() {
                dur if dur < self.spin_duration => (),
                dur if dur < self.yield_duration => std::thread::yield_now(),
                _ => return self.fallback.try_wait(desired_seq, barrier),
            }
        }
    }
}

/// todo docs
/// Implement if you want control over the loop and to break out of the wait loop with an error.
///
/// # Examples
/// ```
/// use ansa::{Barrier, wait::TryWaitStrategy};
///
/// /// waits until `max` iterations of the wait loop
/// struct MaxWait {
///     max_iters: usize
/// }
///
/// struct MaxItersError;
///
/// impl TryWaitStrategy for MaxWait {
///     type Error = MaxItersError;
///
///     fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error> {
///         let mut iters = 0;
///         let mut barrier_seq = barrier.sequence();
///         while barrier_seq < desired_seq {
///             if iters >= self.max_iters {
///                 return Err(MaxItersError)
///             }
///             barrier_seq = barrier.sequence();
///             iters += 1;
///         }
///         Ok(barrier_seq)
///     }
/// }
/// ```
pub trait TryWaitStrategy {
    type Error;

    /// Wait for `barrier` sequence to reach `desired_seq`, or error.
    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, Self::Error>;
}

/// Indicates that the waiting handle has timed out.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TimedOut;

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

/// Wrapper struct for wait strategies which provides a timeout capability.
///
/// If a type, `T`, implements [`Waiting`], then `Timeout<T>` implements [`TryWaitStrategy`].
///
/// This struct is not required for implementing [`TryWaitStrategy`], it is only a convenience
/// for automating implementations of this trait.
#[derive(Debug, Eq, PartialEq)]
pub struct Timeout<W> {
    duration: Duration,
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
    pub fn new(duration: Duration, strategy: W) -> Self {
        Timeout { duration, strategy }
    }
}

impl<W> TryWaitStrategy for Timeout<W>
where
    W: Waiting,
{
    type Error = TimedOut;

    fn try_wait(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        wait_loop_timeout(desired_seq, barrier, self.duration, || {
            self.strategy.waiting()
        })
    }
}
