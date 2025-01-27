//! todo Wait strategies ordered by best latency

use crate::handles::Barrier;
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

/// todo docs
/// Implement if you just want to provide logic to run while looping.
pub trait Waiting {
    fn waiting(&self);
}

/// todo docs
/// Implement if you want control over the loop.
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

/// A Pure busy-spin strategy which offers the lowest possible wait latency at the cost of
/// unrestrained processor use.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusy;

impl Waiting for WaitBusy {
    #[inline]
    fn waiting(&self) {} // do nothing
}

/// A busy-spin strategy which optimises processor use (see the [`spin_loop`](std::hint::spin_loop)
/// docs for details) at the cost of latency.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusyHint;

impl Waiting for WaitBusyHint {
    #[inline]
    fn waiting(&self) {
        std::hint::spin_loop()
    }
}

/// todo docs
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitYield;

impl Waiting for WaitYield {
    #[inline]
    fn waiting(&self) {
        std::thread::yield_now()
    }
}

/// todo docs
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitSleep {
    duration: Duration,
}

impl WaitSleep {
    #[inline]
    pub fn new(secs: u64, nanos: u32) -> Self {
        WaitSleep {
            duration: Duration::new(secs, nanos),
        }
    }
}

impl Waiting for WaitSleep {
    #[inline]
    fn waiting(&self) {
        std::thread::sleep(self.duration)
    }
}

/// todo docs
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
            duration: Duration::from_micros(20),
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
            let mut barrier_seq = barrier.sequence();
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

impl<W: WaitStrategyTimeout> WaitStrategyTimeout for WaitPhased<W> {
    fn wait_timeout(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        let timer = Instant::now();
        loop {
            let mut barrier_seq = barrier.sequence();
            if barrier_seq >= desired_seq {
                break Ok(barrier_seq);
            }
            match timer.elapsed() {
                dur if dur < self.spin_duration => (),
                dur if dur < self.yield_duration => std::thread::yield_now(),
                _ => return self.fallback.wait_timeout(desired_seq, barrier),
            }
        }
    }
}

/// Indicates that the waiting handle has timed out.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TimedOut;

/// todo docs
/// Implement if you want control over the loop and to provide a timeout
pub trait WaitStrategyTimeout {
    /// Wait for `barrier` sequence to reach `desired_seq` until timeout.
    fn wait_timeout(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, TimedOut>;
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

/// todo docs
/// doesn't need to be used for implementing timeouts
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

impl<W> Waiting for Timeout<W>
where
    W: Waiting,
{
    #[inline]
    fn waiting(&self) {
        self.strategy.waiting()
    }
}

impl<W> WaitStrategyTimeout for Timeout<W>
where
    W: Waiting,
{
    fn wait_timeout(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        wait_loop_timeout(desired_seq, barrier, self.duration, || self.waiting())
    }
}

impl WaitStrategyTimeout for Timeout<WaitBlocking> {
    #[inline]
    fn wait_timeout(&self, desired_seq: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        let (condvar, mutex) = &*self.strategy.pair;
        let barrier_seq = wait_loop_timeout(desired_seq, barrier, self.duration, || {
            let _unused = condvar.wait_timeout(mutex.lock().unwrap(), self.strategy.duration);
        })?;
        condvar.notify_all();
        Ok(barrier_seq)
    }
}
