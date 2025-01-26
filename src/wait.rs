//! todo Wait strategies ordered by best latency

use crate::handles::Barrier;
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

/// todo docs
pub trait WaitStrategy {
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64;
}

#[inline]
fn wait_loop(expected: i64, barrier: &Barrier, mut waiting: impl FnMut()) -> i64 {
    let test = || {
        let barrier_seq = barrier.sequence();
        (barrier_seq, barrier_seq >= expected)
    };
    loop {
        if let (seq, true) = test() {
            break seq;
        }
        waiting()
    }
}

/// A Pure busy-spin strategy which offers the lowest wait latency at the cost of increased
/// processor use.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusy;

impl WaitStrategy for WaitBusy {
    #[inline]
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        wait_loop(expected, barrier, || ())
    }
}

/// A busy-spin strategy which optimises processor use (see [`spin_loop`](std::hint::spin_loop) docs
/// for details) at the cost of latency.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusyHint;

impl WaitStrategy for WaitBusyHint {
    #[inline]
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        wait_loop(expected, barrier, std::hint::spin_loop)
    }
}

/// todo docs
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitYield;

impl WaitStrategy for WaitYield {
    #[inline]
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        wait_loop(expected, barrier, std::thread::yield_now)
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

impl WaitStrategy for WaitSleep {
    #[inline]
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        wait_loop(expected, barrier, || std::thread::sleep(self.duration))
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
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        let (condvar, mutex) = &*self.pair;
        let barrier_seq = wait_loop(expected, barrier, || {
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
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        let timer = Instant::now();
        let mut barrier_seq = barrier.sequence();
        while barrier_seq < expected {
            barrier_seq = barrier.sequence();
            match timer.elapsed() {
                dur if dur < self.spin_duration => (),
                dur if dur < self.yield_duration => std::thread::yield_now(),
                _ => return self.fallback.wait(expected, barrier),
            }
        }
        barrier_seq
    }
}

impl<W: WaitStrategyTimeout> WaitStrategyTimeout for WaitPhased<W> {
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        let timer = Instant::now();
        let mut barrier_seq = barrier.sequence();
        while barrier_seq < expected {
            barrier_seq = barrier.sequence();
            match timer.elapsed() {
                dur if dur < self.spin_duration => (),
                dur if dur < self.yield_duration => std::thread::yield_now(),
                _ => return self.fallback.wait_timeout(expected, barrier),
            }
        }
        Ok(barrier_seq)
    }
}

/// Indicates that the waiting handle timed out.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TimedOut;

/// todo docs
pub trait WaitStrategyTimeout {
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, TimedOut>;
}

#[inline]
fn wait_loop_timeout(
    expected: i64,
    barrier: &Barrier,
    duration: Duration,
    mut waiting: impl FnMut(),
) -> Result<i64, TimedOut> {
    let test = || {
        let barrier_seq = barrier.sequence();
        (barrier_seq, barrier_seq >= expected)
    };
    let timer = Instant::now();
    loop {
        if let (seq, true) = test() {
            break Ok(seq);
        }
        if timer.elapsed() > duration {
            break Err(TimedOut);
        }
        waiting()
    }
}

/// todo docs
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Timeout<W: WaitStrategy> {
    duration: Duration,
    strategy: W,
}

impl<W: Copy + WaitStrategy> Copy for Timeout<W> {}

impl<W: WaitStrategy> Timeout<W> {
    pub fn new(duration: Duration, strategy: W) -> Self {
        Timeout { duration, strategy }
    }
}

impl WaitStrategyTimeout for Timeout<WaitBusy> {
    #[inline]
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        wait_loop_timeout(expected, barrier, self.duration, || ())
    }
}

impl WaitStrategyTimeout for Timeout<WaitBusyHint> {
    #[inline]
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        wait_loop_timeout(expected, barrier, self.duration, std::hint::spin_loop)
    }
}

impl WaitStrategyTimeout for Timeout<WaitYield> {
    #[inline]
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        wait_loop_timeout(expected, barrier, self.duration, std::thread::yield_now)
    }
}

impl WaitStrategyTimeout for Timeout<WaitSleep> {
    #[inline]
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        wait_loop_timeout(expected, barrier, self.duration, || {
            std::thread::sleep(self.strategy.duration)
        })
    }
}

impl WaitStrategyTimeout for Timeout<WaitBlocking> {
    #[inline]
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, TimedOut> {
        let (condvar, mutex) = &*self.strategy.pair;
        let barrier_seq = wait_loop_timeout(expected, barrier, self.duration, || {
            let _unused = condvar.wait_timeout(mutex.lock().unwrap(), self.strategy.duration);
        })?;
        condvar.notify_all();
        Ok(barrier_seq)
    }
}
