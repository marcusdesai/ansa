//! todo Wait strategies ordered by best latency

use crate::handles::Barrier;
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

pub trait WaitStrategy {
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64;
}

fn wait_generic(expected: i64, barrier: &Barrier, mut waiting: impl FnMut()) -> i64 {
    let mut barrier_seq = barrier.sequence();
    while barrier_seq < expected {
        waiting();
        barrier_seq = barrier.sequence()
    }
    barrier_seq
}

/// A Pure busy-spin strategy which offers the lowest wait latency at the cost of increased
/// processor use.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusy;

impl WaitStrategy for WaitBusy {
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        wait_generic(expected, barrier, || ())
    }
}

/// A busy-spin strategy which optimises processor use (see [`spin_loop`](std::hint::spin_loop) docs
/// for details) at the cost of latency.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitBusyHint;

impl WaitStrategy for WaitBusyHint {
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        wait_generic(expected, barrier, std::hint::spin_loop)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitYield;

impl WaitStrategy for WaitYield {
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        wait_generic(expected, barrier, std::thread::yield_now)
    }
}

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
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        wait_generic(expected, barrier, || std::thread::sleep(self.duration))
    }
}

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
    fn wait(&self, expected: i64, barrier: &Barrier) -> i64 {
        let (condvar, mutex) = &*self.pair;
        let barrier_seq = wait_generic(expected, barrier, || {
            let _unused = condvar.wait_timeout(mutex.lock().unwrap(), self.duration);
        });
        condvar.notify_all();
        barrier_seq
    }
}

#[derive(Clone, Debug)]
pub struct WaitPhased<W> {
    spin_duration: Duration,
    yield_duration: Duration,
    fallback: W,
}

impl<W> WaitPhased<W>
where
    W: WaitStrategy,
{
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

impl<W: Copy> Copy for WaitPhased<W> {}
impl<W: PartialEq> PartialEq for WaitPhased<W> {
    fn eq(&self, other: &Self) -> bool {
        self.spin_duration == other.spin_duration
            && self.yield_duration == other.yield_duration
            && self.fallback == other.fallback
    }
}
impl<W: Eq + PartialEq> Eq for WaitPhased<W> {}

/// todo
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Timeout;

/// todo
pub trait WaitStrategyTimeout {
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, Timeout>;
}

fn wait_timeout_generic<F>(expected: i64, barrier: &Barrier, mut waiting: F) -> Result<i64, Timeout>
where
    F: FnMut() -> Result<(), Timeout>,
{
    let mut barrier_seq = barrier.sequence();
    while barrier_seq < expected {
        waiting()?;
        barrier_seq = barrier.sequence()
    }
    Ok(barrier_seq)
}

/// todo
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WaitTimeout {
    duration: Duration,
}

impl WaitStrategyTimeout for WaitTimeout {
    fn wait_timeout(&self, expected: i64, barrier: &Barrier) -> Result<i64, Timeout> {
        let timer = Instant::now();
        wait_timeout_generic(expected, barrier, || {
            if timer.elapsed() > self.duration {
                return Err(Timeout);
            }
            Ok(())
        })
    }
}
