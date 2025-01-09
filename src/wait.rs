use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

pub trait WaitStrategy {
    fn wait(&self);

    #[inline]
    fn finalise(&self) {
        // defaults to noop
    }
}

/// A Pure busy-spin strategy which offers the lowest wait latency at the cost of increased
/// processor use.
#[derive(Copy, Clone, Debug)]
pub struct WaitBusy;

impl WaitStrategy for WaitBusy {
    #[inline]
    fn wait(&self) {
        // do absolutely nothing
    }
}

/// A busy-spin strategy which optimises processor use (see [`spin_loop`](std::hint::spin_loop) docs
/// for details) at the cost of latency.
#[derive(Copy, Clone, Debug)]
pub struct WaitBusyHint;

impl WaitStrategy for WaitBusyHint {
    #[inline]
    fn wait(&self) {
        std::hint::spin_loop()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct WaitYield;

impl WaitStrategy for WaitYield {
    #[inline]
    fn wait(&self) {
        std::thread::yield_now()
    }
}

#[derive(Copy, Clone, Debug)]
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
    fn wait(&self) {
        std::thread::sleep(self.duration)
    }
}

#[derive(Clone, Debug)]
pub struct WaitBlocking {
    pair: Arc<(Condvar, Mutex<Empty>)>,
    duration: Duration,
}

#[derive(Copy, Clone, Debug)]
struct Empty;

// All producers and consumers share the same condvar
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
    fn wait(&self) {
        let (condvar, mutex) = &*self.pair;
        // We don't need the mutex to do any work, so just make a new one on every call. This also
        // makes the unwrap okay, because no thread will attempt to lock twice, so we'll never get
        // an error from the call to `lock`.
        let _unused = condvar.wait_timeout(mutex.lock().unwrap(), self.duration);
    }

    #[inline]
    fn finalise(&self) {
        let (condvar, _) = &*self.pair;
        // waking everything allows the implementation to remain unaware of producers and consumers
        condvar.notify_all()
    }
}

pub struct WaitPhased<W> {
    spin_duration: Duration,
    yield_duration: Duration,
    fallback: W,
    timer: Mutex<Option<Instant>>,
    fallback_called: AtomicBool,
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
            timer: Mutex::new(None),
            fallback_called: AtomicBool::new(false),
        }
    }
}

impl<W> WaitStrategy for WaitPhased<W>
where
    W: WaitStrategy,
{
    #[inline]
    fn wait(&self) {
        // unwrap fine as the lock won't ever be called twice by the same thread
        let mut opt_timer = self.timer.lock().unwrap();
        let timer = opt_timer.get_or_insert_with(Instant::now);
        match timer.elapsed() {
            dur if dur < self.spin_duration => (),
            dur if dur < self.yield_duration => std::thread::yield_now(),
            _ => {
                self.fallback_called.store(true, Ordering::Relaxed);
                self.fallback.wait()
            }
        }
    }

    #[inline]
    fn finalise(&self) {
        let fallback_was_called = self.fallback_called.swap(false, Ordering::Relaxed);
        if fallback_was_called {
            self.fallback.finalise();
        }
        // unwrap fine as the lock won't ever be called twice by the same thread
        self.timer.lock().unwrap().take();
    }
}

#[derive(Clone, Debug)]
pub struct WaitPark {
    duration: Duration,
}

impl WaitPark {
    #[inline]
    pub fn new(duration: Duration) -> Self {
        WaitPark { duration }
    }
}

impl WaitStrategy for WaitPark {
    #[inline]
    fn wait(&self) {
        std::thread::park_timeout(self.duration)
    }
}
