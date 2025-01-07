use std::cell::RefCell;
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
#[derive(Copy, Clone)]
pub struct WaitBusy;

impl WaitStrategy for WaitBusy {
    #[inline]
    fn wait(&self) {
        // do absolutely nothing
    }
}

/// A busy-spin strategy which optimises processor use (see [`spin_loop`](std::hint::spin_loop) docs
/// for details) at the cost of latency.
#[derive(Copy, Clone)]
pub struct WaitBusyHint;

impl WaitStrategy for WaitBusyHint {
    #[inline]
    fn wait(&self) {
        std::hint::spin_loop()
    }
}

#[derive(Copy, Clone)]
pub struct WaitYield;

impl WaitStrategy for WaitYield {
    #[inline]
    fn wait(&self) {
        std::thread::yield_now()
    }
}

#[derive(Copy, Clone)]
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

#[derive(Clone)]
pub struct WaitBlocking {
    condvar: Arc<Condvar>,
}

// All producers and consumers share the same condvar
static BLOCK_VAR: LazyLock<Arc<Condvar>> = LazyLock::new(|| Arc::new(Condvar::new()));

impl WaitBlocking {
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        WaitBlocking {
            condvar: Arc::clone(&BLOCK_VAR),
        }
    }
}

impl WaitStrategy for WaitBlocking {
    #[inline]
    fn wait(&self) {
        // We don't need the mutex to do any work, so just make a new one on every call. This also
        // makes the unwrap okay, because no thread will attempt to lock twice, so we'll never get
        // an error from the call to `lock`.
        let mutex = Mutex::new(());
        let _unused = self.condvar.wait(mutex.lock().unwrap());
    }

    #[inline]
    fn finalise(&self) {
        // waking everything allows the implementation to remain unaware of producers and consumers
        self.condvar.notify_all()
    }
}

pub struct WaitPhased<W> {
    spin_duration: Duration,
    yield_duration: Duration,
    fallback: W,
    timer: RefCell<Option<Instant>>,
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
            timer: RefCell::new(None),
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
        let mut opt_timer = self.timer.borrow_mut();
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
        self.timer.replace(None);
    }
}
