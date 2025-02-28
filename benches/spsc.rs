// The code in this file is adapted from the following (MIT licensed) work:
// https://github.com/nicholassm/disruptor-rs/blob/bd15292e34d2c4bb53cba5709e0cf23e9753ebb8/benches/spsc.rs
// authored by: Nicholas Schultz-Møller

use ansa::wait::{WaitBusy, Waiting};
use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use crossbeam_channel::{bounded, TryRecvError};
use disruptor::{BusySpin, Producer};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const QUEUE_SIZE: usize = 1024;
const BURST_SIZES: [u64; 1] = [128];
// const BURST_SIZES: [u64; 3] = [1, 10, 100];
const PAUSES_MS: [u64; 1] = [0];
// const PAUSES_MS: [u64; 3] = [0, 1, 10];

const RNG_SEED: u64 = 10;

#[derive(Copy, Clone, Default)]
struct Event {
    data: i64,
}

fn pause(millis: u64) {
    if millis > 0 {
        thread::sleep(Duration::from_millis(millis));
    }
}

fn new_sink() -> Arc<AtomicI64> {
    Arc::new(AtomicI64::new(0))
}

pub fn spsc_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc");

    for burst_size in BURST_SIZES.into_iter() {
        group.throughput(Throughput::Elements(burst_size));

        base(&mut group, burst_size as i64);

        for pause_ms in PAUSES_MS.into_iter() {
            let inputs = (burst_size as i64, pause_ms);
            let param = format!("burst: {}, pause: {} ms", burst_size, pause_ms);

            crossbeam(&mut group, inputs, &param);
            disruptor(&mut group, inputs, &param);
            ansa(&mut group, inputs, &param);
        }
    }
    group.finish();
}

fn base(group: &mut BenchmarkGroup<WallTime>, burst_size: i64) {
    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let sink = new_sink();

    let benchmark_id = BenchmarkId::new("base", burst_size);
    group.bench_with_input(benchmark_id, &burst_size, move |b, &size| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let mut val = 0;
                for _ in 0..size {
                    val = rng.random();
                    sink.store(val, Ordering::Release);
                }
                while sink.load(Ordering::Acquire) != val {}
            }
            start.elapsed()
        })
    });
}

fn crossbeam(group: &mut BenchmarkGroup<WallTime>, inputs: (i64, u64), param: &str) {
    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let sink = new_sink();

    let (s, r) = bounded::<Event>(QUEUE_SIZE);
    let receiver = {
        let sink = Arc::clone(&sink);
        thread::spawn(move || loop {
            match r.try_recv() {
                Ok(event) => sink.store(event.data, Ordering::Release),
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => break,
            }
        })
    };

    let benchmark_id = BenchmarkId::new("Crossbeam", &param);
    group.bench_with_input(benchmark_id, &inputs, move |b, &(size, pause_ms)| {
        b.iter_custom(|iters| {
            pause(pause_ms);
            let start = Instant::now();
            for _ in 0..iters {
                let mut val = 0;
                for _ in 0..size {
                    val = rng.random();
                    let event = Event { data: val };
                    while s.try_send(event).is_err() {}
                }
                while sink.load(Ordering::Acquire) != val {}
            }
            start.elapsed()
        })
    });

    receiver.join().expect("Should not have panicked.");
}

fn disruptor(group: &mut BenchmarkGroup<WallTime>, inputs: (i64, u64), param: &str) {
    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let sink = new_sink();

    let processor = {
        let sink = Arc::clone(&sink);
        move |event: &Event, _sequence: i64, _end_of_batch: bool| {
            sink.store(event.data, Ordering::Release);
        }
    };

    let mut producer = disruptor::build_single_producer(QUEUE_SIZE, Event::default, BusySpin)
        .handle_events_with(processor)
        .build();

    let benchmark_id = BenchmarkId::new("disruptor", &param);
    group.bench_with_input(benchmark_id, &inputs, move |b, &(size, pause_ms)| {
        b.iter_custom(|iters| {
            pause(pause_ms);
            let start = Instant::now();
            for _ in 0..iters {
                let mut val = 0;
                producer.batch_publish(size as usize, |iter| {
                    iter.for_each(|event| {
                        val = rng.random();
                        event.data = val
                    })
                });
                while sink.load(Ordering::Acquire) != val {}
            }
            start.elapsed()
        });
    });
}

fn ansa(group: &mut BenchmarkGroup<WallTime>, inputs: (i64, u64), param: &str) {
    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let sink = new_sink();

    let (producer, consumer) = ansa::spsc(QUEUE_SIZE, Event::default);
    let mut producer = producer.set_wait_strategy(WaitBusy);
    let mut consumer = consumer.set_wait_strategy(WaitBusy.with_timeout(Duration::from_millis(20)));

    let sink_clone = Arc::clone(&sink);
    let consumer_thread = thread::spawn(move || {
        while let Ok(events) = consumer.try_wait_range(1..) {
            events.apply(|event, _, _| sink_clone.store(event.data, Ordering::Release))
        }
    });

    let benchmark_id = BenchmarkId::new("ansa", &param);
    group.bench_with_input(benchmark_id, &inputs, move |b, &(size, pause_ms)| {
        b.iter_custom(|iters| {
            pause(pause_ms);
            let start = Instant::now();
            for _ in 0..iters {
                let mut val = 0;
                producer.wait(size as u32).apply_mut(|event, _, _| {
                    val = rng.random();
                    event.data = val
                });
                while sink.load(Ordering::Acquire) != val {}
            }
            start.elapsed()
        });
    });

    consumer_thread.join().expect("Should not have panicked.");
}

criterion_group!(spsc, spsc_benchmark);
criterion_main!(spsc);
