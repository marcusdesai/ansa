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
const BATCH_SIZES: [usize; 4] = [1, 10, 100, 256];
const RNG_SEED: u64 = 10;

#[derive(Copy, Clone, Default)]
struct Event {
    data: i64,
}

fn new_sink() -> Arc<AtomicI64> {
    Arc::new(AtomicI64::new(0))
}

pub fn spsc_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc");
    group.measurement_time(Duration::from_secs(5)).sample_size(500);

    for batch_size in BATCH_SIZES.into_iter() {
        group.throughput(Throughput::Elements(batch_size as u64));
        let param = format!("batch size: {}", batch_size);
        crossbeam(&mut group, batch_size, &param);
        disruptor(&mut group, batch_size, &param);
        ansa(&mut group, batch_size, &param);
    }
    group.finish();
}

fn crossbeam(group: &mut BenchmarkGroup<WallTime>, batch: usize, param: &str) {
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
    group.bench_with_input(benchmark_id, &batch, move |b, &batch_size| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let mut val = 0;
                for _ in 0..batch_size {
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

fn disruptor(group: &mut BenchmarkGroup<WallTime>, batch: usize, param: &str) {
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
    group.bench_with_input(benchmark_id, &batch, move |b, &batch_size| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let mut val = 0;
                producer.batch_publish(batch_size, |iter| {
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

fn ansa(group: &mut BenchmarkGroup<WallTime>, batch: usize, param: &str) {
    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let sink = new_sink();

    let (producer, consumer) = ansa::spsc(QUEUE_SIZE, Event::default);
    let mut producer = producer.set_wait_strategy(WaitBusy);
    let mut consumer = consumer.set_wait_strategy(WaitBusy.with_timeout(Duration::from_millis(5)));

    let sink_clone = Arc::clone(&sink);
    let consumer_thread = thread::spawn(move || {
        while let Ok(events) = consumer.try_wait_range(1..) {
            events.for_each(|event, _, _| sink_clone.store(event.data, Ordering::Release))
        }
    });

    let benchmark_id = BenchmarkId::new("ansa", &param);
    group.bench_with_input(benchmark_id, &batch, move |b, &batch_size| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let mut val = 0;
                producer.wait(batch_size).for_each(|event, _, _| {
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
