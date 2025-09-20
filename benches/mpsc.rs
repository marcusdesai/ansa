// The code in this file is adapted from the spsc.rs benchmark
// Modified to support multiple producers in separate threads

use ansa::wait::{WaitBusy, Waiting};
use criterion::measurement::WallTime;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use crossbeam_channel::{bounded, TryRecvError};
use disruptor::{BusySpin, Producer};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const QUEUE_SIZE: usize = 1024;
const BATCH_SIZES: [usize; 4] = [1, 10, 100, 256];
const RNG_SEED: u64 = 10;
const NUM_PRODUCERS: usize = 3;

#[derive(Copy, Clone, Default)]
struct Event {
    data: i64,
}

fn new_sink() -> Arc<AtomicI64> {
    Arc::new(AtomicI64::new(0))
}

fn new_counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

pub fn mpsc_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc");
    group.measurement_time(Duration::from_secs(1)).sample_size(100);

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
    let sink = new_sink();
    let counter = new_counter();

    let (s, r) = bounded::<Event>(QUEUE_SIZE);
    let receiver = {
        let sink = Arc::clone(&sink);
        let counter = Arc::clone(&counter);
        thread::spawn(move || loop {
            match r.try_recv() {
                Ok(event) => {
                    sink.store(event.data, Ordering::Release);
                    counter.fetch_add(1, Ordering::Release);
                }
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => break,
            }
        })
    };

    let benchmark_id = BenchmarkId::new("Crossbeam", &param);
    group.bench_with_input(benchmark_id, &batch, move |b, &batch_size| {
        b.iter_custom(|iters| {
            let start = Instant::now();

            for iter in 0..iters {
                // Create producer threads for this iteration
                let barrier = Arc::new(Barrier::new(NUM_PRODUCERS));
                let mut producers = Vec::new();

                for i in 0..NUM_PRODUCERS {
                    let sender = s.clone();
                    let barrier = Arc::clone(&barrier);

                    let handle = thread::spawn(move || {
                        // let mut rng = SmallRng::seed_from_u64(RNG_SEED + i as u64);

                        barrier.wait();

                        // let mut val = 0;
                        for _ in 0..batch_size {
                            // val = rng.random();
                            // let event = Event { data: rng.random() };
                            let event = Event { data: black_box(1) };
                            while sender.try_send(event).is_err() {}
                        }
                    });
                    producers.push(handle);
                }

                // Wait for all producers to finish
                for producer in producers {
                    producer.join().expect("Producer thread panicked");
                }

                // Wait for all events to be consumed
                let expected = (iter as usize + 1) * batch_size * NUM_PRODUCERS;
                while counter.load(Ordering::Acquire) < expected {}
            }

            start.elapsed()
        })
    });

    receiver.join().expect("Should not have panicked.");
}

fn disruptor(group: &mut BenchmarkGroup<WallTime>, batch: usize, param: &str) {
    let sink = new_sink();
    let counter = new_counter();

    let processor = {
        let sink = Arc::clone(&sink);
        let counter = Arc::clone(&counter);
        move |event: &Event, _sequence: i64, _end_of_batch: bool| {
            sink.store(event.data, Ordering::Release);
            counter.fetch_add(1, Ordering::Release);
        }
    };

    let producer = disruptor::build_multi_producer(QUEUE_SIZE, Event::default, BusySpin)
        .handle_events_with(processor)
        .build();

    let benchmark_id = BenchmarkId::new("disruptor", &param);
    group.bench_with_input(benchmark_id, &batch, move |b, &batch_size| {
        b.iter_custom(|iters| {
            let start = Instant::now();

            for iter in 0..iters {
                // Create producer threads for this iteration
                let barrier = Arc::new(Barrier::new(NUM_PRODUCERS));
                let mut producers = Vec::new();

                for i in 0..NUM_PRODUCERS {
                    let mut producer = producer.clone();
                    let barrier = Arc::clone(&barrier);

                    let handle = thread::spawn(move || {
                        // let mut rng = SmallRng::seed_from_u64(RNG_SEED + i as u64);

                        barrier.wait();

                        // let mut val = 0;
                        producer.batch_publish(batch_size, |iter| {
                            iter.for_each(|event| {
                                // val = rng.random();
                                // event.data = rng.random()
                                event.data = black_box(1)
                            })
                        });
                    });
                    producers.push(handle);
                }

                // Wait for all producers to finish
                for producer in producers {
                    producer.join().expect("Producer thread panicked");
                }

                // Wait for all events to be consumed
                let expected = (iter as usize + 1) * batch_size * NUM_PRODUCERS;
                while counter.load(Ordering::Acquire) < expected {}
            }

            start.elapsed()
        });
    });
}

fn ansa(group: &mut BenchmarkGroup<WallTime>, batch: usize, param: &str) {
    let sink = new_sink();
    let counter = new_counter();

    let (producer, consumer) = ansa::mpsc(QUEUE_SIZE, Event::default);
    let producer = producer.set_wait_strategy(WaitBusy);
    let mut consumer = consumer.set_wait_strategy(WaitBusy.with_timeout(Duration::from_millis(5)));

    let sink_clone = Arc::clone(&sink);
    let counter_clone = Arc::clone(&counter);
    let consumer_thread = thread::spawn(move || {
        while let Ok(events) = consumer.try_wait_range(1..) {
            events.for_each(|event, _, _| {
                sink_clone.store(event.data, Ordering::Release);
                counter_clone.fetch_add(1, Ordering::Release);
            })
        }
    });

    let benchmark_id = BenchmarkId::new("ansa", &param);
    group.bench_with_input(benchmark_id, &batch, move |b, &batch_size| {
        b.iter_custom(|iters| {
            let start = Instant::now();

            for iter in 0..iters {
                // Create producer threads for this iteration
                let barrier = Arc::new(Barrier::new(NUM_PRODUCERS));
                let mut producers = Vec::new();

                for i in 0..NUM_PRODUCERS {
                    let mut producer = producer.clone();
                    let barrier = Arc::clone(&barrier);

                    let handle = thread::spawn(move || {
                        // let mut rng = SmallRng::seed_from_u64(RNG_SEED + i as u64);

                        barrier.wait();

                        // let mut val = 0;
                        producer.wait_for_each(batch_size, |event, _, _| {
                            // val = rng.random();
                            // event.data = rng.random()
                            event.data = black_box(1)
                        });
                    });
                    producers.push(handle);
                }

                // Wait for all producers to finish
                for producer in producers {
                    producer.join().expect("Producer thread panicked");
                }

                // Wait for all events to be consumed
                let expected = (iter as usize + 1) * batch_size * NUM_PRODUCERS;
                while counter.load(Ordering::Acquire) < expected {}
            }

            start.elapsed()
        });
    });

    consumer_thread.join().expect("Should not have panicked.");
}

criterion_group!(mpsc, mpsc_benchmark);
criterion_main!(mpsc);
