use ansa::wait::WaitBusy;
use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion};
use disruptor::{BusySpin, Producer};
use std::time::Duration;

const COUNT_TO: i64 = 500_000_000;

#[derive(Copy, Clone, Default)]
struct Event {
    data: i64,
}

fn format_param() -> String {
    let num = COUNT_TO
        .to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap()
        .join(",");
    format!("count to: {}", num)
}

fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("Counting");
    group.measurement_time(Duration::from_secs(30));

    ansa(&mut group);
    disruptor(&mut group);
    crossbeam(&mut group);
}

fn ansa(group: &mut BenchmarkGroup<WallTime>) {
    const BATCH: i64 = 1024;
    const QUEUE: usize = 2_usize.pow(18);

    let id = BenchmarkId::new("ansa", format_param());
    group.bench_with_input(id, &COUNT_TO, |b, &count_to| {
        b.iter(|| {
            let (producer, consumer) = ansa::spsc(QUEUE, Event::default);
            let mut producer = producer.set_wait_strategy(WaitBusy);
            let mut consumer = consumer.set_wait_strategy(WaitBusy);

            let consumer_thread = std::thread::spawn(move || {
                let mut counter = 0;
                while counter < count_to {
                    consumer.wait(BATCH as usize).for_each(|event, _, _| counter = event.data);
                }
            });
            for _ in 0..(count_to + BATCH - 1) / BATCH {
                producer.wait(BATCH as usize).for_each(|event, seq, _| event.data = seq);
            }
            consumer_thread.join().expect("should not panic");
        })
    });
}

fn disruptor(group: &mut BenchmarkGroup<WallTime>) {
    const BATCH: i64 = 256;
    const QUEUE: usize = 2_usize.pow(12);

    let id = BenchmarkId::new("disruptor", format_param());
    group.bench_with_input(id, &COUNT_TO, |b, &count_to| {
        b.iter(|| {
            let processor = { move |state: &mut i64, event: &Event, _, _| *state = event.data };

            let mut producer = disruptor::build_single_producer(QUEUE, Event::default, BusySpin)
                .handle_events_and_state_with(processor, || 0i64)
                .build();

            let mut counter = 0;
            for _ in 0..(count_to + BATCH - 1) / BATCH {
                producer.batch_publish(BATCH as usize, |iter| {
                    iter.for_each(|event| {
                        event.data = counter;
                        counter += 1;
                    })
                });
            }
        })
    });
}

fn crossbeam(group: &mut BenchmarkGroup<WallTime>) {
    const BATCH: i64 = 256;
    const QUEUE: usize = 2_usize.pow(12);

    group.sample_size(10);

    let id = BenchmarkId::new("crossbeam", format_param());
    group.bench_with_input(id, &COUNT_TO, |b, &count_to| {
        b.iter(|| {
            let (sender, receiver) = crossbeam_channel::bounded::<Event>(QUEUE);
            let receiver_thread = {
                std::thread::spawn(move || {
                    let mut counter = 0;
                    while counter < count_to {
                        match receiver.try_recv() {
                            Ok(event) => counter = event.data,
                            Err(crossbeam_channel::TryRecvError::Empty) => continue,
                            Err(crossbeam_channel::TryRecvError::Disconnected) => (),
                        }
                    }
                })
            };
            let mut counter = 0;
            for _ in 0..(count_to + BATCH - 1) / BATCH {
                for _ in 0..BATCH {
                    let event = Event { data: counter };
                    while sender.try_send(event).is_err() {}
                    counter += 1;
                }
            }
            receiver_thread.join().expect("should not panic");
        })
    });
}

criterion_group!(benches, bench_count);
criterion_main!(benches);
