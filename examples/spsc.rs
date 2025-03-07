use ansa::wait::WaitBusy;
use std::hint::black_box;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Copy, Clone, Default)]
struct Event {
    data: i64,
}

fn main() {
    let sink = Arc::new(AtomicI64::new(0));

    let batch = 1024_i64;
    let queue = 2_usize.pow(18);
    let num = black_box(1_000_000_000);

    let (producer, consumer) = ansa::spsc(queue, Event::default);
    let mut producer = producer.set_wait_strategy(WaitBusy);
    let mut consumer = consumer.set_wait_strategy(WaitBusy);

    let consumer_thread = {
        let sink = Arc::clone(&sink);
        std::thread::spawn(move || {
            let mut end = false;
            while !end {
                consumer.wait(batch as u32).for_each(|event, seq, _| {
                    sink.store(event.data, Ordering::Release);
                    end = seq >= num - 1;
                });
            }
        })
    };

    let start = Instant::now();
    for _ in 0..(num + batch - 1) / batch {
        producer.wait(batch as u32).for_each(|event, seq, _| event.data = seq);
    }
    while !consumer_thread.is_finished() {}
    let end = start.elapsed();

    println!("{}", end.as_millis());
    println!("{}", sink.load(Ordering::Acquire));
}
