use ansa::wait::WaitBusy;
use std::hint::black_box;
use std::time::Instant;

#[derive(Copy, Clone, Default)]
struct Event {
    data: i64,
}

fn main() {
    let batch = 1024_i64;
    let queue = 2_usize.pow(18);
    let num = black_box(1_000_000_000);

    let (producer, consumer) = ansa::spsc(queue, Event::default);
    let mut producer = producer.set_wait_strategy(WaitBusy);
    let mut consumer = consumer.set_wait_strategy(WaitBusy);

    let consumer_thread = std::thread::spawn(move || {
        let mut counter = 0;
        while counter < num {
            consumer.wait(batch as usize).for_each(|event, _, _| counter = event.data);
        }
        counter
    });

    let start = Instant::now();
    for _ in 0..(num + batch - 1) / batch {
        producer.wait(batch as usize).for_each(|event, seq, _| event.data = seq);
    }
    let counter = consumer_thread.join().expect("should not panic");
    let elapsed = start.elapsed();

    assert!(counter >= num);
    println!("elapsed: {}", elapsed.as_millis());
    println!("counter: {}", counter);
}
