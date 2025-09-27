use ansa::wait::WaitBusy;
use std::hint::black_box;
use std::time::Instant;

#[derive(Copy, Clone, Default)]
struct Event {
    data: u32,
}

fn main() {
    let batch = 2048;
    let queue = 2_usize.pow(17);
    let num = black_box(1_000_000_000);

    let (producer, consumer) = ansa::spsc(queue, Event::default);
    let mut producer = producer.set_wait_strategy(WaitBusy);
    let mut consumer = consumer.set_wait_strategy(WaitBusy);

    let consumer_thread = std::thread::Builder::new()
        .name("spsc consumer".to_owned())
        .spawn(move || {
            let mut counter = 0;
            while counter < num {
                consumer.wait(batch as usize).for_each(|event, _, _| counter = event.data);
            }
            counter
        })
        .expect("thread spawned");

    let start = Instant::now();
    for _ in 0..(num + batch - 1) / batch {
        producer.wait(batch as usize).for_each(|event, seq, _| event.data = seq as u32);
    }
    let seq_reached = consumer_thread.join().expect("should not panic");
    let elapsed = start.elapsed();

    assert!(seq_reached >= num);
    println!("      elapsed ms: {}", elapsed.as_millis());
    println!("sequence reached: {}", seq_reached);
}
