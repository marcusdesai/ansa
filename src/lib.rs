mod builder;
mod handles;
mod ringbuffer;
mod wait;

pub use builder::*;
pub use handles::*;
pub use wait::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use crate::{DisruptorBuilder, Follows, WaitBusyHint, WaitPhased, WaitYield};
    use std::time::Duration;

    // Integration tests

    #[test]
    fn test_single_producer() {
        let (mut producer, mut consumers) = DisruptorBuilder::new(64, || 0i64)
            .add_consumer(0, Follows::Producer)
            .wait_strategy(|| {
                WaitPhased::new(Duration::new(1, 0), Duration::new(1, 0), WaitBusyHint)
            })
            .build_single_producer();

        let num_of_events = 200;
        let mut result = vec![0i64; num_of_events + 1];

        std::thread::scope(|s| {
            s.spawn(move || {
                let mut counter = 0;
                while counter < num_of_events {
                    producer.batch_write(20, |i, seq, _| {
                        counter += 1;
                        *i = seq;
                    })
                }
            });
            let out = &mut result;
            let consumer = consumers.get(&0).unwrap();
            s.spawn(move || {
                let mut counter = 0;
                while counter < num_of_events {
                    consumer.batch_read(20, |i, seq, _| {
                        counter += 1;
                        out[seq as usize] = *i;
                    })
                }
            });
        });

        let expected: Vec<_> = (0..=200).collect();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_complex() {
        #[derive(Default, Copy, Clone)]
        struct Event {
            consumer_break: bool,
            seq: i64,
        }

        let (mut producer, mut consumers) = DisruptorBuilder::new(128, Event::default)
            .add_consumer(0, Follows::Producer)
            .add_consumer(1, Follows::Producer)
            .add_consumer(2, Follows::Consumer(0))
            .add_consumer(3, Follows::Consumers(vec![1, 2]))
            .wait_strategy(|| WaitYield)
            .build_single_producer();

        // vec of all written events
        let mut consumer_0_out = vec![];
        // count of all correctly assign seq values to events, should == seq
        let consumer_1_seq_increment_counter = Arc::new(AtomicU32::new(0));
        // counter for how many times consumer 2's read func is called, should == seq
        let consumer_2_call_counter = Arc::new(AtomicU32::new(0));
        // remains true while consumer 3 reads that the above 2 counters are >= seq that it sees
        let mut consumer_3_check_flag = true;

        let num_of_events = 300;

        std::thread::scope(|s| {
            s.spawn(move || {
                let mut counter = 0;
                while counter < num_of_events {
                    producer.batch_write(20, |event, seq, _| {
                        counter += 1;
                        event.seq = seq;
                        if seq == num_of_events {
                            event.consumer_break = true;
                        }
                    })
                }
            });

            let consumer_0 = consumers.get(&0).unwrap();
            let out = &mut consumer_0_out;
            s.spawn(move || {
                let mut should_continue = true;
                while should_continue {
                    consumer_0.read(|event, seq, _| {
                        out.push(*event);
                    });
                }
            });
        });
    }
}
