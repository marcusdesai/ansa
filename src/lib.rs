mod builder;
mod handles;
mod ringbuffer;
mod wait;

pub use builder::*;
pub use handles::*;
pub use wait::*;

#[cfg(test)]
mod tests {
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

        let consumer = consumers.pop().unwrap();

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
        let (producer, consumers) = DisruptorBuilder::new(64, || 0i64)
            .add_consumer(0, Follows::Producer)
            .add_consumer(1, Follows::Producer)
            .add_consumer(2, Follows::Consumer(0))
            .add_consumer(3, Follows::Consumers(vec![1, 2]))
            .wait_strategy(|| WaitYield)
            .build_multi_producer();
    }
}
