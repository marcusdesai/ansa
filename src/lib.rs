mod builder;
mod handles;
mod ringbuffer;
mod wait;

pub use builder::*;
pub use handles::*;
pub use wait::*;

#[cfg(test)]
mod tests {
    use crate::{DisruptorBuilder, Follows, WaitBusy, WaitBusyHint, WaitPhased, WaitYield};
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    // Integration tests

    #[test]
    fn test_single_producer() {
        let (mut producer, consumers) = DisruptorBuilder::new(64, || 0i64)
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
    fn test_multi_producer() {
        let size = 512;
        let (multi_1, mut consumers) = DisruptorBuilder::new(size, || 0i64)
            .add_consumer(0, Follows::Producer)
            .wait_strategy(|| WaitBusy)
            .build_multi_producer();

        let multi_2 = multi_1.clone();
        let multi_3 = multi_1.clone();

        let consumer = consumers.remove(&0).unwrap();

        let sync_barrier = Arc::new(std::sync::Barrier::new(4));
        let done = Arc::new(AtomicI64::new(0));
        let mut result = vec![0i64; size];
        let publish_amount = 100;

        std::thread::scope(|s| {
            for mut producer in [multi_1, multi_2, multi_3] {
                let sb = Arc::clone(&sync_barrier);
                let dc = Arc::clone(&done);
                s.spawn(move || {
                    sb.wait();
                    let mut counter = 0;
                    while counter < 2 {
                        counter += 1;
                        producer.batch_write(publish_amount / 2, |i, seq, _| *i = seq);
                    }
                    dc.fetch_add(1, Ordering::Relaxed);
                });
            }

            let out = &mut result;
            s.spawn(move || {
                sync_barrier.wait();
                let mut counter = 0;
                while counter != publish_amount * 3 {
                    consumer.read(|i, seq, _| {
                        counter += 1;
                        out[seq as usize] = *i;
                    })
                }
            });
        });

        assert_eq!(done.load(Ordering::Relaxed), 3);

        let mut expected = vec![0i64; size];
        for i in 0..=publish_amount * 3 {
            expected[i as usize] = i as i64
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn test_complex_consumer_dag() {
        #[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
        struct Event {
            consumer_break: bool,
            seq: i64,
        }

        let (mut producer, consumers) = DisruptorBuilder::new(128, Event::default)
            .add_consumer(0, Follows::Producer)
            .add_consumer(1, Follows::Producer)
            .add_consumer(2, Follows::Consumer(0))
            .add_consumer(3, Follows::Consumers(vec![1, 2]))
            .wait_strategy(|| WaitYield)
            .build_single_producer();

        // vec of all written events
        let mut consumer_0_out = vec![];
        // count of all correctly assign seq values to events, should == seq
        let consumer_1_seq_increment_counter = Arc::new(AtomicI64::new(0));
        // counter for how many times consumer 2's read func is called, should == seq
        let consumer_2_call_counter = Arc::new(AtomicI64::new(0));
        // remains true while consumer 3 reads that the above 2 counters are >= seq value it sees
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
                    consumer_0.read(|event, _, _| {
                        out.push(*event);
                        if event.consumer_break {
                            should_continue = false;
                        }
                    });
                }
            });

            let consumer_1 = consumers.get(&1).unwrap();
            let c1_counter = Arc::clone(&consumer_1_seq_increment_counter);
            s.spawn(move || {
                let mut should_continue = true;
                while should_continue {
                    consumer_1.batch_read(10, |event, seq, _| {
                        if event.seq == seq {
                            c1_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        if event.consumer_break {
                            should_continue = false;
                        }
                    })
                }
            });

            let consumer_2 = consumers.get(&2).unwrap();
            let c2_counter = Arc::clone(&consumer_2_call_counter);
            s.spawn(move || {
                let mut should_continue = true;
                while should_continue {
                    consumer_2.read(|event, _, _| {
                        c2_counter.fetch_add(1, Ordering::Relaxed);
                        if event.consumer_break {
                            should_continue = false;
                        }
                    })
                }
            });

            let consumer_3 = consumers.get(&3).unwrap();
            let c1_counter = Arc::clone(&consumer_1_seq_increment_counter);
            let c2_counter = Arc::clone(&consumer_2_call_counter);
            let c3_flag = &mut consumer_3_check_flag;
            s.spawn(move || {
                let mut should_continue = true;
                while should_continue {
                    consumer_3.batch_read(20, |event, seq, _| {
                        // both counters should be at or ahead of the seq value this consumer sees
                        *c3_flag = c1_counter.load(Ordering::Relaxed) >= seq;
                        *c3_flag = c2_counter.load(Ordering::Relaxed) >= seq;
                        should_continue = !event.consumer_break;
                    })
                }
            });
        });

        // checks
        let mut consumer_0_out_expected: Vec<_> = (1..=num_of_events)
            .map(|i| Event {
                consumer_break: false,
                seq: i,
            })
            .collect();
        consumer_0_out_expected.last_mut().unwrap().consumer_break = true;

        assert_eq!(consumer_0_out, consumer_0_out_expected);
        assert_eq!(
            consumer_1_seq_increment_counter.load(Ordering::Relaxed),
            num_of_events
        );
        assert_eq!(
            consumer_2_call_counter.load(Ordering::Relaxed),
            num_of_events
        );
        assert!(consumer_3_check_flag)
    }
}
