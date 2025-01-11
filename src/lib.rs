mod builder;
mod handles;
mod ringbuffer;
pub mod wait;

pub use builder::*;
pub use handles::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    // Integration tests

    #[test]
    fn test_single_producer() {
        let (mut producer, consumers) = DisruptorBuilder::new(64, || 0i64)
            .add_consumer(0, Follows::Producer)
            .wait_strategy(|| {
                WaitPhased::new(Duration::from_millis(1), Duration::new(1, 0), WaitBusyHint)
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
                        should_continue = !event.consumer_break;
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
                        should_continue = !event.consumer_break;
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
                        should_continue = !event.consumer_break;
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

    // if running miri, requires MIRIFLAGS="-Zmiri-disable-isolation" to be set
    #[test]
    fn test_wait_blocking() {
        let (mut producer, mut consumers) = DisruptorBuilder::new(32, || 0i64)
            .add_consumer(0, Follows::Producer)
            .add_consumer(1, Follows::Consumer(0))
            .wait_strategy(WaitBlocking::new)
            .build_single_producer();

        let consumer_0 = consumers.remove(&0).unwrap();
        let consumer_1 = consumers.remove(&1).unwrap();

        let num_of_events = 100;
        let c0_counter = Arc::new(AtomicI64::new(0));
        let c1_counter = Arc::new(AtomicI64::new(0));

        let join_p = std::thread::spawn(move || {
            let mut counter = 0;
            while counter < num_of_events {
                producer.batch_write(10, |i, seq, _| {
                    counter += 1;
                    *i = seq;
                });
                // add some time just to make sure the consumers end up waiting
                std::thread::sleep(Duration::from_micros(10));
            }
        });

        let c0_out = Arc::clone(&c0_counter);
        let join_c0 = std::thread::spawn(move || {
            let mut should_continue = true;
            while should_continue {
                consumer_0.read(|_, seq, _| {
                    let prev = c0_out.fetch_add(1, Ordering::Relaxed);
                    println!("c0 {}; out {}", seq, prev + 1);
                    should_continue = seq < num_of_events;
                });
            }
        });

        let c1_out = Arc::clone(&c1_counter);
        let join_c1 = std::thread::spawn(move || {
            let mut should_continue = true;
            while should_continue {
                consumer_1.batch_read(5, |_, seq, _| {
                    let prev = c1_out.fetch_add(1, Ordering::Relaxed);
                    println!("c1 {}; out {}", seq, prev + 1);
                    should_continue = seq < num_of_events;
                });
            }
        });

        #[cfg(not(miri))]
        let loop_timeout = Duration::from_millis(100);
        // miri interprets the code and thus takes much longer to run than even debug builds. So we
        // make the timeout significantly longer to account for this.
        #[cfg(miri)]
        let loop_timeout = Duration::from_secs(10);

        let timer = Instant::now();
        while !join_c1.is_finished() {
            if timer.elapsed() > loop_timeout {
                break;
            }
        }

        // fail if while loop was timed out, as this will usually indicate deadlock
        assert!(join_c1.is_finished(), "took too long");

        // join to ensure the threads finish all their work before we read the counters.
        join_p.join().expect("done p");
        join_c0.join().expect("done c0");
        join_c1.join().expect("done c1");

        assert_eq!(c0_counter.load(Ordering::Relaxed), num_of_events);
        assert_eq!(c1_counter.load(Ordering::Relaxed), num_of_events);
    }
}
