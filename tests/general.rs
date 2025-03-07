use ansa::wait::{WaitBusy, WaitYield};
use ansa::*;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[test]
fn test_complex_consumer_dag() {
    #[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
    struct Event {
        consumer_break: bool,
        seq: i64,
    }

    let mut handles = DisruptorBuilder::new(128, Event::default)
        .add_handle(0, Handle::Consumer, Follows::LeadProducer)
        .add_handle(1, Handle::Consumer, Follows::LeadProducer)
        .add_handle(2, Handle::Consumer, Follows::Handles(vec![0]))
        .add_handle(3, Handle::Consumer, Follows::Handles(vec![1, 2]))
        .wait_strategy(WaitYield)
        .build()
        .unwrap();

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
        let mut producer = handles.take_lead().unwrap();
        s.spawn(move || {
            let mut should_continue = true;
            while should_continue {
                producer.wait(20).for_each(|event, seq, _| {
                    event.seq = seq;
                    if seq == num_of_events - 1 {
                        event.consumer_break = true;
                        should_continue = false;
                    }
                })
            }
        });

        let mut consumer_0 = handles.take_consumer(0).unwrap();
        let out = &mut consumer_0_out;
        s.spawn(move || {
            let mut should_continue = true;
            while should_continue {
                consumer_0.wait_range(1..).for_each(|event, _, _| {
                    out.push(*event);
                    should_continue = !event.consumer_break;
                });
            }
        });

        let mut consumer_1 = handles.take_consumer(1).unwrap();
        let c1_counter = Arc::clone(&consumer_1_seq_increment_counter);
        s.spawn(move || {
            let mut should_continue = true;
            while should_continue {
                consumer_1.wait(10).for_each(|event, seq, _| {
                    if event.seq == seq {
                        c1_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    should_continue = !event.consumer_break;
                })
            }
        });

        let mut consumer_2 = handles.take_consumer(2).unwrap();
        let c2_counter = Arc::clone(&consumer_2_call_counter);
        s.spawn(move || {
            let mut should_continue = true;
            while should_continue {
                consumer_2.wait_range(1..).for_each(|event, _, _| {
                    c2_counter.fetch_add(1, Ordering::Relaxed);
                    should_continue = !event.consumer_break;
                })
            }
        });

        let mut consumer_3 = handles.take_consumer(3).unwrap();
        let c1_counter = Arc::clone(&consumer_1_seq_increment_counter);
        let c2_counter = Arc::clone(&consumer_2_call_counter);
        let c3_flag = &mut consumer_3_check_flag;
        s.spawn(move || {
            let mut should_continue = true;
            while should_continue {
                consumer_3.wait(20).for_each(|event, seq, _| {
                    // both counters should be at or ahead of the seq value this consumer sees
                    *c3_flag = c1_counter.load(Ordering::Relaxed) >= seq;
                    *c3_flag = c2_counter.load(Ordering::Relaxed) >= seq;
                    should_continue = !event.consumer_break;
                })
            }
        });
    });

    let mut consumer_0_out_expected: Vec<_> = (0..num_of_events)
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

#[test]
fn test_producer_conversions() {
    #[derive(Default)]
    struct Event {
        seq: i64,
        seq_times_2: i64,
    }

    let mut handles = DisruptorBuilder::new(128, Event::default)
        .add_handle(0, Handle::Producer, Follows::LeadProducer)
        .add_handle(1, Handle::Consumer, Follows::Handles(vec![0]))
        .wait_strategy(WaitBusy)
        .build()
        .unwrap();

    let num_of_events = 300;
    let mut is_times_2 = true;

    std::thread::scope(|s| {
        let lead = handles.take_lead().unwrap();
        s.spawn(move || {
            let mut producer = lead;
            for _ in 0..2 {
                producer.wait(50).for_each(|event, seq, _| event.seq = seq)
            }
            let multi = producer.into_multi();
            let mut joins = vec![];
            for mut multi_producer in [multi.clone(), multi] {
                let join = std::thread::spawn(move || {
                    for _ in 0..5 {
                        multi_producer.wait_for_each(10, |event, seq, _| event.seq = seq)
                    }
                    multi_producer
                });
                joins.push(join)
            }
            let mut producer = joins
                .into_iter()
                .map(|h| h.join().expect("done"))
                .fold(None, |opt, i| opt.or(i.into_producer()))
                .expect("single");
            for _ in 0..2 {
                producer.wait(50).for_each(|event, seq, _| event.seq = seq)
            }
        });

        let trailing = handles.take_producer(0).unwrap();
        s.spawn(move || {
            let multi = trailing.into_multi();
            let mut joins = vec![];
            for mut multi_producer in [multi.clone(), multi.clone(), multi] {
                let join = std::thread::spawn(move || {
                    for _ in 0..5 {
                        multi_producer
                            .wait_for_each(10, |event, _, _| event.seq_times_2 = event.seq * 2)
                    }
                    multi_producer
                });
                joins.push(join)
            }
            let mut producer = joins
                .into_iter()
                .map(|h| h.join().expect("done"))
                .fold(None, |opt, i| opt.or(i.into_producer()))
                .expect("single");
            for _ in 0..3 {
                producer.wait(50).for_each(|event, _, _| event.seq_times_2 = event.seq * 2)
            }
        });

        let mut consumer = handles.take_consumer(1).unwrap();
        s.spawn(move || {
            let mut counter = 0;
            while counter < num_of_events {
                consumer.wait(20).for_each(|event, seq, _| {
                    counter += 1;
                    is_times_2 = is_times_2 && event.seq == seq && event.seq_times_2 == seq * 2;
                })
            }
        });
    });

    assert!(is_times_2);
}
