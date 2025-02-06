use ansa::wait::WaitStrategy;
use ansa::{Barrier, DisruptorBuilder, DisruptorHandles, Follows, Handle};
use std::time::{Duration, Instant};

// Any test that doesn't finish within this limit is likely infinitely looping, and thus has failed
#[cfg(not(miri))]
const TIME_LIMIT: Duration = Duration::from_millis(100);
#[cfg(miri)]
const TIME_LIMIT: Duration = Duration::from_secs(10);

// Simulates WaitBusy and allows us to avoid hanging threads by limiting wait times
#[derive(Copy, Clone, Debug)]
pub struct WaitPanic;

unsafe impl WaitStrategy for WaitPanic {
    // busy wait but panic after time limit
    fn wait(&self, desired_seq: i64, barrier: &Barrier) -> i64 {
        let timer = Instant::now();
        loop {
            let barrier_seq = barrier.sequence();
            if barrier_seq >= desired_seq {
                break barrier_seq;
            }
            if timer.elapsed() > TIME_LIMIT {
                panic!("time limit exceeded in wait")
            }
        }
    }
}

/// All producers must write the sequence number to each event.
pub fn run_test_lead<P: Send, I, F>(
    num_of_events: u32,
    make_producer: impl FnOnce(&mut DisruptorHandles<i64, WaitPanic>) -> P,
    producer_funcs: impl Fn(P) -> I,
) where
    I: IntoIterator<Item = F>,
    F: FnMut() + Send,
{
    assert_eq!(num_of_events % 20, 0);
    let mut handles = DisruptorBuilder::new(64, || 0i64)
        .add_handle(0, Handle::Consumer, Follows::LeadProducer)
        .wait_strategy(WaitPanic)
        .build()
        .unwrap();

    let mut result = Vec::new();
    std::thread::scope(|s| {
        let mut join_handles = Vec::new();

        let lead = make_producer(&mut handles);
        for mut func in producer_funcs(lead) {
            let jh = s.spawn(move || func());
            join_handles.push(jh);
        }

        let out = &mut result;
        let mut consumer = handles.take_consumer(0).unwrap();
        let jh = s.spawn(move || {
            for _ in 0..num_of_events / 20 {
                consumer.wait(20).read(|i, _, _| out.push(*i))
            }
        });
        join_handles.push(jh);

        let timer = Instant::now();
        while timer.elapsed() < TIME_LIMIT {
            if join_handles.iter().all(|jh| jh.is_finished()) {
                break;
            }
        }
        for jh in join_handles {
            assert!(jh.is_finished(), "test time limit reached");
            jh.join().expect("done");
        }
    });

    let expected: Vec<_> = (0..num_of_events as i64).collect();
    assert_eq!(result, expected);
}

/// All producers must write zero to each event.
pub fn run_test_trailing<P: Send, I, F>(
    num_of_events: u32,
    make_producer: impl FnOnce(u64, &mut DisruptorHandles<i64, WaitPanic>) -> P,
    producer_funcs: impl Fn(P) -> I,
) where
    I: IntoIterator<Item = F>,
    F: FnMut() + Send,
{
    assert_eq!(num_of_events % 20, 0);
    let trailing_id = 1;
    let mut handles = DisruptorBuilder::new(64, || 0i64)
        .add_handle(0, Handle::Consumer, Follows::LeadProducer)
        .add_handle(trailing_id, Handle::Producer, Follows::Handles(vec![0]))
        .add_handle(2, Handle::Consumer, Follows::Handles(vec![1]))
        .wait_strategy(WaitPanic)
        .build()
        .unwrap();

    let mut results_seq = Vec::new();
    let mut results_blank = Vec::new();
    std::thread::scope(|s| {
        let mut join_handles = Vec::new();

        let mut lead = handles.take_lead().unwrap();
        let jh = s.spawn(move || {
            for _ in 0..num_of_events / 20 {
                lead.wait(20).write(|i, seq, _| *i = seq)
            }
        });
        join_handles.push(jh);

        let trailing = make_producer(trailing_id, &mut handles);
        for mut func in producer_funcs(trailing) {
            let jh = s.spawn(move || func());
            join_handles.push(jh);
        }

        let consumers = [
            (&mut results_seq, handles.take_consumer(0).unwrap()),
            (&mut results_blank, handles.take_consumer(2).unwrap()),
        ];
        for (out, mut consumer) in consumers {
            let jh = s.spawn(move || {
                for _ in 0..num_of_events / 20 {
                    consumer.wait(20).read(|i, _, _| out.push(*i))
                }
            });
            join_handles.push(jh);
        }

        let timer = Instant::now();
        while timer.elapsed() < TIME_LIMIT {
            if join_handles.iter().all(|jh| jh.is_finished()) {
                break;
            }
        }
        for jh in join_handles {
            assert!(jh.is_finished(), "test time limit reached");
            jh.join().expect("done");
        }
    });

    let expected_seqs: Vec<_> = (0..num_of_events as i64).collect();
    assert_eq!(results_seq, expected_seqs);

    let expected_blanks = vec![0; num_of_events as usize];
    assert_eq!(results_blank, expected_blanks);
}
