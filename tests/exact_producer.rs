mod common;

const BATCH: u32 = 16;

#[test]
fn test_lead_exact_producer() {
    let num_of_events = 320;
    common::run_test_lead(
        num_of_events,
        |handles| handles.take_lead().unwrap().into_exact::<BATCH>().unwrap(),
        |mut producer| {
            [move || {
                for _ in 0..num_of_events / BATCH {
                    producer.wait().write(|i, seq, _| *i = seq)
                }
            }]
        },
    );
}

#[test]
fn test_trailing_exact_producer() {
    let num_of_events = 320;
    common::run_test_trailing(
        num_of_events,
        |id, handles| handles.take_producer(id).unwrap().into_exact::<BATCH>().unwrap(),
        |mut producer| {
            [move || {
                for _ in 0..num_of_events / BATCH {
                    producer.wait().write(|i, _, _| *i = 0)
                }
            }]
        },
    );
}
