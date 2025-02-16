mod common;

#[test]
fn test_lead_producer() {
    let num_of_events = 320;
    common::run_test_lead(
        num_of_events,
        |handles| handles.take_lead().unwrap(),
        |mut producer| {
            [move || {
                for _ in 0..num_of_events / 16 {
                    producer.wait(16).apply(|i, seq, _| *i = seq)
                }
            }]
        },
    );
}

#[test]
fn test_trailing_producer() {
    let num_of_events = 320;
    common::run_test_trailing(
        num_of_events,
        |id, handles| handles.take_producer(id).unwrap(),
        |mut producer| {
            [move || {
                for _ in 0..num_of_events / 16 {
                    producer.wait(16).apply(|i, _, _| *i = 0)
                }
            }]
        },
    );
}
