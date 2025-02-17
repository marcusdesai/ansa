mod common;

#[test]
fn test_lead_multi_producer() {
    let num_of_events = 360;
    common::run_test_lead(
        num_of_events,
        |handles| handles.take_lead().unwrap().into_multi(),
        |multi| {
            let mut funcs = Vec::new();
            for mut producer in [multi.clone(), multi.clone(), multi] {
                funcs.push(move || {
                    for _ in 0..num_of_events / (20 * 3) {
                        producer.wait_apply(20, |i, seq, _| *i = seq)
                    }
                })
            }
            funcs
        },
    );
}

#[test]
fn test_trailing_multi_producer() {
    let num_of_events = 360;
    common::run_test_trailing(
        num_of_events,
        |id, handles| handles.take_producer(id).unwrap().into_multi(),
        |multi| {
            let mut funcs = Vec::new();
            for mut producer in [multi.clone(), multi.clone(), multi] {
                funcs.push(move || {
                    for _ in 0..num_of_events / (20 * 3) {
                        producer.wait_apply(20, |i, _, _| *i = 0)
                    }
                })
            }
            funcs
        },
    );
}
