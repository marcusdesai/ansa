mod common;

#[test]
fn test_lead_multi_producer() {
    let num_of_events = 360;
    common::run_test_lead(
        num_of_events,
        |handles| handles.take_lead().unwrap().into_multi(),
        |multi| {
            [multi.clone(), multi.clone(), multi].into_iter().map(|mut clone| {
                move || {
                    for _ in 0..num_of_events / (20 * 3) {
                        clone.wait_for_each(20, |i, seq, _| *i = seq)
                    }
                }
            })
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
            [multi.clone(), multi.clone(), multi].into_iter().map(|mut clone| {
                move || {
                    for _ in 0..num_of_events / (20 * 3) {
                        clone.wait_for_each(20, |i, _, _| *i = 0)
                    }
                }
            })
        },
    );
}
