mod common;

const BATCH: u32 = 16;

#[test]
fn test_lead_exact_multi_producer() {
    let num_of_events = 240;
    common::run_test_lead(
        num_of_events,
        |handles| handles.take_lead().unwrap().into_exact_multi::<BATCH>().unwrap(),
        |multi| {
            let mut funcs = Vec::new();
            for mut producer in [multi.clone(), multi.clone(), multi] {
                funcs.push(move || {
                    for _ in 0..num_of_events / (BATCH * 3) {
                        producer.wait().write(|i, seq, _| *i = seq)
                    }
                })
            }
            funcs
        },
    );
}

#[test]
fn test_trailing_exact_multi_producer() {
    let num_of_events = 240;
    common::run_test_trailing(
        num_of_events,
        |id, handles| handles.take_producer(id).unwrap().into_exact_multi::<BATCH>().unwrap(),
        |multi| {
            let mut funcs = Vec::new();
            for mut producer in [multi.clone(), multi.clone(), multi] {
                funcs.push(move || {
                    for _ in 0..num_of_events / (BATCH * 3) {
                        producer.wait().write(|i, _, _| *i = 0)
                    }
                })
            }
            funcs
        },
    );
}
