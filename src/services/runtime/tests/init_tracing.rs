use service_runtime::init_tracing;

#[test]
fn double_call_does_not_panic() {
    init_tracing();
    init_tracing();
}
