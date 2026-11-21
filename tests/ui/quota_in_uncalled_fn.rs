// The error must fire even when the function is never called.
#[allow(dead_code)]
fn never_called() -> porin::Quota {
    porin::quota!("10/second; 20/second")
}

fn main() {}
