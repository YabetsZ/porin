//! Compile-fail tests for `quota!`. Compiler wording changes between
//! releases, so these run only in CI's pinned-toolchain job:
//! `cargo +1.99.1 test --test ui -- --ignored`.

#[test]
#[ignore = "compiler output is toolchain-specific; run with the pinned toolchain"]
fn quota_macro_rejects_bad_notation() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
