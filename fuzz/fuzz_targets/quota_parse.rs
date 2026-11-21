//! Parsing never panics, and anything that parses survives a Display roundtrip.
#![no_main]

use libfuzzer_sys::fuzz_target;
use porin::Quota;

fuzz_target!(|notation: &str| {
    if let Ok(quota) = notation.parse::<Quota>() {
        let text = quota.to_string();
        assert_eq!(text.parse::<Quota>(), Ok(quota), "roundtrip via {text:?}");
    }
});
