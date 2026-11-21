//! Key encodings: collision-free, self-delimiting and stable.
#![cfg(test)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use porin::KeyEncode;
use proptest::prelude::*;

/// Fails if two different values share an encoding, or if one encoding is a
/// prefix of another. A small alphabet in the strategies makes near-misses
/// such as `("ab", "c")` vs `("a", "bc")` common.
fn assert_injective_and_prefix_free<T>(values: &[T]) -> Result<(), TestCaseError>
where
    T: KeyEncode + Eq + Debug,
{
    let mut seen: HashMap<Vec<u8>, &T> = HashMap::new();
    for value in values {
        let bytes = value.key_bytes();
        if let Some(previous) = seen.insert(bytes.clone(), value) {
            prop_assert_eq!(previous, value, "same encoding {:?}", bytes);
        }
    }
    let encodings: Vec<_> = seen.keys().collect();
    for a in &encodings {
        for b in &encodings {
            prop_assert!(
                a == b || !b.starts_with(a),
                "{:?} is a prefix of {:?}",
                a,
                b
            );
        }
    }
    Ok(())
}

fn small_string() -> impl Strategy<Value = String> {
    "[ab]{0,3}"
}

fn ip() -> impl Strategy<Value = IpAddr> {
    prop_oneof![
        any::<[u8; 4]>().prop_map(|octets| IpAddr::V4(Ipv4Addr::from(octets))),
        any::<[u8; 16]>().prop_map(|octets| IpAddr::V6(Ipv6Addr::from(octets))),
        // An IPv4 address and its IPv4-mapped IPv6 form are different keys.
        any::<[u8; 4]>().prop_map(|octets| IpAddr::V6(Ipv4Addr::from(octets).to_ipv6_mapped())),
    ]
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn strings_and_string_tuples(
        strings in prop::collection::vec(small_string(), 0..40),
        pairs in prop::collection::vec((small_string(), small_string()), 0..40),
        triples in prop::collection::vec((small_string(), small_string(), small_string()), 0..40),
    ) {
        assert_injective_and_prefix_free(&strings)?;
        assert_injective_and_prefix_free(&pairs)?;
        assert_injective_and_prefix_free(&triples)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn byte_vectors_options_and_nesting(
        bytes in prop::collection::vec(prop::collection::vec(0..3_u8, 0..4), 0..40),
        options in prop::collection::vec(prop::option::of(small_string()), 0..40),
        nested in prop::collection::vec(prop::collection::vec(small_string(), 0..3), 0..40),
    ) {
        assert_injective_and_prefix_free(&bytes)?;
        assert_injective_and_prefix_free(&options)?;
        assert_injective_and_prefix_free(&nested)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn mixed_tuples_integers_and_ips(
        tenants in prop::collection::vec((any::<u64>(), small_string()), 0..40),
        signed in prop::collection::vec((any::<i32>(), any::<bool>(), any::<char>()), 0..40),
        platform in prop::collection::vec((any::<isize>(), any::<usize>()), 0..40),
        ips in prop::collection::vec(ip(), 0..40),
        lengths in prop::collection::vec(prop::collection::vec(any::<u8>(), 120..140), 0..10),
        chars in prop::collection::vec(any::<char>(), 0..40),
        arrays in prop::collection::vec(any::<[u16; 2]>(), 0..40),
    ) {
        assert_injective_and_prefix_free(&tenants)?;
        assert_injective_and_prefix_free(&signed)?;
        assert_injective_and_prefix_free(&platform)?;
        assert_injective_and_prefix_free(&ips)?;
        // Lengths around 128 cross the one-to-two-byte varint boundary.
        assert_injective_and_prefix_free(&lengths)?;
        assert_injective_and_prefix_free(&chars)?;
        assert_injective_and_prefix_free(&arrays)?;
    }
}

#[test]
fn borrowed_and_owned_forms_encode_the_same() {
    let owned = String::from("tenant-7");
    assert_eq!(owned.key_bytes(), "tenant-7".key_bytes());
    assert_eq!(
        std::sync::Arc::<str>::from("tenant-7").key_bytes(),
        owned.key_bytes()
    );
    assert_eq!(
        std::borrow::Cow::Borrowed("tenant-7").key_bytes(),
        owned.key_bytes()
    );
    assert_eq!(vec![1_u8, 2].key_bytes(), [1_u8, 2][..].key_bytes());
    let boxed: Box<u32> = Box::new(7);
    assert_eq!(boxed.key_bytes(), 7_u32.key_bytes());
    assert_eq!(std::rc::Rc::new(7_u32).key_bytes(), 7_u32.key_bytes());
}

/// These bytes may already live in someone's Redis. Changing any of them is a
/// breaking change and needs a deliberate migration, not a test update.
#[test]
fn encodings_are_stable() {
    let cases: [(&str, Vec<u8>, &[u8]); 12] = [
        ("u32", 0x0102_0304_u32.key_bytes(), &[1, 2, 3, 4]),
        ("i16", (-2_i16).key_bytes(), &[0xff, 0xfe]),
        ("usize", 1_usize.key_bytes(), &[0, 0, 0, 0, 0, 0, 0, 1]),
        ("isize", (-1_isize).key_bytes(), &[0xff; 8]),
        ("bool", true.key_bytes(), &[1]),
        ("char", 'é'.key_bytes(), &[0, 0, 0, 0xe9]),
        ("array", [1_u8, 2].key_bytes(), &[1, 2]),
        ("str", "ab".key_bytes(), &[2, b'a', b'b']),
        (
            "long str",
            "x".repeat(200).key_bytes()[..2].to_vec(),
            &[0xc8, 0x01],
        ),
        ("option", Some(5_u8).key_bytes(), &[1, 5]),
        ("tuple", (1_u8, "a").key_bytes(), &[1, 1, b'a']),
        (
            "ipv4",
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)).key_bytes(),
            &[4, 10, 0, 0, 1],
        ),
    ];
    for (name, actual, expected) in cases {
        assert_eq!(actual, expected, "{name}");
    }
}
