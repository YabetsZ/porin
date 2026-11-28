//! Turning keys into bytes for shared stores.
//!
//! The in-memory store only needs `K: Hash + Eq`. Stores shared between
//! processes, such as Redis, need every key as bytes, and two different keys
//! must never produce the same bytes, or their limits would merge.
//! [`KeyEncode`] provides that encoding.

use std::borrow::Cow;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::rc::Rc;
use std::sync::Arc;

/// A stable, collision-free byte encoding of a key.
///
/// # Contract
///
/// For any two values `a` and `b` of the same type:
///
/// - **Injective:** if `a != b`, their encodings differ.
/// - **Self-delimiting:** one encoding is never a prefix of another. This is
///   what lets tuples and structs simply concatenate their fields'
///   encodings: `("ab", "c")` and `("a", "bc")` stay distinct.
/// - **Stable:** the same value encodes the same way on every platform, in
///   every process and in every future version, since shared stores keep
///   keys around. porin treats a change to its own encodings as a breaking
///   change.
///
/// The provided implementations cover integers, `bool`, `char`, strings,
/// byte and other slices, arrays, `Option`, tuples of up to four elements,
/// IP addresses, and references and smart pointers to any of these.
///
/// # Implementing it
///
/// Encode each field, in a fixed order, with its own `KeyEncode`
/// implementation. That keeps the contract, because every provided encoding
/// is self-delimiting.
///
/// ```
/// use porin::KeyEncode;
///
/// #[derive(PartialEq, Eq, Hash)]
/// struct TenantRoute {
///     tenant: u64,
///     route: String,
/// }
///
/// impl KeyEncode for TenantRoute {
///     fn encode_key(&self, out: &mut Vec<u8>) {
///         self.tenant.encode_key(out);
///         self.route.encode_key(out);
///     }
/// }
///
/// let key = TenantRoute { tenant: 7, route: "/search".into() };
/// assert_eq!(key.key_bytes(), (7_u64, "/search").key_bytes());
/// ```
pub trait KeyEncode {
    /// Appends this key's encoding to `out`.
    fn encode_key(&self, out: &mut Vec<u8>);

    /// Returns this key's encoding as a new buffer.
    fn key_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_key(&mut out);
        out
    }
}

/// Appends `value` as an unsigned LEB128 varint, which is self-delimiting:
/// every byte but the last has its high bit set.
fn encode_len(value: usize, out: &mut Vec<u8>) {
    // usize is at most 64 bits on every supported platform.
    let mut value = u64::try_from(value).unwrap_or(u64::MAX);
    loop {
        let low = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(low);
            return;
        }
        // `low` is below 0x80, so this only sets the continuation bit.
        out.push(low | 0x80);
    }
}

/// Fixed-width big-endian integers: every value of a type has the same
/// length, so the encoding is self-delimiting.
macro_rules! fixed_width {
    ($($int:ty),*) => {$(
        impl KeyEncode for $int {
            fn encode_key(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_be_bytes());
            }
        }
    )*};
}

fixed_width!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

/// Encoded as a `u64`, so the bytes are the same on 32- and 64-bit platforms.
impl KeyEncode for usize {
    fn encode_key(&self, out: &mut Vec<u8>) {
        // usize is at most 64 bits on every platform Rust supports.
        (*self as u64).encode_key(out);
    }
}

/// Encoded as an `i64`, so the bytes are the same on 32- and 64-bit platforms.
impl KeyEncode for isize {
    fn encode_key(&self, out: &mut Vec<u8>) {
        // isize is at most 64 bits on every platform Rust supports.
        (*self as i64).encode_key(out);
    }
}

/// The one key of a limiter with a single, global limit, such as a
/// `Limiter<(), _, RedisStore<_>>` shared by every instance. It encodes to
/// nothing.
impl KeyEncode for () {
    fn encode_key(&self, _out: &mut Vec<u8>) {}
}

impl KeyEncode for bool {
    fn encode_key(&self, out: &mut Vec<u8>) {
        out.push(u8::from(*self));
    }
}

impl KeyEncode for char {
    fn encode_key(&self, out: &mut Vec<u8>) {
        u32::from(*self).encode_key(out);
    }
}

/// The byte length as a varint, then the UTF-8 bytes.
impl KeyEncode for str {
    fn encode_key(&self, out: &mut Vec<u8>) {
        encode_len(self.len(), out);
        out.extend_from_slice(self.as_bytes());
    }
}

impl KeyEncode for String {
    fn encode_key(&self, out: &mut Vec<u8>) {
        self.as_str().encode_key(out);
    }
}

/// The element count as a varint, then each element. For `[u8]` that is the
/// length followed by the raw bytes.
impl<T: KeyEncode> KeyEncode for [T] {
    fn encode_key(&self, out: &mut Vec<u8>) {
        encode_len(self.len(), out);
        for item in self {
            item.encode_key(out);
        }
    }
}

impl<T: KeyEncode> KeyEncode for Vec<T> {
    fn encode_key(&self, out: &mut Vec<u8>) {
        self.as_slice().encode_key(out);
    }
}

/// Each element in order, with no length: every value of `[T; N]` has `N`
/// elements, so the encoding is already self-delimiting.
impl<T: KeyEncode, const N: usize> KeyEncode for [T; N] {
    fn encode_key(&self, out: &mut Vec<u8>) {
        for item in self {
            item.encode_key(out);
        }
    }
}

/// A `0` byte for `None`, or a `1` byte followed by the value.
impl<T: KeyEncode> KeyEncode for Option<T> {
    fn encode_key(&self, out: &mut Vec<u8>) {
        match self {
            None => out.push(0),
            Some(value) => {
                out.push(1);
                value.encode_key(out);
            }
        }
    }
}

macro_rules! tuple {
    ($($name:ident),+) => {
        /// Each element in order.
        impl<$($name: KeyEncode),+> KeyEncode for ($($name,)+) {
            #[expect(non_snake_case, reason = "the bindings reuse the type parameter names")]
            fn encode_key(&self, out: &mut Vec<u8>) {
                let ($($name,)+) = self;
                $($name.encode_key(out);)+
            }
        }
    };
}

tuple!(A);
tuple!(A, B);
tuple!(A, B, C);
tuple!(A, B, C, D);

impl KeyEncode for Ipv4Addr {
    fn encode_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.octets());
    }
}

impl KeyEncode for Ipv6Addr {
    fn encode_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.octets());
    }
}

/// A `4` or `6` byte, then the address octets.
impl KeyEncode for IpAddr {
    fn encode_key(&self, out: &mut Vec<u8>) {
        match self {
            Self::V4(address) => {
                out.push(4);
                address.encode_key(out);
            }
            Self::V6(address) => {
                out.push(6);
                address.encode_key(out);
            }
        }
    }
}

impl<T: KeyEncode + ?Sized> KeyEncode for &T {
    fn encode_key(&self, out: &mut Vec<u8>) {
        (**self).encode_key(out);
    }
}

impl<T: KeyEncode + ?Sized> KeyEncode for Box<T> {
    fn encode_key(&self, out: &mut Vec<u8>) {
        (**self).encode_key(out);
    }
}

impl<T: KeyEncode + ?Sized> KeyEncode for Arc<T> {
    fn encode_key(&self, out: &mut Vec<u8>) {
        (**self).encode_key(out);
    }
}

impl<T: KeyEncode + ?Sized> KeyEncode for Rc<T> {
    fn encode_key(&self, out: &mut Vec<u8>) {
        (**self).encode_key(out);
    }
}

impl<T> KeyEncode for Cow<'_, T>
where
    T: KeyEncode + ToOwned + ?Sized,
{
    fn encode_key(&self, out: &mut Vec<u8>) {
        self.as_ref().encode_key(out);
    }
}
