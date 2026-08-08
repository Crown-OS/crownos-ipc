//! Compile-time FNV-1a 64 hashing for method/event selectors.

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub const fn fnv1a_64(bytes: &[u8]) -> u64 {
    fnv1a_64_continue(FNV_OFFSET, bytes)
}

pub const fn fnv1a_64_continue(mut state: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        state ^= bytes[i] as u64;
        state = state.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    state
}

/// Selector for a method: hashes service name, method name and the source
/// spelling of its signature, so any drift in arguments or return type
/// yields a different selector and fails loudly as `UNKNOWN_METHOD`.
pub const fn selector(service: &str, method: &str, signature: &str) -> u64 {
    let mut h = fnv1a_64(service.as_bytes());
    h = fnv1a_64_continue(h, &[0]);
    h = fnv1a_64_continue(h, method.as_bytes());
    h = fnv1a_64_continue(h, &[0]);
    fnv1a_64_continue(h, signature.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x85944171f73967e8);
    }

    #[test]
    fn selector_changes_on_drift() {
        let base = selector("svc", "method", "(String, u8) -> ()");
        assert_ne!(base, selector("svc", "method", "(String, u16) -> ()"));
        assert_ne!(base, selector("svc", "method2", "(String, u8) -> ()"));
        assert_ne!(base, selector("svc2", "method", "(String, u8) -> ()"));
    }

    #[test]
    fn separator_prevents_ambiguity() {
        assert_ne!(selector("ab", "c", "d"), selector("a", "bc", "d"));
    }
}
