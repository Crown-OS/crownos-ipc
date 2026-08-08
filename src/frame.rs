use crate::error::Error;

/// Hard cap on a frame payload; bulk data travels out-of-band via fds.
pub const MAX_FRAME: u32 = 1 << 20;
pub const MAX_FDS_PER_MSG: usize = 16;
pub const HEADER_LEN: usize = 20;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Expects a `Reply` or `Error` with the same `req_id`.
    Request = 0,
    /// Fire-and-forget; never answered.
    Notify = 1,
    Reply = 2,
    /// Payload is a postcard-encoded [`crate::RemoteError`].
    Error = 3,
    /// `selector` names the event to subscribe to; acked with an empty `Reply`.
    Subscribe = 4,
    Unsubscribe = 5,
    /// Server push; `req_id` is 0.
    Event = 6,
}

impl Kind {
    fn from_u8(v: u8) -> Option<Kind> {
        Some(match v {
            0 => Kind::Request,
            1 => Kind::Notify,
            2 => Kind::Reply,
            3 => Kind::Error,
            4 => Kind::Subscribe,
            5 => Kind::Unsubscribe,
            6 => Kind::Event,
            _ => return None,
        })
    }
}

/// Fixed 20-byte little-endian wire header:
/// `len u32 | kind u8 | fd_count u8 | reserved u16 | selector u64 | req_id u32`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub len: u32,
    pub kind: Kind,
    pub fd_count: u8,
    pub selector: u64,
    pub req_id: u32,
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&self.len.to_le_bytes());
        buf[4] = self.kind as u8;
        buf[5] = self.fd_count;
        // bytes 6..8 stay zero (reserved)
        buf[8..16].copy_from_slice(&self.selector.to_le_bytes());
        buf[16..20].copy_from_slice(&self.req_id.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8; HEADER_LEN]) -> Result<Header, Error> {
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if len > MAX_FRAME {
            return Err(Error::Malformed("frame exceeds size cap"));
        }
        let kind = Kind::from_u8(buf[4]).ok_or(Error::Malformed("unknown message kind"))?;
        let fd_count = buf[5];
        if usize::from(fd_count) > MAX_FDS_PER_MSG {
            return Err(Error::Malformed("too many fds"));
        }
        if buf[6] != 0 || buf[7] != 0 {
            return Err(Error::Malformed("nonzero reserved bytes"));
        }
        let selector = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        let req_id = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
        Ok(Header { len, kind, fd_count, selector, req_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Header {
        Header { len: 42, kind: Kind::Request, fd_count: 3, selector: 0xdead_beef_cafe_f00d, req_id: 7 }
    }

    #[test]
    fn round_trip() {
        let h = sample();
        let decoded = Header::decode(&h.encode()).expect("decode");
        assert_eq!(h, decoded);
    }

    #[test]
    fn rejects_oversize() {
        let mut h = sample();
        h.len = MAX_FRAME + 1;
        assert!(matches!(Header::decode(&h.encode()), Err(Error::Malformed(_))));
    }

    #[test]
    fn rejects_unknown_kind() {
        let mut buf = sample().encode();
        buf[4] = 200;
        assert!(matches!(Header::decode(&buf), Err(Error::Malformed(_))));
    }

    #[test]
    fn rejects_fd_overflow() {
        let mut buf = sample().encode();
        buf[5] = 17;
        assert!(matches!(Header::decode(&buf), Err(Error::Malformed(_))));
    }

    #[test]
    fn rejects_reserved_bits() {
        let mut buf = sample().encode();
        buf[6] = 1;
        assert!(matches!(Header::decode(&buf), Err(Error::Malformed(_))));
    }
}
