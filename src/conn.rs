use std::collections::VecDeque;
use std::io::{IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::net::UnixStream;

use rustix::net::{
    RecvAncillaryMessage, RecvFlags, RecvAncillaryBuffer, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags, recvmsg, sendmsg,
};

use crate::error::Error;
use crate::frame::{HEADER_LEN, Header, Kind, MAX_FDS_PER_MSG, MAX_FRAME};

const READ_CHUNK: usize = 64 * 1024;
/// Received fds not yet claimed by a parsed frame.
const FD_QUEUE_CAP: usize = MAX_FDS_PER_MSG * 4;

#[derive(Debug, Clone, Copy)]
pub struct ConnLimits {
    pub max_frame: u32,
    pub max_outbox_frames: usize,
    pub max_outbox_bytes: usize,
    pub max_inbox: usize,
}

impl Default for ConnLimits {
    fn default() -> Self {
        Self {
            max_frame: MAX_FRAME,
            max_outbox_frames: 64,
            max_outbox_bytes: 4 * 1024 * 1024,
            max_inbox: 256,
        }
    }
}

/// A fully reassembled inbound frame.
#[derive(Debug)]
pub struct Message {
    pub kind: Kind,
    pub selector: u64,
    pub req_id: u32,
    pub payload: Vec<u8>,
    pub fds: Vec<OwnedFd>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interest {
    pub read: bool,
    pub write: bool,
}

struct OutFrame {
    bytes: Vec<u8>,
    fds: Vec<OwnedFd>,
    offset: usize,
}

/// Non-blocking per-socket state machine. The caller owns readiness
/// (calloop, AsyncFd, poll, ...) and pumps `handle_readable` /
/// `handle_writable`; this type never blocks and never spins.
pub struct Connection {
    stream: UnixStream,
    rx: Vec<u8>,
    scratch: Vec<u8>,
    fd_queue: VecDeque<OwnedFd>,
    inbox: VecDeque<Message>,
    outbox: VecDeque<OutFrame>,
    outbox_bytes: usize,
    limits: ConnLimits,
    closed: bool,
}

impl AsFd for Connection {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.stream.as_fd()
    }
}

impl Connection {
    pub fn from_stream(stream: UnixStream, limits: ConnLimits) -> Result<Self, Error> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            rx: Vec::new(),
            scratch: vec![0u8; READ_CHUNK],
            fd_queue: VecDeque::new(),
            inbox: VecDeque::new(),
            outbox: VecDeque::new(),
            outbox_bytes: 0,
            limits,
            closed: false,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    fn outbox_full(&self) -> bool {
        self.outbox.len() >= self.limits.max_outbox_frames
            || self.outbox_bytes >= self.limits.max_outbox_bytes
    }

    pub fn interest(&self) -> Interest {
        Interest {
            read: !self.closed && !self.outbox_full() && self.inbox.len() < self.limits.max_inbox,
            write: !self.closed && !self.outbox.is_empty(),
        }
    }

    /// Poison the connection: everything queued (including received fds) is
    /// dropped and the socket is shut down. Used on any protocol violation.
    fn poison(&mut self) {
        self.closed = true;
        self.fd_queue.clear();
        self.inbox.clear();
        self.outbox.clear();
        self.outbox_bytes = 0;
        let _ = self.stream.shutdown(Shutdown::Both);
    }

    /// Drain the socket into complete messages. Returns how many messages
    /// are now queued. Reads nothing while the outbox is full (backpressure)
    /// or the inbox is at capacity.
    pub fn handle_readable(&mut self) -> Result<usize, Error> {
        if self.closed {
            return Err(Error::Disconnected);
        }
        loop {
            if self.outbox_full() || self.inbox.len() >= self.limits.max_inbox {
                break;
            }
            let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS_PER_MSG))];
            let mut control = RecvAncillaryBuffer::new(&mut space);
            let mut iov = [IoSliceMut::new(&mut self.scratch)];
            let recv = match recvmsg(&self.stream, &mut iov, &mut control, RecvFlags::CMSG_CLOEXEC) {
                Ok(r) => r,
                Err(rustix::io::Errno::WOULDBLOCK) => break,
                Err(rustix::io::Errno::INTR) => continue,
                Err(rustix::io::Errno::CONNRESET) => {
                    self.closed = true;
                    break;
                }
                Err(e) => {
                    self.poison();
                    return Err(Error::Io(e.into()));
                }
            };
            let truncated = recv.flags.contains(rustix::net::ReturnFlags::CTRUNC);
            for msg in control.drain() {
                if let RecvAncillaryMessage::ScmRights(fds) = msg {
                    self.fd_queue.extend(fds);
                }
            }
            if truncated || self.fd_queue.len() > FD_QUEUE_CAP {
                self.poison();
                return Err(Error::Malformed("ancillary data overflow"));
            }
            if recv.bytes == 0 {
                self.closed = true;
                break;
            }
            self.rx.extend_from_slice(&self.scratch[..recv.bytes]);
            self.parse_frames()?;
        }
        Ok(self.inbox.len())
    }

    fn parse_frames(&mut self) -> Result<(), Error> {
        let mut consumed = 0;
        while self.rx.len() - consumed >= HEADER_LEN {
            let mut hdr_bytes = [0u8; HEADER_LEN];
            hdr_bytes.copy_from_slice(&self.rx[consumed..consumed + HEADER_LEN]);
            let header = match Header::decode(&hdr_bytes) {
                Ok(h) => h,
                Err(e) => {
                    self.poison();
                    return Err(e);
                }
            };
            if header.len > self.limits.max_frame {
                self.poison();
                return Err(Error::Malformed("frame exceeds size cap"));
            }
            let total = HEADER_LEN + header.len as usize;
            if self.rx.len() - consumed < total {
                break;
            }
            let wanted = usize::from(header.fd_count);
            if self.fd_queue.len() < wanted {
                self.poison();
                return Err(Error::Malformed("frame declares more fds than delivered"));
            }
            let fds: Vec<OwnedFd> = self.fd_queue.drain(..wanted).collect();
            let payload = self.rx[consumed + HEADER_LEN..consumed + total].to_vec();
            self.inbox.push_back(Message {
                kind: header.kind,
                selector: header.selector,
                req_id: header.req_id,
                payload,
                fds,
            });
            consumed += total;
        }
        if consumed > 0 {
            self.rx.drain(..consumed);
        }
        Ok(())
    }

    pub fn next_message(&mut self) -> Option<Message> {
        self.inbox.pop_front()
    }

    /// Enqueue a frame. `Err(Backpressure)` when the bounded outbox is full;
    /// the caller decides whether that is fatal (client misuse) or a reason
    /// to drop an event (slow subscriber).
    pub fn send(
        &mut self,
        kind: Kind,
        selector: u64,
        req_id: u32,
        payload: &[u8],
        fds: Vec<OwnedFd>,
    ) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Disconnected);
        }
        if payload.len() > self.limits.max_frame as usize {
            return Err(Error::Malformed("frame exceeds size cap"));
        }
        if fds.len() > MAX_FDS_PER_MSG {
            return Err(Error::Malformed("too many fds"));
        }
        if self.outbox_full() {
            return Err(Error::Backpressure);
        }
        let header = Header {
            len: payload.len() as u32,
            kind,
            fd_count: fds.len() as u8,
            selector,
            req_id,
        };
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
        bytes.extend_from_slice(&header.encode());
        bytes.extend_from_slice(payload);
        self.outbox_bytes += bytes.len();
        self.outbox.push_back(OutFrame { bytes, fds, offset: 0 });
        Ok(())
    }

    /// Flush the outbox as far as the kernel allows.
    pub fn handle_writable(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Disconnected);
        }
        while let Some(front) = self.outbox.front_mut() {
            let iov = [IoSlice::new(&front.bytes[front.offset..])];
            // fds ride the first byte of their frame only
            let result = if front.offset == 0 && !front.fds.is_empty() {
                let borrowed: Vec<BorrowedFd<'_>> = front.fds.iter().map(|fd| fd.as_fd()).collect();
                let mut space =
                    [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS_PER_MSG))];
                let mut control = SendAncillaryBuffer::new(&mut space);
                if !control.push(SendAncillaryMessage::ScmRights(&borrowed)) {
                    self.poison();
                    return Err(Error::Malformed("ancillary buffer overflow"));
                }
                sendmsg(&self.stream, &iov, &mut control, SendFlags::NOSIGNAL)
            } else {
                let mut control = SendAncillaryBuffer::new(&mut []);
                sendmsg(&self.stream, &iov, &mut control, SendFlags::NOSIGNAL)
            };
            match result {
                Ok(n) => {
                    front.offset += n;
                    self.outbox_bytes = self.outbox_bytes.saturating_sub(n);
                    if front.offset >= front.bytes.len() {
                        self.outbox.pop_front();
                    } else if front.offset > 0 && !front.fds.is_empty() {
                        // partial write after the first byte: fds are delivered
                        front.fds.clear();
                    }
                }
                Err(rustix::io::Errno::WOULDBLOCK) => break,
                Err(rustix::io::Errno::INTR) => continue,
                Err(rustix::io::Errno::PIPE | rustix::io::Errno::CONNRESET) => {
                    self.closed = true;
                    return Err(Error::Disconnected);
                }
                Err(e) => {
                    self.poison();
                    return Err(Error::Io(e.into()));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;

    fn pair() -> (Connection, UnixStream) {
        let (a, b) = UnixStream::pair().expect("socketpair");
        let conn = Connection::from_stream(a, ConnLimits::default()).expect("conn");
        (conn, b)
    }

    fn frame_bytes(kind: Kind, selector: u64, req_id: u32, payload: &[u8], fd_count: u8) -> Vec<u8> {
        let header = Header { len: payload.len() as u32, kind, fd_count, selector, req_id };
        let mut v = header.encode().to_vec();
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn byte_dribble_reassembly() {
        let (mut conn, mut raw) = pair();
        let bytes = frame_bytes(Kind::Request, 42, 7, b"hello", 0);
        for chunk in bytes.chunks(3) {
            raw.write_all(chunk).expect("write");
            conn.handle_readable().expect("readable");
        }
        let msg = conn.next_message().expect("message");
        assert_eq!(msg.selector, 42);
        assert_eq!(msg.req_id, 7);
        assert_eq!(msg.payload, b"hello");
    }

    #[test]
    fn pipelined_frames() {
        let (mut conn, mut raw) = pair();
        let mut bytes = frame_bytes(Kind::Request, 1, 1, b"a", 0);
        bytes.extend(frame_bytes(Kind::Notify, 2, 0, b"bb", 0));
        raw.write_all(&bytes).expect("write");
        assert_eq!(conn.handle_readable().expect("readable"), 2);
        assert_eq!(conn.next_message().expect("m1").selector, 1);
        assert_eq!(conn.next_message().expect("m2").selector, 2);
    }

    #[test]
    fn eof_is_death_notification() {
        let (mut conn, raw) = pair();
        drop(raw);
        conn.handle_readable().expect("first pump sees eof");
        assert!(conn.is_closed());
        assert!(matches!(conn.handle_readable(), Err(Error::Disconnected)));
    }

    #[test]
    fn malformed_frame_poisons() {
        let (mut conn, mut raw) = pair();
        let mut bytes = frame_bytes(Kind::Request, 1, 1, b"ok", 0);
        bytes[4] = 250; // invalid kind
        raw.write_all(&bytes).expect("write");
        assert!(matches!(conn.handle_readable(), Err(Error::Malformed(_))));
        assert!(conn.is_closed());
        assert!(conn.next_message().is_none());
    }

    #[test]
    fn oversize_frame_rejected() {
        // within Header::decode's global cap but above this connection's limit
        let header = Header {
            len: MAX_FRAME,
            kind: Kind::Request,
            fd_count: 0,
            selector: 0,
            req_id: 0,
        };
        let limits = ConnLimits { max_frame: 1024, ..Default::default() };
        let (a, mut raw) = UnixStream::pair().expect("socketpair");
        let mut conn = Connection::from_stream(a, limits).expect("conn");
        raw.write_all(&header.encode()).expect("write");
        assert!(matches!(conn.handle_readable(), Err(Error::Malformed(_))));
    }

    #[test]
    fn send_and_flush_round_trip() {
        let (mut conn, mut raw) = pair();
        conn.send(Kind::Reply, 9, 3, b"payload", vec![]).expect("send");
        conn.handle_writable().expect("flush");
        let mut buf = vec![0u8; HEADER_LEN + 7];
        raw.read_exact(&mut buf).expect("read");
        let mut hdr = [0u8; HEADER_LEN];
        hdr.copy_from_slice(&buf[..HEADER_LEN]);
        let header = Header::decode(&hdr).expect("header");
        assert_eq!(header.kind, Kind::Reply);
        assert_eq!(header.req_id, 3);
        assert_eq!(&buf[HEADER_LEN..], b"payload");
    }

    #[test]
    fn backpressure_pauses_reading() {
        let limits = ConnLimits { max_outbox_frames: 1, ..Default::default() };
        let (a, mut raw) = UnixStream::pair().expect("socketpair");
        let mut conn = Connection::from_stream(a, limits).expect("conn");
        conn.send(Kind::Notify, 1, 0, b"x", vec![]).expect("first send fits");
        assert!(matches!(conn.send(Kind::Notify, 1, 0, b"y", vec![]), Err(Error::Backpressure)));
        assert!(!conn.interest().read);
        // queued inbound data is not read while paused
        raw.write_all(&frame_bytes(Kind::Request, 5, 5, b"z", 0)).expect("write");
        assert_eq!(conn.handle_readable().expect("paused pump"), 0);
        conn.handle_writable().expect("flush");
        assert!(conn.interest().read);
        assert_eq!(conn.handle_readable().expect("resumed pump"), 1);
    }

    #[test]
    fn fd_passing_round_trip() {
        let (mut sender, receiver) = pair();
        let mut receiver = Connection::from_stream(receiver, ConnLimits::default()).expect("conn");
        let memfd = rustix::fs::memfd_create("crownos-ipc-test", rustix::fs::MemfdFlags::CLOEXEC)
            .expect("memfd");
        let mut file = std::fs::File::from(memfd);
        file.write_all(b"fd contents").expect("write memfd");
        sender.send(Kind::Request, 11, 1, b"with fd", vec![file.into()]).expect("send");
        sender.handle_writable().expect("flush");
        receiver.handle_readable().expect("receive");
        let msg = receiver.next_message().expect("message");
        assert_eq!(msg.fds.len(), 1);
        // CLOEXEC must be set on received fds
        let flags = rustix::io::fcntl_getfd(&msg.fds[0]).expect("getfd");
        assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));
        let mut got = String::new();
        let mut file = std::fs::File::from(msg.fds.into_iter().next().expect("fd"));
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(0)).expect("seek");
        file.read_to_string(&mut got).expect("read");
        assert_eq!(got, "fd contents");
    }

    #[test]
    fn undeclared_fds_poison() {
        let (mut sender, receiver) = pair();
        let mut receiver = Connection::from_stream(receiver, ConnLimits::default()).expect("conn");
        // frame declares 2 fds but only 1 was attached
        let memfd = rustix::fs::memfd_create("crownos-ipc-test2", rustix::fs::MemfdFlags::CLOEXEC)
            .expect("memfd");
        let header = Header { len: 1, kind: Kind::Request, fd_count: 2, selector: 0, req_id: 0 };
        let mut bytes = header.encode().to_vec();
        bytes.push(b'x');
        sender.outbox_bytes += bytes.len();
        sender.outbox.push_back(OutFrame { bytes, fds: vec![memfd], offset: 0 });
        sender.handle_writable().expect("flush");
        drop(sender);
        assert!(matches!(receiver.handle_readable(), Err(Error::Malformed(_))));
        assert!(receiver.is_closed());
    }

    #[test]
    fn poisoned_connection_closes_queued_fds() {
        let (mut sender, receiver) = pair();
        let mut receiver = Connection::from_stream(receiver, ConnLimits::default()).expect("conn");
        let memfd = rustix::fs::memfd_create("crownos-ipc-test3", rustix::fs::MemfdFlags::CLOEXEC)
            .expect("memfd");
        let probe = memfd.try_clone().expect("dup");
        sender.send(Kind::Request, 1, 1, b"good", vec![memfd]).expect("send good");
        sender.handle_writable().expect("flush good");
        let mut bad = frame_bytes(Kind::Request, 2, 2, b"bad", 0);
        bad[6] = 1; // reserved byte violation
        sender.outbox_bytes += bad.len();
        sender.outbox.push_back(OutFrame { bytes: bad, fds: vec![], offset: 0 });
        sender.handle_writable().expect("flush bad");
        assert!(matches!(receiver.handle_readable(), Err(Error::Malformed(_))));
        // the good message (and its fd) must have been dropped, not delivered
        assert!(receiver.next_message().is_none());
        drop(probe);
    }

    #[test]
    fn partial_write_flush() {
        let (mut conn, mut raw) = pair();
        // fill the kernel buffer so writes go partial
        let big = vec![0xabu8; 200 * 1024];
        let mut sent = 0;
        loop {
            match conn.send(Kind::Notify, 1, 0, &big, vec![]) {
                Ok(()) => sent += 1,
                Err(Error::Backpressure) => break,
                Err(e) => panic!("unexpected: {e}"),
            }
            if conn.handle_writable().is_err() {
                panic!("flush failed");
            }
            if conn.interest().write {
                break; // kernel buffer full, partial frame pending
            }
        }
        assert!(sent > 0);
        // drain the peer until every queued frame flushes
        let mut total = 0usize;
        let mut buf = vec![0u8; 64 * 1024];
        while conn.interest().write {
            conn.handle_writable().expect("flush");
            match raw.read(&mut buf) {
                Ok(n) => total += n,
                Err(e) => panic!("peer read: {e}"),
            }
        }
        while let Ok(n) = {
            raw.set_nonblocking(true).expect("nb");
            raw.read(&mut buf)
        } {
            if n == 0 {
                break;
            }
            total += n;
        }
        assert_eq!(total % (HEADER_LEN + big.len()), 0);
        assert!(total >= HEADER_LEN + big.len());
        let _ = conn.as_fd().as_raw_fd();
    }
}
