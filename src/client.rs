use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::conn::{ConnLimits, Connection, Interest, Message};
use crate::error::{Error, RemoteError};
use crate::frame::Kind;
use crate::proto::{EventMsg, MethodMsg};
use crate::socket;

const MAX_QUEUED_EVENTS: usize = 1024;

/// Correlation handle for an in-flight request. Resolve it with
/// [`Client::take`] (non-blocking) or [`Client::wait`] (blocking).
#[must_use = "a pending request resolves via Client::take or Client::wait"]
#[derive(Debug)]
pub struct Pending<R> {
    req_id: u32,
    _reply: PhantomData<R>,
}

enum Slot {
    Waiting,
    Ready(Result<(Vec<u8>, Vec<OwnedFd>), RemoteError>),
}

/// Connection to one service. Non-blocking core: pump `handle_readable` /
/// `handle_writable` from any event loop, or use the `*_blocking` helpers.
pub struct Client {
    conn: Connection,
    next_req: u32,
    pending: HashMap<u32, Slot>,
    events: VecDeque<Message>,
}

impl AsFd for Client {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.conn.as_fd()
    }
}

impl Client {
    /// Connect to `service` under `$XDG_RUNTIME_DIR/crownos`. 0-RTT: no
    /// handshake — the first frame written is the first request.
    pub fn connect(service: &str) -> Result<Self, Error> {
        let dir = socket::socket_dir()?;
        Self::connect_in(&dir, service)
    }

    /// Connect via an explicit socket directory.
    pub fn connect_in(dir: &Path, service: &str) -> Result<Self, Error> {
        let stream = socket::connect(dir, service)?;
        Self::from_stream(stream)
    }

    pub fn from_stream(stream: UnixStream) -> Result<Self, Error> {
        Ok(Self {
            conn: Connection::from_stream(stream, ConnLimits::default())?,
            next_req: 1,
            pending: HashMap::new(),
            events: VecDeque::new(),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    pub fn interest(&self) -> Interest {
        self.conn.interest()
    }

    pub fn handle_writable(&mut self) -> Result<(), Error> {
        self.conn.handle_writable()
    }

    /// Pump inbound frames: replies resolve their [`Pending`] slot, events
    /// queue for [`Client::next_event`].
    pub fn handle_readable(&mut self) -> Result<(), Error> {
        self.conn.handle_readable()?;
        while let Some(msg) = self.conn.next_message() {
            match msg.kind {
                Kind::Reply => {
                    if let Some(slot) = self.pending.get_mut(&msg.req_id) {
                        *slot = Slot::Ready(Ok((msg.payload, msg.fds)));
                    }
                }
                Kind::Error => {
                    let err: RemoteError = postcard::from_bytes(&msg.payload)?;
                    if let Some(slot) = self.pending.get_mut(&msg.req_id) {
                        *slot = Slot::Ready(Err(err));
                    }
                }
                Kind::Event => {
                    if self.events.len() >= MAX_QUEUED_EVENTS {
                        log::warn!("event queue full; dropping oldest event");
                        self.events.pop_front();
                    }
                    self.events.push_back(msg);
                }
                Kind::Request | Kind::Notify | Kind::Subscribe | Kind::Unsubscribe => {
                    return Err(Error::Malformed("server sent client-only frame"));
                }
            }
        }
        Ok(())
    }

    fn alloc_req(&mut self) -> u32 {
        let id = self.next_req;
        self.next_req = self.next_req.wrapping_add(1).max(1);
        id
    }

    /// Enqueue a request; flush with `handle_writable` (done opportunistically
    /// here) and resolve the returned handle later.
    pub fn start_request<M: MethodMsg>(
        &mut self,
        args: &M::Args,
        fds: Vec<OwnedFd>,
    ) -> Result<Pending<M::Reply>, Error> {
        debug_assert_eq!(fds.len(), M::FD_COUNT);
        let payload = postcard::to_stdvec(args)?;
        let req_id = self.alloc_req();
        self.conn.send(Kind::Request, M::SELECTOR, req_id, &payload, fds)?;
        self.pending.insert(req_id, Slot::Waiting);
        self.conn.handle_writable()?;
        Ok(Pending { req_id, _reply: PhantomData })
    }

    /// Fire-and-forget send for `notify` methods.
    pub fn notify<M: MethodMsg>(&mut self, args: &M::Args, fds: Vec<OwnedFd>) -> Result<(), Error> {
        debug_assert_eq!(fds.len(), M::FD_COUNT);
        let payload = postcard::to_stdvec(args)?;
        self.conn.send(Kind::Notify, M::SELECTOR, 0, &payload, fds)?;
        self.conn.handle_writable()
    }

    /// Non-blocking harvest: `Ok(Some(reply))` once the answer arrived.
    pub fn take<R: DeserializeOwned>(&mut self, pending: &Pending<R>) -> Result<Option<R>, Error> {
        match self.pending.get(&pending.req_id) {
            Some(Slot::Waiting) => {
                if self.conn.is_closed() {
                    self.pending.remove(&pending.req_id);
                    return Err(Error::Disconnected);
                }
                Ok(None)
            }
            Some(Slot::Ready(_)) => {
                let Some(Slot::Ready(result)) = self.pending.remove(&pending.req_id) else {
                    return Err(Error::Disconnected);
                };
                match result {
                    Ok((payload, _fds)) => Ok(Some(postcard::from_bytes(&payload)?)),
                    Err(remote) => Err(Error::Remote(remote)),
                }
            }
            None => Err(Error::Disconnected),
        }
    }

    /// Block (via `poll(2)`) until the reply arrives or `timeout` elapses.
    pub fn wait<R: DeserializeOwned>(
        &mut self,
        pending: Pending<R>,
        timeout: Option<Duration>,
    ) -> Result<R, Error> {
        let deadline = timeout.map(|t| std::time::Instant::now() + t);
        loop {
            if let Some(reply) = self.take(&pending)? {
                return Ok(reply);
            }
            let remaining = match deadline {
                Some(d) => {
                    let now = std::time::Instant::now();
                    if now >= d {
                        return Err(Error::Timeout);
                    }
                    Some(d - now)
                }
                None => None,
            };
            crate::blocking::poll_once(&self.conn, self.conn.interest(), remaining)?;
            self.handle_writable()?;
            self.handle_readable()?;
        }
    }

    /// One-call convenience: send a request and block for its reply.
    pub fn call_blocking<M: MethodMsg>(
        &mut self,
        args: &M::Args,
        fds: Vec<OwnedFd>,
        timeout: Option<Duration>,
    ) -> Result<M::Reply, Error> {
        let pending = self.start_request::<M>(args, fds)?;
        self.wait(pending, timeout)
    }

    /// Ask the server to start pushing `E` events to this connection.
    pub fn subscribe<E: EventMsg>(&mut self) -> Result<Pending<()>, Error> {
        let req_id = self.alloc_req();
        self.conn.send(Kind::Subscribe, E::SELECTOR, req_id, &[], vec![])?;
        self.pending.insert(req_id, Slot::Waiting);
        self.conn.handle_writable()?;
        Ok(Pending { req_id, _reply: PhantomData })
    }

    pub fn unsubscribe<E: EventMsg>(&mut self) -> Result<Pending<()>, Error> {
        let req_id = self.alloc_req();
        self.conn.send(Kind::Unsubscribe, E::SELECTOR, req_id, &[], vec![])?;
        self.pending.insert(req_id, Slot::Waiting);
        self.conn.handle_writable()?;
        Ok(Pending { req_id, _reply: PhantomData })
    }

    /// Blocking subscribe that waits for the server's acknowledgement.
    pub fn subscribe_blocking<E: EventMsg>(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        let pending = self.subscribe::<E>()?;
        self.wait(pending, timeout)
    }

    /// Pop the next queued event of type `E`; undecodable or foreign events
    /// stay queued for other extractors.
    pub fn next_event<E: EventMsg>(&mut self) -> Option<E> {
        let pos = self.events.iter().position(|m| m.selector == E::SELECTOR)?;
        let msg = self.events.remove(pos)?;
        match postcard::from_bytes(&msg.payload) {
            Ok(event) => Some(event),
            Err(e) => {
                log::warn!("dropping undecodable event: {e}");
                None
            }
        }
    }

    /// Pop the next queued event regardless of type.
    pub fn next_event_raw(&mut self) -> Option<Message> {
        self.events.pop_front()
    }
}
