use std::collections::{HashMap, HashSet};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;

use crate::conn::{ConnLimits, Connection, Interest, Message};
use crate::cred::{Authorizer, Credentials};
use crate::error::{Error, RemoteError};
use crate::frame::Kind;
use crate::proto::EventMsg;
use crate::socket;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub conn: ConnLimits,
    pub max_conns_per_uid: usize,
    pub max_subscriptions: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self { conn: ConnLimits::default(), max_conns_per_uid: 32, max_subscriptions: 64 }
    }
}

/// Generation-tagged handle to a connected peer; stale ids are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId {
    index: u32,
    generation: u32,
}

struct Peer {
    conn: Connection,
    cred: Credentials,
    subs: HashSet<u64>,
}

struct Slot {
    generation: u32,
    peer: Option<Peer>,
}

pub struct ServiceBuilder {
    service: String,
    directory: Option<PathBuf>,
    authorizer: Authorizer,
    limits: Limits,
}

impl ServiceBuilder {
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            directory: None,
            authorizer: Authorizer::default(),
            limits: Limits::default(),
        }
    }

    /// Override the socket directory (defaults to `$XDG_RUNTIME_DIR/crownos`).
    pub fn directory(mut self, dir: impl Into<PathBuf>) -> Self {
        self.directory = Some(dir.into());
        self
    }

    pub fn authorizer(mut self, authorizer: Authorizer) -> Self {
        self.authorizer = authorizer;
        self
    }

    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    pub fn build(self) -> Result<Server, Error> {
        let dir = match &self.directory {
            Some(d) => {
                socket::ensure_dir(d)?;
                d.clone()
            }
            None => socket::socket_dir()?,
        };
        let listener = socket::bind_service(&dir, &self.service)?;
        Ok(Server::from_parts(listener, self))
    }

    /// Build on a listener the caller already owns — e.g. one received via
    /// systemd socket activation (`LISTEN_FDS`). The listener is switched to
    /// non-blocking mode.
    pub fn build_with_listener(self, listener: UnixListener) -> Result<Server, Error> {
        listener.set_nonblocking(true)?;
        Ok(Server::from_parts(listener, self))
    }
}

/// A bound service: accepts peers, authenticates them via `SO_PEERCRED`,
/// enforces per-uid and subscription limits, and routes frames. Deliberately
/// single-owner (`&mut self` everywhere) so any event loop can drive it.
pub struct Server {
    listener: UnixListener,
    service: String,
    authorizer: Authorizer,
    limits: Limits,
    slots: Vec<Slot>,
    uid_counts: HashMap<u32, usize>,
}

impl AsFd for Server {
    /// The *listener* fd — register it for readability to drive [`Server::accept`].
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.listener.as_fd()
    }
}

impl Server {
    fn from_parts(listener: UnixListener, builder: ServiceBuilder) -> Self {
        Self {
            listener,
            service: builder.service,
            authorizer: builder.authorizer,
            limits: builder.limits,
            slots: Vec::new(),
            uid_counts: HashMap::new(),
        }
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    /// Accept one authorized peer. `Ok(None)` when no connection is pending.
    /// Unauthorized or over-limit peers are closed silently and the next
    /// pending connection is tried.
    pub fn accept(&mut self) -> Result<Option<PeerId>, Error> {
        loop {
            let stream = match self.listener.accept() {
                Ok((stream, _)) => stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::Io(e)),
            };
            let cred = match Credentials::from_socket(&stream) {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("{}: failed to read peer credentials: {e}", self.service);
                    continue;
                }
            };
            if !self.authorizer.allows(&cred) {
                log::warn!("{}: rejected peer uid {} pid {}", self.service, cred.uid, cred.pid);
                continue;
            }
            let count = self.uid_counts.entry(cred.uid).or_insert(0);
            if *count >= self.limits.max_conns_per_uid {
                log::warn!("{}: uid {} exceeded connection cap", self.service, cred.uid);
                continue;
            }
            *count += 1;
            let conn = Connection::from_stream(stream, self.limits.conn)?;
            let peer = Peer { conn, cred, subs: HashSet::new() };
            let index = match self.slots.iter().position(|s| s.peer.is_none()) {
                Some(i) => {
                    self.slots[i].peer = Some(peer);
                    i
                }
                None => {
                    self.slots.push(Slot { generation: 0, peer: Some(peer) });
                    self.slots.len() - 1
                }
            };
            return Ok(Some(PeerId { index: index as u32, generation: self.slots[index].generation }));
        }
    }

    fn peer(&self, id: PeerId) -> Option<&Peer> {
        self.slots
            .get(id.index as usize)
            .filter(|s| s.generation == id.generation)
            .and_then(|s| s.peer.as_ref())
    }

    fn peer_mut(&mut self, id: PeerId) -> Option<&mut Peer> {
        self.slots
            .get_mut(id.index as usize)
            .filter(|s| s.generation == id.generation)
            .and_then(|s| s.peer.as_mut())
    }

    pub fn peer_ids(&self) -> Vec<PeerId> {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.peer.is_some())
            .map(|(i, s)| PeerId { index: i as u32, generation: s.generation })
            .collect()
    }

    pub fn peer_fd(&self, id: PeerId) -> Option<BorrowedFd<'_>> {
        self.peer(id).map(|p| p.conn.as_fd())
    }

    pub fn peer_interest(&self, id: PeerId) -> Option<Interest> {
        self.peer(id).map(|p| p.conn.interest())
    }

    pub fn credentials(&self, id: PeerId) -> Option<&Credentials> {
        self.peer(id).map(|p| &p.cred)
    }

    /// Pump inbound data for one peer. On any error the peer is removed and
    /// the error returned — the caller only needs to deregister its source.
    pub fn handle_readable(&mut self, id: PeerId) -> Result<(), Error> {
        let Some(peer) = self.peer_mut(id) else { return Err(Error::Disconnected) };
        match peer.conn.handle_readable() {
            Ok(_) => Ok(()),
            Err(e) => {
                self.drop_peer(id);
                Err(e)
            }
        }
    }

    pub fn handle_writable(&mut self, id: PeerId) -> Result<(), Error> {
        let Some(peer) = self.peer_mut(id) else { return Err(Error::Disconnected) };
        match peer.conn.handle_writable() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.drop_peer(id);
                Err(e)
            }
        }
    }

    /// Next application request (`Request` or `Notify`) from a peer.
    /// Subscription bookkeeping frames are consumed internally; any frame a
    /// client must not send (`Reply`, `Event`, ...) poisons the connection.
    pub fn next_request(&mut self, id: PeerId) -> Option<Message> {
        loop {
            let max_subs = self.limits.max_subscriptions;
            let peer = self.peer_mut(id)?;
            let msg = peer.conn.next_message()?;
            match msg.kind {
                Kind::Request | Kind::Notify => return Some(msg),
                Kind::Subscribe => {
                    if peer.subs.len() >= max_subs {
                        let err = RemoteError::new(RemoteError::LIMIT, "subscription cap reached");
                        self.reply_err(id, msg.req_id, &err).ok()?;
                    } else {
                        peer.subs.insert(msg.selector);
                        self.reply(id, msg.req_id, &[], vec![]).ok()?;
                    }
                }
                Kind::Unsubscribe => {
                    peer.subs.remove(&msg.selector);
                    self.reply(id, msg.req_id, &[], vec![]).ok()?;
                }
                Kind::Reply | Kind::Error | Kind::Event => {
                    log::warn!("{}: peer sent server-only frame; dropping it", self.service);
                    self.drop_peer(id);
                    return None;
                }
            }
        }
    }

    /// True if the peer reached EOF and everything it sent has been drained;
    /// the peer is removed. Call after exhausting [`Server::next_request`].
    pub fn reap(&mut self, id: PeerId) -> bool {
        let Some(peer) = self.peer(id) else { return true };
        if peer.conn.is_closed() {
            self.drop_peer(id);
            true
        } else {
            false
        }
    }

    pub fn reply(
        &mut self,
        id: PeerId,
        req_id: u32,
        payload: &[u8],
        fds: Vec<OwnedFd>,
    ) -> Result<(), Error> {
        self.send_to(id, Kind::Reply, 0, req_id, payload, fds)
    }

    pub fn reply_err(&mut self, id: PeerId, req_id: u32, err: &RemoteError) -> Result<(), Error> {
        let payload = postcard::to_stdvec(err)?;
        self.send_to(id, Kind::Error, 0, req_id, &payload, vec![])
    }

    fn send_to(
        &mut self,
        id: PeerId,
        kind: Kind,
        selector: u64,
        req_id: u32,
        payload: &[u8],
        fds: Vec<OwnedFd>,
    ) -> Result<(), Error> {
        let Some(peer) = self.peer_mut(id) else { return Err(Error::Disconnected) };
        peer.conn.send(kind, selector, req_id, payload, fds)?;
        // opportunistic flush keeps latency low without extra interest churn
        match peer.conn.handle_writable() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.drop_peer(id);
                Err(e)
            }
        }
    }

    /// Broadcast an event to every subscribed peer. Slow peers with a full
    /// outbox lose the event (events are lossy under pushback by design).
    pub fn emit<E: EventMsg>(&mut self, event: &E) -> Result<(), Error> {
        let payload = postcard::to_stdvec(event)?;
        for id in self.peer_ids() {
            let subscribed = self.peer(id).is_some_and(|p| p.subs.contains(&E::SELECTOR));
            if !subscribed {
                continue;
            }
            match self.send_to(id, Kind::Event, E::SELECTOR, 0, &payload, vec![]) {
                Ok(()) => {}
                Err(Error::Backpressure) => {
                    log::warn!("{}: dropping event for slow peer", self.service);
                }
                Err(_) => {} // peer already removed by send_to
            }
        }
        Ok(())
    }

    pub fn drop_peer(&mut self, id: PeerId) {
        let Some(slot) = self.slots.get_mut(id.index as usize) else { return };
        if slot.generation != id.generation {
            return;
        }
        if let Some(peer) = slot.peer.take() {
            slot.generation = slot.generation.wrapping_add(1);
            if let Some(count) = self.uid_counts.get_mut(&peer.cred.uid) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.uid_counts.remove(&peer.cred.uid);
                }
            }
        }
    }
}
