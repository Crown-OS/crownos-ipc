//! calloop adapter (feature `calloop`).
//!
//! Lets a calloop-driven app — a crownshell widget, a panel, the compositor —
//! serve a protocol from the event loop it already has, instead of running a
//! second runtime beside it.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::fd::{AsFd, OwnedFd};
use std::rc::Rc;

use calloop::generic::{Generic, NoIoDrop};
use calloop::{
    EventSource, Interest as PollInterest, LoopHandle, Mode, Poll, PostAction, Readiness,
    RegistrationToken, Token, TokenFactory,
};

use crate::conn::{Interest, Message};
use crate::error::Error;
use crate::proto::EventMsg;
use crate::server::{PeerId, Server};

fn poll_interest(interest: Interest) -> PollInterest {
    PollInterest {
        readable: interest.read,
        writable: interest.write,
    }
}

fn insert_failed(e: calloop::InsertError<impl Sized>) -> Error {
    Error::Io(e.error.into())
}

/// A peer socket whose poll interest lives in a shared cell.
///
/// [`Generic::interest`] can only be reached through the owning `Dispatcher`,
/// which is mutably borrowed for the duration of its own callback — so a peer
/// could never narrow or widen its own registration from the very place that
/// learns the outbox drained. Reading the interest out of a `Cell` at
/// (re)registration time sidesteps that: the callback sets the cell and asks
/// calloop to update, which defers the actual reregistration until after the
/// callback returns.
struct PeerSource {
    generic: Generic<OwnedFd, Error>,
    interest: Rc<Cell<Interest>>,
}

impl EventSource for PeerSource {
    type Event = Readiness;
    type Metadata = NoIoDrop<OwnedFd>;
    type Ret = Result<PostAction, Error>;
    type Error = Error;

    fn process_events<C>(
        &mut self,
        readiness: Readiness,
        token: Token,
        callback: C,
    ) -> Result<PostAction, Error>
    where
        C: FnMut(Readiness, &mut NoIoDrop<OwnedFd>) -> Result<PostAction, Error>,
    {
        self.generic.process_events(readiness, token, callback)
    }

    fn register(&mut self, poll: &mut Poll, tokens: &mut TokenFactory) -> calloop::Result<()> {
        self.generic.interest = poll_interest(self.interest.get());
        self.generic.register(poll, tokens)
    }

    fn reregister(&mut self, poll: &mut Poll, tokens: &mut TokenFactory) -> calloop::Result<()> {
        self.generic.interest = poll_interest(self.interest.get());
        self.generic.reregister(poll, tokens)
    }

    fn unregister(&mut self, poll: &mut Poll) -> calloop::Result<()> {
        self.generic.unregister(poll)
    }
}

/// The two loop operations the peer bookkeeping needs, type-erased so
/// [`CalloopService`] does not have to carry the loop's data parameter.
struct LoopOps {
    update: Box<dyn Fn(&RegistrationToken)>,
    remove: Box<dyn Fn(RegistrationToken)>,
}

struct PeerReg {
    token: RegistrationToken,
    interest: Rc<Cell<Interest>>,
}

/// State shared between the owning app and every callback in the loop.
struct Registry {
    server: Rc<RefCell<Server>>,
    peers: RefCell<HashMap<PeerId, PeerReg>>,
    ops: LoopOps,
}

impl Registry {
    /// Bring every peer's poll registration back in line with
    /// [`Server::peer_interest`], and drop the sources of peers the server no
    /// longer knows about (reaped, dropped for a protocol violation, ...).
    ///
    /// `current` is the peer whose callback is running, if any: calloop cannot
    /// unregister a source from inside that source's own callback, so its
    /// removal is handed back as a [`PostAction`] for the callback to return.
    fn sync(&self, current: Option<PeerId>) -> PostAction {
        let server = self.server.borrow();
        let mut post = PostAction::Continue;
        self.peers
            .borrow_mut()
            .retain(|&id, reg| match server.peer_interest(id) {
                Some(interest) => {
                    if reg.interest.replace(interest) != interest {
                        (self.ops.update)(&reg.token);
                    }
                    true
                }
                None => {
                    if current == Some(id) {
                        post = PostAction::Remove;
                    } else {
                        (self.ops.remove)(reg.token);
                    }
                    false
                }
            });
        post
    }
}

/// A [`Server`] driven by a calloop event loop.
///
/// The listener and every accepted peer become `Generic` sources; the service
/// keeps their registration tokens and deregisters all of them when dropped,
/// so tearing the service down needs no extra call (and breaks the reference
/// cycle the callbacks' cloned [`LoopHandle`] would otherwise form with the
/// loop).
pub struct CalloopService {
    registry: Rc<Registry>,
    listener: RegistrationToken,
}

impl CalloopService {
    /// Registers `server`'s listener with the loop and, as peers connect,
    /// their sockets too. `dispatch` is called for each decoded request.
    ///
    /// A generated `protocol!` dispatch function slots straight in:
    ///
    /// ```ignore
    /// CalloopService::insert(&handle, server, |app, server, peer, msg| {
    ///     crownpad::dispatch(&mut app.pad, server, peer, msg)
    /// })?;
    /// ```
    ///
    /// Errors from a single peer are logged and cost that peer its
    /// connection; only an error `dispatch` itself returns — fatal to the
    /// whole service, per [`crate::protocol!`] — leaves the loop.
    ///
    /// # Re-entrancy
    ///
    /// `dispatch` is handed the `&mut Server` it needs, so it must use that
    /// rather than calling [`CalloopService::emit`] or
    /// [`CalloopService::with_server`] on the service it is being driven by:
    /// those take the shared server borrow that `dispatch` already holds.
    /// Interest and reaping are re-synced after `dispatch` returns, so
    /// emitting through the borrowed server behaves identically.
    pub fn insert<D, F>(
        handle: &LoopHandle<'static, D>,
        server: Server,
        dispatch: F,
    ) -> Result<Self, Error>
    where
        D: 'static,
        F: FnMut(&mut D, &mut Server, PeerId, Message) -> Result<(), Error> + 'static,
    {
        let listener_fd = server.as_fd().try_clone_to_owned()?;
        let registry = Rc::new(Registry {
            server: Rc::new(RefCell::new(server)),
            peers: RefCell::new(HashMap::new()),
            ops: LoopOps {
                update: {
                    let handle = handle.clone();
                    Box::new(move |token| {
                        if let Err(e) = handle.update(token) {
                            log::warn!("crownos-ipc: peer reregistration failed: {e}");
                        }
                    })
                },
                remove: {
                    let handle = handle.clone();
                    Box::new(move |token| handle.remove(token))
                },
            },
        });

        let source: Generic<OwnedFd, Error> =
            Generic::new_with_error(listener_fd, PollInterest::READ, Mode::Level);
        let dispatch = Rc::new(RefCell::new(dispatch));
        let loop_handle = handle.clone();
        let accept_registry = Rc::clone(&registry);
        let listener = handle
            .insert_source(source, move |_readiness, _fd, _data: &mut D| {
                loop {
                    // A failed accept is almost always about one connection: a
                    // peer that reset between the SYN queue and here, or a
                    // process-wide fd limit. Propagating it would carry the
                    // error out of `EventLoop::run` and take the whole host
                    // application down with the service, so it is logged and
                    // the listener left to try again on the next wakeup.
                    let accepted = match accept_registry.server.borrow_mut().accept() {
                        Ok(accepted) => accepted,
                        Err(e) => {
                            log::warn!("crownos-ipc: accept failed: {e}");
                            break;
                        }
                    };
                    let Some(peer) = accepted else { break };
                    if let Err(e) = register_peer(&loop_handle, &accept_registry, &dispatch, peer) {
                        log::warn!("crownos-ipc: cannot watch new peer: {e}");
                        accept_registry.server.borrow_mut().drop_peer(peer);
                    }
                }
                accept_registry.sync(None);
                Ok(PostAction::Continue)
            })
            .map_err(insert_failed)?;

        Ok(Self { registry, listener })
    }

    /// Broadcast an event to every subscribed peer.
    pub fn emit<E: EventMsg>(&self, event: &E) -> Result<(), Error> {
        let result = self.registry.server.borrow_mut().emit(event);
        self.registry.sync(None);
        result
    }

    /// Run `f` against the server, re-syncing peer registrations afterwards.
    pub fn with_server<R>(&self, f: impl FnOnce(&mut Server) -> R) -> R {
        let result = f(&mut self.registry.server.borrow_mut());
        self.registry.sync(None);
        result
    }
}

impl Drop for CalloopService {
    fn drop(&mut self) {
        for (_, reg) in self.registry.peers.borrow_mut().drain() {
            (self.registry.ops.remove)(reg.token);
        }
        (self.registry.ops.remove)(self.listener);
    }
}

fn register_peer<D, F>(
    handle: &LoopHandle<'static, D>,
    registry: &Rc<Registry>,
    dispatch: &Rc<RefCell<F>>,
    peer: PeerId,
) -> Result<(), Error>
where
    D: 'static,
    F: FnMut(&mut D, &mut Server, PeerId, Message) -> Result<(), Error> + 'static,
{
    let (fd, interest) = {
        let server = registry.server.borrow();
        let Some(fd) = server.peer_fd(peer) else {
            return Ok(());
        };
        let interest = server.peer_interest(peer).unwrap_or(Interest {
            read: true,
            write: false,
        });
        (fd.try_clone_to_owned()?, interest)
    };
    let cell = Rc::new(Cell::new(interest));
    let source = PeerSource {
        generic: Generic::new_with_error(fd, poll_interest(interest), Mode::Level),
        interest: Rc::clone(&cell),
    };

    let cb_registry = Rc::clone(registry);
    let cb_dispatch = Rc::clone(dispatch);
    let token = handle
        .insert_source(source, move |readiness, _fd, data: &mut D| {
            pump_peer(&cb_registry, &cb_dispatch, peer, readiness, data)
        })
        .map_err(insert_failed)?;

    registry.peers.borrow_mut().insert(
        peer,
        PeerReg {
            token,
            interest: cell,
        },
    );
    Ok(())
}

/// One readiness pass over a single peer: flush, fill, drain, reap, re-sync.
///
/// Invariant: no borrow of the shared `Server` outlives one statement here.
/// `dispatch` is user code that is handed its own `&mut Server`, and it may
/// well emit or drop peers through it; keeping a borrow alive around it — or
/// around `sync`, which borrows the server again — would abort the process.
fn pump_peer<D, F>(
    registry: &Registry,
    dispatch: &RefCell<F>,
    peer: PeerId,
    readiness: Readiness,
    data: &mut D,
) -> Result<PostAction, Error>
where
    F: FnMut(&mut D, &mut Server, PeerId, Message) -> Result<(), Error>,
{
    let mut live = true;
    if readiness.writable
        && let Err(e) = registry.server.borrow_mut().handle_writable(peer)
    {
        log::debug!("crownos-ipc: dropping peer after failed write: {e}");
        live = false;
    }
    if live
        && (readiness.readable || readiness.error)
        && let Err(e) = registry.server.borrow_mut().handle_readable(peer)
    {
        log::debug!("crownos-ipc: dropping peer after failed read: {e}");
        live = false;
    }

    if live {
        loop {
            let Some(msg) = registry.server.borrow_mut().next_request(peer) else {
                break;
            };
            let mut handler = dispatch.borrow_mut();
            let mut server = registry.server.borrow_mut();
            (*handler)(data, &mut server, peer, msg)?;
        }
    }

    // `reap` retires a peer that hit EOF; `sync` then notices it is gone and
    // pulls its source out of the loop.
    registry.server.borrow_mut().reap(peer);
    Ok(registry.sync(Some(peer)))
}
