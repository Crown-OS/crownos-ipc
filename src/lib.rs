//! # crownos-ipc
//!
//! A brokerless, Unix-socket IPC bus built for CrownOS.
//!
//! ## Design
//!
//! - **Brokerless:** Unlike Dbus every service binds its own socket at
//!   `$XDG_RUNTIME_DIR/crownos/<service>.sock`; messages travel exactly one
//!   hop with no daemon in between. Service discovery is the socket path.
//!
//! - **Zero-handshake.** Peers are authenticated by the kernel
//!   (`SO_PEERCRED`) at `accept(2)`; the first frame a client writes is its
//!   first request. Same-uid peers are allowed by default ([`Authorizer`]).
//!
//! - **Rigid wire format.** A 20-byte binary header plus a
//!   [postcard](https://postcard.jamesmunns.com)-encoded payload. Every
//!   method has a compile-time selector hashed from its service, name and
//!   full signature — incompatible peers get a loud `UNKNOWN_METHOD`
//!   error, never silently misdecoded data. Malformed input disconnects
//!   the sender.
//! - **File descriptors** ride `SCM_RIGHTS` (up to 16 per message,
//!   `MSG_CMSG_CLOEXEC`, closed on every error path). Bulk data belongs in
//!   a memfd, not in frames (1 MiB cap).
//! - **Bounded everything.** Per-uid connection caps, subscription caps,
//!   bounded outboxes with read-side backpressure; a slow event subscriber
//!   loses events instead of wedging the service.
//! - **Runtime-agnostic.** The core is a non-blocking state machine pumped
//!   by *any* event loop. It comes with optional features that ships adapters
//!   for `calloop`, `tokio` and `smol`.
//!
//! ## Defining a protocol
//!
//! One [`protocol!`] definition generates both the typed client and the
//! server trait
//!
//! ## Server Side Setup
//! It can be a bar or anything that wants to expose some interfaces
//! ```ignore
//! use crownos_ipc::adapter::calloop::CalloopService;
//! use crownos_ipc::{ServiceBuilder, protocols::crownpad};
//!
//! let server = ServiceBuilder::new("crownpad").build()?;
//! let ipc = CalloopService::insert(&handle, server, |app, server, peer, msg| {
//!     crownpad::dispatch(&mut app.pad, server, peer, msg)
//! })?;
//! ipc.emit(&crownpad::Visibility { visible: true })?;
//! ```
//!
//! Without the feature the same thing is a few lines against [`Server`]
//! directly: register its listener fd, `accept` on readable, register each
//! `peer_fd`, then `handle_writable` / `handle_readable` / `next_request` /
//! `reap` per peer.
//!
//! ## Client Side Setup
//! e.g. a CLI or a keybinding helper that wants to interact with any servers
//!
//! ### Blocking
//! ```ignore
//! use crownos_ipc::protocols::crownpad;
//!
//! let mut pad = crownpad::Client::connect()?;
//! pad.toggle()?; // fire-and-forget
//! ```
//!
//! ### Tokio Runtime (feature `tokio`)
//!
//! ```ignore
//! use crownos_ipc::adapter::tokio::AsyncClient;
//! use crownos_ipc::protocols::crownotify;
//!
//! let mut c = AsyncClient::connect("crownotify")?;
//! let id = c.call::<crownotify::send_music_notification>(
//!     &crownotify::send_music_notification { song_name: "song".into(), percentage: 80 },
//!     vec![],
//! ).await?;
//! ```
//!
//! The `smol` feature provides the same surface under [`adapter::smol`].
//! A simple daemon can also just run [`blocking::serve`] on a thread and
//! bridge into its UI loop with a `calloop` channel.

#[doc(hidden)]
pub mod adapter;

#[doc(hidden)]
pub mod hash;

#[doc(hidden)]
pub mod blocking;

mod client;
mod conn;
mod cred;
mod error;
mod frame;
mod proto;
mod server;
mod socket;

// re-exported for macro-generated code and payload tooling
pub use postcard;

pub use client::{Client, Pending};
pub use conn::{ConnLimits, Connection, Interest, Message};
pub use cred::{Authorizer, Credentials};
pub use error::{Error, RemoteError};
pub use frame::Kind;
pub use proto::{EventMsg, MethodMsg};
pub use server::{Limits, PeerId, Server, ServiceBuilder};
