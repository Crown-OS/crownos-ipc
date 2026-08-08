//! # crownos-ipc — the CrownOS system bus
//!
//! A brokerless, Unix-socket IPC bus for CrownOS system apps: the
//! compositor, panels, the launchpad, the notification daemon and the
//! phone companion all talk through it instead of D-Bus.
//!
//! ## Design
//!
//! - **Brokerless.** Every service binds its own socket at
//!   `$XDG_RUNTIME_DIR/crownos/<service>.sock`; messages travel exactly one
//!   hop with no daemon in between. Service discovery is the socket path.
//! - **Zero-handshake.** Peers are authenticated by the kernel
//!   (`SO_PEERCRED`) at `accept(2)`; the first frame a client writes is its
//!   first request. Same-uid peers are allowed by default ([`Authorizer`]).
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
//!   by *any* event loop; optional `tokio` and `smol` features add async
//!   adapters, and [`blocking`] covers CLIs and simple daemons.
//!
//! ## Defining a protocol
//!
//! One [`protocol!`] definition generates both the typed client and the
//! server trait, so the two sides cannot drift apart. The CrownOS system
//! protocols ship in [`protocols`].
//!
//! ## Serving (any calloop app, e.g. a crownshell widget)
//!
//! ```ignore
//! use crownos_ipc::{ServiceBuilder, protocols::crownpad};
//!
//! let mut server = ServiceBuilder::new("crownpad").build()?;
//! // register server (the listener fd) with calloop as a Generic source;
//! // on readable: while let Some(peer) = server.accept()? { register
//! // server.peer_fd(peer) as another source }
//! // per-peer source callback:
//! //   server.handle_writable(peer) / server.handle_readable(peer);
//! //   while let Some(msg) = server.next_request(peer) {
//! //       crownpad::dispatch(&mut handler, &mut server, peer, msg)?;
//! //   }
//! //   if server.reap(peer) { /* deregister source */ }
//! ```
//!
//! ## Calling (blocking, e.g. a CLI or a keybinding helper)
//!
//! ```ignore
//! use crownos_ipc::protocols::crownpad;
//!
//! let mut pad = crownpad::Client::connect()?;
//! pad.toggle()?; // fire-and-forget
//! ```
//!
//! ## Calling (tokio, feature `tokio`)
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

pub mod adapter;
pub mod blocking;
pub mod client;
pub mod conn;
pub mod cred;
pub mod error;
pub mod frame;
pub mod hash;
pub mod proto;
pub mod protocols;
pub mod server;
pub mod socket;

// re-exported for macro-generated code and payload tooling
pub use postcard;

pub use client::{Client, Pending};
pub use conn::{ConnLimits, Connection, Interest, Message};
pub use cred::{Authorizer, Credentials};
pub use error::{Error, RemoteError};
pub use frame::Kind;
pub use proto::{EventMsg, MethodMsg};
pub use server::{Limits, PeerId, Server, ServiceBuilder};
