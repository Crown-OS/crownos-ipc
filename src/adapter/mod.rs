//! Optional async-runtime adapters over the sync non-blocking core.
//!
//! Each adapter watches readiness on a `dup(2)` of the connection's fd and
//! pumps the runtime-agnostic state machine; no protocol logic lives here.

#[cfg(feature = "calloop")]
pub mod calloop;
#[cfg(feature = "smol")]
pub mod smol;
#[cfg(feature = "tokio")]
pub mod tokio;
