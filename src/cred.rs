use std::os::fd::{AsFd, OwnedFd};

/// Kernel-verified peer identity, captured once at accept time.
#[derive(Debug)]
pub struct Credentials {
    pub uid: u32,
    pub gid: u32,
    pub pid: i32,
    /// Best-effort pidfd for the peer. Advisory metadata only (exe, cgroup
    /// lookup, death watching) — authorization decisions use `uid`/`gid`,
    /// never the reusable `pid`.
    pub pidfd: Option<OwnedFd>,
}

impl Credentials {
    pub(crate) fn from_socket(fd: impl AsFd) -> std::io::Result<Self> {
        let ucred = rustix::net::sockopt::socket_peercred(&fd)?;
        let pidfd = rustix::process::pidfd_open(ucred.pid, rustix::process::PidfdFlags::empty()).ok();
        Ok(Self {
            uid: ucred.uid.as_raw(),
            gid: ucred.gid.as_raw(),
            pid: ucred.pid.as_raw_nonzero().get(),
            pidfd,
        })
    }
}

/// Access decision made at accept, before any bytes are read.
#[derive(Default)]
pub enum Authorizer {
    /// Allow only peers with our own effective uid (the default).
    #[default]
    SameUid,
    /// Allow an explicit set of uids, plus our own.
    Uids(Vec<u32>),
    /// Arbitrary predicate over the peer credentials.
    Custom(Box<dyn Fn(&Credentials) -> bool + Send + Sync>),
}

impl std::fmt::Debug for Authorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Authorizer::SameUid => f.write_str("SameUid"),
            Authorizer::Uids(uids) => f.debug_tuple("Uids").field(uids).finish(),
            Authorizer::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

impl Authorizer {
    pub(crate) fn allows(&self, cred: &Credentials) -> bool {
        let own = rustix::process::geteuid().as_raw();
        match self {
            Authorizer::SameUid => cred.uid == own,
            Authorizer::Uids(uids) => cred.uid == own || uids.contains(&cred.uid),
            Authorizer::Custom(pred) => pred(cred),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    #[test]
    fn peercred_reflects_self() {
        let (a, _b) = UnixStream::pair().expect("socketpair");
        let cred = Credentials::from_socket(&a).expect("peercred");
        assert_eq!(cred.uid, rustix::process::geteuid().as_raw());
        assert_eq!(cred.pid, std::process::id() as i32);
        assert!(cred.pidfd.is_some());
    }

    #[test]
    fn authorizer_decisions() {
        let (a, _b) = UnixStream::pair().expect("socketpair");
        let cred = Credentials::from_socket(&a).expect("peercred");
        assert!(Authorizer::SameUid.allows(&cred));
        assert!(Authorizer::Uids(vec![]).allows(&cred)); // own uid is implicit
        let deny_all = Authorizer::Custom(Box::new(|_| false));
        assert!(!deny_all.allows(&cred));
        let other = Credentials { uid: cred.uid + 1, gid: cred.gid, pid: cred.pid, pidfd: None };
        assert!(!Authorizer::SameUid.allows(&other));
        assert!(Authorizer::Uids(vec![cred.uid + 1]).allows(&other));
    }
}
