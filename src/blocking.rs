//! `poll(2)`-driven conveniences for CLIs, tests and simple daemons.

use std::os::fd::AsFd;
use std::time::Duration;

use rustix::event::{PollFd, PollFlags};

use crate::conn::Interest;
use crate::error::Error;
use crate::server::{PeerId, Server};

/// Wait once for the requested readiness (or `timeout`).
pub(crate) fn poll_once(
    fd: &impl AsFd,
    interest: Interest,
    timeout: Option<Duration>,
) -> Result<(), Error> {
    let mut flags = PollFlags::empty();
    if interest.read {
        flags |= PollFlags::IN;
    }
    if interest.write {
        flags |= PollFlags::OUT;
    }
    if flags.is_empty() {
        flags = PollFlags::IN;
    }
    let timespec = timeout.map(|t| rustix::event::Timespec {
        tv_sec: t.as_secs() as i64,
        tv_nsec: i64::from(t.subsec_nanos()),
    });
    let mut fds = [PollFd::new(fd, flags)];
    loop {
        match rustix::event::poll(&mut fds, timespec.as_ref()) {
            Ok(0) => return Err(Error::Timeout),
            Ok(_) => return Ok(()),
            Err(rustix::io::Errno::INTR) => continue,
            Err(e) => return Err(Error::Io(e.into())),
        }
    }
}

/// Single-threaded serve loop: polls the listener and every peer, pumping
/// each and handing application messages to `dispatch`. Runs until
/// `dispatch` returns `Ok(false)`.
///
/// `dispatch` gets each `Request`/`Notify` message; generated protocol
/// dispatch functions slot in directly.
pub fn serve<F>(server: &mut Server, mut dispatch: F) -> Result<(), Error>
where
    F: FnMut(&mut Server, PeerId, crate::Message) -> Result<bool, Error>,
{
    loop {
        let peers = server.peer_ids();
        let mut fds = Vec::with_capacity(peers.len() + 1);
        fds.push(PollFd::new(&*server, PollFlags::IN));
        let mut polled = Vec::with_capacity(peers.len());
        for &id in &peers {
            let Some(interest) = server.peer_interest(id) else {
                continue;
            };
            let mut flags = PollFlags::empty();
            if interest.read {
                flags |= PollFlags::IN;
            }
            if interest.write {
                flags |= PollFlags::OUT;
            }
            let Some(fd) = server.peer_fd(id) else {
                continue;
            };
            fds.push(PollFd::from_borrowed_fd(fd, flags));
            polled.push(id);
        }
        match rustix::event::poll(&mut fds, None) {
            Ok(_) => {}
            Err(rustix::io::Errno::INTR) => continue,
            Err(e) => return Err(Error::Io(e.into())),
        }
        let mut ready = Vec::with_capacity(polled.len() + 1);
        let listener_ready = !fds[0].revents().is_empty();
        for (i, &id) in polled.iter().enumerate() {
            let revents = fds[i + 1].revents();
            if !revents.is_empty() {
                ready.push((id, revents));
            }
        }
        drop(fds);
        if listener_ready {
            while server.accept()?.is_some() {}
        }
        for (id, revents) in ready {
            if revents.contains(PollFlags::OUT) && server.handle_writable(id).is_err() {
                continue;
            }
            if revents.intersects(PollFlags::IN | PollFlags::HUP | PollFlags::ERR)
                && server.handle_readable(id).is_err()
            {
                continue;
            }
            while let Some(msg) = server.next_request(id) {
                if !dispatch(server, id, msg)? {
                    return Ok(());
                }
            }
            server.reap(id);
        }
    }
}
