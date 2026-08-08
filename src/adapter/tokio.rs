//! tokio adapter (feature `tokio`).

use std::collections::HashMap;
use std::future::poll_fn;
use std::os::fd::{AsFd, OwnedFd};
use std::path::Path;
use std::task::Poll;

use serde::de::DeserializeOwned;
use tokio::io::unix::AsyncFd;
use tokio::io::{Interest as TokioInterest, Ready};

use crate::client::{Client, Pending};
use crate::error::Error;
use crate::proto::{EventMsg, MethodMsg};
use crate::server::{PeerId, Server};

fn watcher_for(fd: impl AsFd) -> Result<AsyncFd<OwnedFd>, Error> {
    let dup = fd.as_fd().try_clone_to_owned()?;
    AsyncFd::with_interest(dup, TokioInterest::READABLE | TokioInterest::WRITABLE)
        .map_err(Error::Io)
}

/// Async wrapper over [`Client`]. Typed protocol methods are used through
/// the generic [`AsyncClient::call`]/[`AsyncClient::notify`] with the
/// macro-generated method structs.
pub struct AsyncClient {
    client: Client,
    watcher: AsyncFd<OwnedFd>,
}

impl AsyncClient {
    pub fn new(client: Client) -> Result<Self, Error> {
        let watcher = watcher_for(&client)?;
        Ok(Self { client, watcher })
    }

    pub fn connect(service: &str) -> Result<Self, Error> {
        Self::new(Client::connect(service)?)
    }

    pub fn connect_in(dir: &Path, service: &str) -> Result<Self, Error> {
        Self::new(Client::connect_in(dir, service)?)
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    async fn flush(&mut self) -> Result<(), Error> {
        while self.client.interest().write {
            let mut guard = self.watcher.writable().await.map_err(Error::Io)?;
            guard.clear_ready_matching(Ready::WRITABLE);
            self.client.handle_writable()?;
        }
        Ok(())
    }

    async fn pump(&mut self) -> Result<(), Error> {
        self.flush().await?;
        let mut guard = self.watcher.readable().await.map_err(Error::Io)?;
        guard.clear_ready_matching(Ready::READABLE);
        self.client.handle_readable()
    }

    /// Resolve an already-started request.
    pub async fn resolve<R: DeserializeOwned>(&mut self, pending: Pending<R>) -> Result<R, Error> {
        loop {
            if let Some(reply) = self.client.take(&pending)? {
                return Ok(reply);
            }
            self.pump().await?;
        }
    }

    pub async fn call<M: MethodMsg>(
        &mut self,
        args: &M::Args,
        fds: Vec<OwnedFd>,
    ) -> Result<M::Reply, Error> {
        let pending = self.client.start_request::<M>(args, fds)?;
        self.resolve(pending).await
    }

    pub async fn notify<M: MethodMsg>(
        &mut self,
        args: &M::Args,
        fds: Vec<OwnedFd>,
    ) -> Result<(), Error> {
        self.client.notify::<M>(args, fds)?;
        self.flush().await
    }

    pub async fn subscribe<E: EventMsg>(&mut self) -> Result<(), Error> {
        let pending = self.client.subscribe::<E>()?;
        self.resolve(pending).await
    }

    pub async fn next_event<E: EventMsg>(&mut self) -> Result<E, Error> {
        loop {
            if let Some(event) = self.client.next_event::<E>() {
                return Ok(event);
            }
            if self.client.is_closed() {
                return Err(Error::Disconnected);
            }
            self.pump().await?;
        }
    }
}

/// Drive a [`Server`] on the tokio runtime. `dispatch` receives each
/// application message (generated `dispatch` functions slot in directly)
/// and ends the loop by returning `Ok(false)`.
pub async fn serve<F>(server: &mut Server, mut dispatch: F) -> Result<(), Error>
where
    F: FnMut(&mut Server, PeerId, crate::Message) -> Result<bool, Error>,
{
    let listener = watcher_for(&*server)?;
    let mut watchers: HashMap<PeerId, AsyncFd<OwnedFd>> = HashMap::new();
    loop {
        let (accept_ready, ready) = poll_fn(|cx| {
            let mut any = false;
            let mut accept = false;
            if let Poll::Ready(Ok(mut guard)) = listener.poll_read_ready(cx) {
                guard.clear_ready_matching(Ready::READABLE);
                accept = true;
                any = true;
            }
            let mut ready: Vec<(PeerId, bool, bool)> = Vec::new();
            for (&id, watcher) in &watchers {
                let Some(interest) = server.peer_interest(id) else {
                    ready.push((id, false, false));
                    any = true;
                    continue;
                };
                let mut readable = false;
                let mut writable = false;
                if interest.read
                    && let Poll::Ready(Ok(mut guard)) = watcher.poll_read_ready(cx)
                {
                    guard.clear_ready_matching(Ready::READABLE);
                    readable = true;
                }
                if interest.write
                    && let Poll::Ready(Ok(mut guard)) = watcher.poll_write_ready(cx)
                {
                    guard.clear_ready_matching(Ready::WRITABLE);
                    writable = true;
                }
                if readable || writable {
                    ready.push((id, readable, writable));
                    any = true;
                }
            }
            if any { Poll::Ready((accept, ready)) } else { Poll::Pending }
        })
        .await;

        if accept_ready {
            while let Some(id) = server.accept()? {
                let Some(fd) = server.peer_fd(id) else { continue };
                watchers.insert(id, watcher_for(fd)?);
            }
        }
        for (id, readable, writable) in ready {
            if server.peer_interest(id).is_none() {
                watchers.remove(&id);
                continue;
            }
            if writable && server.handle_writable(id).is_err() {
                watchers.remove(&id);
                continue;
            }
            if readable {
                if server.handle_readable(id).is_err() {
                    watchers.remove(&id);
                    continue;
                }
                while let Some(msg) = server.next_request(id) {
                    if !dispatch(server, id, msg)? {
                        return Ok(());
                    }
                }
            }
            if server.reap(id) || server.peer_interest(id).is_none() {
                watchers.remove(&id);
            }
        }
    }
}
