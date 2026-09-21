//! smol / async-io adapter (feature `smol`).

use std::collections::HashMap;
use std::future::poll_fn;
use std::os::fd::{AsFd, OwnedFd};
use std::path::Path;
use std::task::Poll;

use async_io::Async;
use serde::de::DeserializeOwned;

use crate::client::{Client, Pending};
use crate::error::Error;
use crate::proto::{EventMsg, MethodMsg};
use crate::server::{PeerId, Server};

fn watcher_for(fd: impl AsFd) -> Result<Async<OwnedFd>, Error> {
    let dup = fd.as_fd().try_clone_to_owned()?;
    Async::new(dup).map_err(Error::Io)
}

/// Async wrapper over [`Client`] for smol / async-io executors.
pub struct AsyncClient {
    client: Client,
    watcher: Async<OwnedFd>,
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
            self.watcher.writable().await?;
            self.client.handle_writable()?;
        }
        Ok(())
    }

    async fn pump(&mut self) -> Result<(), Error> {
        self.flush().await?;
        self.watcher.readable().await?;
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

/// Drive a [`Server`] on a smol / async-io executor. `dispatch` receives
/// each application message and ends the loop by returning `Ok(false)`.
pub async fn serve<F>(server: &mut Server, mut dispatch: F) -> Result<(), Error>
where
    F: FnMut(&mut Server, PeerId, crate::Message) -> Result<bool, Error>,
{
    let listener = watcher_for(&*server)?;
    let mut watchers: HashMap<PeerId, Async<OwnedFd>> = HashMap::new();
    loop {
        let (accept_ready, ready) = poll_fn(|cx| {
            let mut any = false;
            let mut accept = false;
            if listener.poll_readable(cx).is_ready() {
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
                let readable = interest.read && watcher.poll_readable(cx).is_ready();
                let writable = interest.write && watcher.poll_writable(cx).is_ready();
                if readable || writable {
                    ready.push((id, readable, writable));
                    any = true;
                }
            }
            if any {
                Poll::Ready((accept, ready))
            } else {
                Poll::Pending
            }
        })
        .await;

        if accept_ready {
            while let Some(id) = server.accept()? {
                let Some(fd) = server.peer_fd(id) else {
                    continue;
                };
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
