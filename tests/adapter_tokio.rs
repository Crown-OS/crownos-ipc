#![cfg(feature = "tokio")]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use crownos_ipc::adapter::tokio::{AsyncClient, serve};
use crownos_ipc::{RemoteError, ServiceBuilder};

crownos_ipc::protocol! {
    pub mod tokproto: "toktest" {
        fn double(x: u64) -> u64;
        notify stop();
        event Tick { n: u32 };
    }
}

struct H {
    stop: bool,
}

impl tokproto::Handler for H {
    fn double(&mut self, x: u64) -> Result<u64, RemoteError> {
        Ok(x * 2)
    }
    fn stop(&mut self) {
        self.stop = true;
    }
}

fn test_dir() -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("crownos-ipc-tok-{}-{n}", std::process::id()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_call_and_events() {
    let dir = test_dir();
    let mut server = ServiceBuilder::new("toktest").directory(&dir).build().expect("bind");
    let server_task = tokio::spawn(async move {
        let mut h = H { stop: false };
        serve(&mut server, |srv, peer, msg| {
            let selector = msg.selector;
            tokproto::dispatch(&mut h, srv, peer, msg)?;
            if selector == <tokproto::double as crownos_ipc::MethodMsg>::SELECTOR {
                srv.emit(&tokproto::Tick { n: 1 })?;
            }
            Ok(!h.stop)
        })
        .await
        .expect("serve");
    });

    let mut client = AsyncClient::connect_in(&dir, "toktest").expect("connect");
    client.subscribe::<tokproto::Tick>().await.expect("subscribe");

    let doubled = client
        .call::<tokproto::double>(&tokproto::double { x: 21 }, vec![])
        .await
        .expect("call");
    assert_eq!(doubled, 42);

    let tick = client.next_event::<tokproto::Tick>().await.expect("event");
    assert_eq!(tick, tokproto::Tick { n: 1 });

    client.notify::<tokproto::stop>(&tokproto::stop {}, vec![]).await.expect("stop");
    tokio::time::timeout(std::time::Duration::from_secs(5), server_task)
        .await
        .expect("server did not stop")
        .expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_concurrent_clients() {
    let dir = test_dir();
    let mut server = ServiceBuilder::new("toktest").directory(&dir).build().expect("bind");
    let server_task = tokio::spawn(async move {
        let mut h = H { stop: false };
        serve(&mut server, |srv, peer, msg| {
            tokproto::dispatch(&mut h, srv, peer, msg)?;
            Ok(!h.stop)
        })
        .await
        .expect("serve");
    });

    let mut tasks = Vec::new();
    for i in 0..8u64 {
        let dir = dir.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = AsyncClient::connect_in(&dir, "toktest").expect("connect");
            let r = c.call::<tokproto::double>(&tokproto::double { x: i }, vec![]).await.expect("call");
            assert_eq!(r, i * 2);
        }));
    }
    for t in tasks {
        t.await.expect("client task");
    }

    let mut c = AsyncClient::connect_in(&dir, "toktest").expect("connect");
    c.notify::<tokproto::stop>(&tokproto::stop {}, vec![]).await.expect("stop");
    tokio::time::timeout(std::time::Duration::from_secs(5), server_task)
        .await
        .expect("server did not stop")
        .expect("server task");
}
