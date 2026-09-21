#![cfg(feature = "smol")]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use crownos_ipc::adapter::smol::{AsyncClient, serve};
use crownos_ipc::{RemoteError, ServiceBuilder};

crownos_ipc::protocol! {
    pub mod smolproto: "smoltest" {
        fn double(x: u64) -> u64;
        notify stop();
        event Tick { n: u32 };
    }
}

struct H {
    stop: bool,
}

impl smolproto::Handler for H {
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
    std::env::temp_dir().join(format!("crownos-ipc-smol-{}-{n}", std::process::id()))
}

#[test]
fn smol_call_and_events() {
    let dir = test_dir();
    let mut server = ServiceBuilder::new("smoltest")
        .directory(&dir)
        .build()
        .expect("bind");
    // one executor thread drives the server, the test thread blocks on the client
    let server_thread = std::thread::spawn(move || {
        futures_lite::future::block_on(async move {
            let mut h = H { stop: false };
            serve(&mut server, |srv, peer, msg| {
                let selector = msg.selector;
                smolproto::dispatch(&mut h, srv, peer, msg)?;
                if selector == <smolproto::double as crownos_ipc::MethodMsg>::SELECTOR {
                    srv.emit(&smolproto::Tick { n: 7 })?;
                }
                Ok(!h.stop)
            })
            .await
            .expect("serve");
        });
    });

    futures_lite::future::block_on(async {
        let mut client = AsyncClient::connect_in(&dir, "smoltest").expect("connect");
        client
            .subscribe::<smolproto::Tick>()
            .await
            .expect("subscribe");
        let doubled = client
            .call::<smolproto::double>(&smolproto::double { x: 8 }, vec![])
            .await
            .expect("call");
        assert_eq!(doubled, 16);
        let tick = client.next_event::<smolproto::Tick>().await.expect("event");
        assert_eq!(tick, smolproto::Tick { n: 7 });
        client
            .notify::<smolproto::stop>(&smolproto::stop {}, vec![])
            .await
            .expect("stop");
    });

    server_thread.join().expect("server thread");
}
