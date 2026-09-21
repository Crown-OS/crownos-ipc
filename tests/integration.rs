use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crownos_ipc::{
    Authorizer, Client, Error, EventMsg, Kind, Limits, MethodMsg, RemoteError, ServiceBuilder,
    blocking,
};

const TIMEOUT: Option<Duration> = Some(Duration::from_secs(5));

fn test_dir() -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("crownos-ipc-it-{}-{n}", std::process::id()))
}

#[derive(Serialize, Deserialize)]
struct Echo {
    text: String,
}

impl MethodMsg for Echo {
    type Args = Echo;
    type Reply = String;
    const SERVICE: &'static str = "testsvc";
    const SELECTOR: u64 = crownos_ipc::hash::selector("testsvc", "echo", "(String) -> String");
    const KIND: Kind = Kind::Request;
    const FD_COUNT: usize = 0;
}

#[derive(Serialize, Deserialize)]
struct Fail;

impl MethodMsg for Fail {
    type Args = Fail;
    type Reply = ();
    const SERVICE: &'static str = "testsvc";
    const SELECTOR: u64 = crownos_ipc::hash::selector("testsvc", "fail", "() -> ()");
    const KIND: Kind = Kind::Request;
    const FD_COUNT: usize = 0;
}

#[derive(Serialize, Deserialize)]
struct Forged;

impl MethodMsg for Forged {
    type Args = Forged;
    type Reply = ();
    const SERVICE: &'static str = "testsvc";
    const SELECTOR: u64 = 0xdead_dead_dead_dead;
    const KIND: Kind = Kind::Request;
    const FD_COUNT: usize = 0;
}

#[derive(Serialize, Deserialize)]
struct EmitPing;

impl MethodMsg for EmitPing {
    type Args = EmitPing;
    type Reply = ();
    const SERVICE: &'static str = "testsvc";
    const SELECTOR: u64 = crownos_ipc::hash::selector("testsvc", "emit_ping", "() -> ()");
    const KIND: Kind = Kind::Request;
    const FD_COUNT: usize = 0;
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Ping {
    seq: u32,
}

impl EventMsg for Ping {
    const SERVICE: &'static str = "testsvc";
    const SELECTOR: u64 = crownos_ipc::hash::selector("testsvc", "event.ping", "{u32}");
}

#[derive(Serialize, Deserialize)]
struct Shutdown;

impl MethodMsg for Shutdown {
    type Args = Shutdown;
    type Reply = ();
    const SERVICE: &'static str = "testsvc";
    const SELECTOR: u64 = crownos_ipc::hash::selector("testsvc", "shutdown", "() -> ()");
    const KIND: Kind = Kind::Request;
    const FD_COUNT: usize = 0;
}

/// Serve `testsvc` on a background thread until a `Shutdown` request arrives.
fn spawn_server(
    dir: PathBuf,
    authorizer: Authorizer,
    limits: Limits,
) -> std::thread::JoinHandle<()> {
    let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
    let ready2 = ready.clone();
    let handle = std::thread::spawn(move || {
        let mut server = ServiceBuilder::new("testsvc")
            .directory(&dir)
            .authorizer(authorizer)
            .limits(limits)
            .build()
            .expect("bind test service");
        ready2.wait();
        let mut ping_seq = 0u32;
        blocking::serve(&mut server, |srv, peer, msg| {
            if msg.selector == Echo::SELECTOR {
                let args: Echo = postcard::from_bytes(&msg.payload)?;
                let reply = postcard::to_stdvec(&args.text)?;
                srv.reply(peer, msg.req_id, &reply, vec![])?;
            } else if msg.selector == Fail::SELECTOR {
                srv.reply_err(peer, msg.req_id, &RemoteError::handler("nope"))?;
            } else if msg.selector == EmitPing::SELECTOR {
                srv.reply(peer, msg.req_id, &postcard::to_stdvec(&())?, vec![])?;
                ping_seq += 1;
                srv.emit(&Ping { seq: ping_seq })?;
            } else if msg.selector == Shutdown::SELECTOR {
                srv.reply(peer, msg.req_id, &postcard::to_stdvec(&())?, vec![])?;
                return Ok(false);
            } else if msg.kind == Kind::Request {
                srv.reply_err(peer, msg.req_id, &RemoteError::unknown_method(msg.selector))?;
            }
            Ok(true)
        })
        .expect("serve loop");
    });
    ready.wait();
    handle
}

fn shutdown(dir: &std::path::Path) {
    let mut c = Client::connect_in(dir, "testsvc").expect("connect for shutdown");
    c.call_blocking::<Shutdown>(&Shutdown, vec![], TIMEOUT)
        .expect("shutdown");
}

#[test]
fn echo_round_trip() {
    let dir = test_dir();
    let server = spawn_server(dir.clone(), Authorizer::SameUid, Limits::default());
    let mut client = Client::connect_in(&dir, "testsvc").expect("connect");
    let reply = client
        .call_blocking::<Echo>(
            &Echo {
                text: "hello".into(),
            },
            vec![],
            TIMEOUT,
        )
        .expect("echo");
    assert_eq!(reply, "hello");
    shutdown(&dir);
    server.join().expect("server thread");
}

#[test]
fn pipelined_out_of_order_correlation() {
    let dir = test_dir();
    let server = spawn_server(dir.clone(), Authorizer::SameUid, Limits::default());
    let mut client = Client::connect_in(&dir, "testsvc").expect("connect");
    let p1 = client
        .start_request::<Echo>(&Echo { text: "one".into() }, vec![])
        .expect("p1");
    let p2 = client
        .start_request::<Echo>(&Echo { text: "two".into() }, vec![])
        .expect("p2");
    let p3 = client
        .start_request::<Echo>(
            &Echo {
                text: "three".into(),
            },
            vec![],
        )
        .expect("p3");
    // resolve in reverse order of sending
    assert_eq!(client.wait(p3, TIMEOUT).expect("r3"), "three");
    assert_eq!(client.wait(p1, TIMEOUT).expect("r1"), "one");
    assert_eq!(client.wait(p2, TIMEOUT).expect("r2"), "two");
    shutdown(&dir);
    server.join().expect("server thread");
}

#[test]
fn remote_error_propagates() {
    let dir = test_dir();
    let server = spawn_server(dir.clone(), Authorizer::SameUid, Limits::default());
    let mut client = Client::connect_in(&dir, "testsvc").expect("connect");
    let err = client
        .call_blocking::<Fail>(&Fail, vec![], TIMEOUT)
        .expect_err("must fail");
    match err {
        Error::Remote(remote) => {
            assert_eq!(remote.code, RemoteError::HANDLER);
            assert_eq!(remote.message, "nope");
        }
        other => panic!("expected remote error, got {other}"),
    }
    shutdown(&dir);
    server.join().expect("server thread");
}

#[test]
fn forged_selector_fails_loudly() {
    let dir = test_dir();
    let server = spawn_server(dir.clone(), Authorizer::SameUid, Limits::default());
    let mut client = Client::connect_in(&dir, "testsvc").expect("connect");
    let err = client
        .call_blocking::<Forged>(&Forged, vec![], TIMEOUT)
        .expect_err("unknown");
    match err {
        Error::Remote(remote) => assert_eq!(remote.code, RemoteError::UNKNOWN_METHOD),
        other => panic!("expected unknown-method error, got {other}"),
    }
    shutdown(&dir);
    server.join().expect("server thread");
}

#[test]
fn events_reach_only_subscribers() {
    let dir = test_dir();
    let server = spawn_server(dir.clone(), Authorizer::SameUid, Limits::default());
    let mut subscriber = Client::connect_in(&dir, "testsvc").expect("connect sub");
    let mut bystander = Client::connect_in(&dir, "testsvc").expect("connect bys");
    subscriber
        .subscribe_blocking::<Ping>(TIMEOUT)
        .expect("subscribe");
    // trigger an event from the bystander connection
    bystander
        .call_blocking::<EmitPing>(&EmitPing, vec![], TIMEOUT)
        .expect("emit");
    // subscriber sees it
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let event = loop {
        if let Some(e) = subscriber.next_event::<Ping>() {
            break e;
        }
        assert!(std::time::Instant::now() < deadline, "event never arrived");
        let _ = subscriber.handle_readable();
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(event, Ping { seq: 1 });
    // bystander (not subscribed) must have nothing queued
    bystander.handle_readable().expect("pump bystander");
    assert!(bystander.next_event::<Ping>().is_none());
    shutdown(&dir);
    server.join().expect("server thread");
}

#[test]
fn unauthorized_peer_is_dropped() {
    let dir = test_dir();
    let server = spawn_server(
        dir.clone(),
        Authorizer::Custom(Box::new(|_| false)),
        Limits::default(),
    );
    let mut client = Client::connect_in(&dir, "testsvc").expect("kernel-level connect succeeds");
    let err = client
        .call_blocking::<Echo>(&Echo { text: "hi".into() }, vec![], TIMEOUT)
        .expect_err("must be rejected");
    assert!(matches!(err, Error::Disconnected), "got {err}");
    drop(server); // serve loop has no live peers; kill it via thread abandonment
}

#[test]
fn per_uid_connection_cap() {
    let dir = test_dir();
    let limits = Limits {
        max_conns_per_uid: 2,
        ..Default::default()
    };
    let server = spawn_server(dir.clone(), Authorizer::SameUid, limits);
    let mut c1 = Client::connect_in(&dir, "testsvc").expect("c1");
    let mut c2 = Client::connect_in(&dir, "testsvc").expect("c2");
    assert_eq!(
        c1.call_blocking::<Echo>(&Echo { text: "1".into() }, vec![], TIMEOUT)
            .expect("c1 works"),
        "1"
    );
    assert_eq!(
        c2.call_blocking::<Echo>(&Echo { text: "2".into() }, vec![], TIMEOUT)
            .expect("c2 works"),
        "2"
    );
    let mut c3 = Client::connect_in(&dir, "testsvc").expect("c3 kernel connect");
    let err = c3
        .call_blocking::<Echo>(&Echo { text: "3".into() }, vec![], TIMEOUT)
        .expect_err("over cap");
    assert!(matches!(err, Error::Disconnected), "got {err}");
    // survivors still work
    assert_eq!(
        c1.call_blocking::<Echo>(
            &Echo {
                text: "again".into()
            },
            vec![],
            TIMEOUT
        )
        .expect("c1"),
        "again"
    );
    // free slots, then retry shutdown until the server has reaped them
    drop((c1, c2));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut c = Client::connect_in(&dir, "testsvc").expect("connect for shutdown");
        match c.call_blocking::<Shutdown>(&Shutdown, vec![], TIMEOUT) {
            Ok(()) => break,
            Err(Error::Disconnected) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("shutdown: {e}"),
        }
    }
    server.join().expect("server thread");
}

#[test]
fn server_death_mid_call_disconnects() {
    let dir = test_dir();
    let server = spawn_server(dir.clone(), Authorizer::SameUid, Limits::default());
    let mut client = Client::connect_in(&dir, "testsvc").expect("connect");
    // shutting down from another connection kills the serve loop
    shutdown(&dir);
    server.join().expect("server thread");
    let err = client
        .call_blocking::<Echo>(
            &Echo {
                text: "too late".into(),
            },
            vec![],
            TIMEOUT,
        )
        .expect_err("server gone");
    assert!(
        matches!(err, Error::Disconnected | Error::Io(_)),
        "got {err}"
    );
}

#[test]
fn slow_subscriber_loses_events_not_the_server() {
    let dir = test_dir();
    let server = spawn_server(dir.clone(), Authorizer::SameUid, Limits::default());
    let mut lazy = Client::connect_in(&dir, "testsvc").expect("connect lazy");
    lazy.subscribe_blocking::<Ping>(TIMEOUT).expect("subscribe");
    // lazy never pumps again; flood it with events via another connection
    let mut driver = Client::connect_in(&dir, "testsvc").expect("connect driver");
    for _ in 0..5000 {
        driver
            .call_blocking::<EmitPing>(&EmitPing, vec![], TIMEOUT)
            .expect("emit");
    }
    // server must still serve promptly despite the wedged subscriber
    assert_eq!(
        driver
            .call_blocking::<Echo>(&Echo { text: "ok".into() }, vec![], TIMEOUT)
            .expect("echo"),
        "ok"
    );
    shutdown(&dir);
    drop(lazy);
    server.join().expect("server thread");
}

#[test]
fn garbage_input_does_not_kill_server() {
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    let dir = test_dir();
    let server = spawn_server(dir.clone(), Authorizer::SameUid, Limits::default());
    // hurl random-ish garbage at the socket from several raw connections
    for i in 0u8..8 {
        let mut raw = UnixStream::connect(dir.join("testsvc.sock")).expect("raw connect");
        let garbage: Vec<u8> = (0..2048)
            .map(|j| (i as usize * 31 + j * 7) as u8 | 0x80)
            .collect();
        let _ = raw.write_all(&garbage);
    }
    // a well-behaved client still gets service
    let mut client = Client::connect_in(&dir, "testsvc").expect("connect");
    let reply = client
        .call_blocking::<Echo>(
            &Echo {
                text: "alive".into(),
            },
            vec![],
            TIMEOUT,
        )
        .expect("server survived");
    assert_eq!(reply, "alive");
    shutdown(&dir);
    server.join().expect("server thread");
}
