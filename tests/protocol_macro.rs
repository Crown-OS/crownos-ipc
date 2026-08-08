use std::io::{Read as _, Seek as _, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crownos_ipc::{Error, MethodMsg, RemoteError, ServiceBuilder, blocking};

const TIMEOUT: Option<Duration> = Some(Duration::from_secs(5));

crownos_ipc::protocol! {
    /// Exercises every method flavor the macro supports.
    pub mod testproto: "macrotest" {
        /// Void request.
        fn poke();
        /// Request with args and a reply.
        fn add(a: u32, b: u32) -> u32;
        /// Request that can fail.
        fn must_fail(reason: String) -> u32;
        /// Fire-and-forget with args.
        notify set_flag(on: bool);
        /// Request carrying a file descriptor.
        fn read_fd(prefix: String) fds(file) -> String;
        /// Server-push events.
        event FlagChanged { on: bool };
        event Counted { total: u64 };
    }
}

struct TestHandler {
    flag: bool,
    pokes: u32,
}

impl testproto::Handler for TestHandler {
    fn poke(&mut self) -> Result<(), RemoteError> {
        self.pokes += 1;
        Ok(())
    }

    fn add(&mut self, a: u32, b: u32) -> Result<u32, RemoteError> {
        Ok(a + b)
    }

    fn must_fail(&mut self, reason: String) -> Result<u32, RemoteError> {
        Err(RemoteError::handler(reason))
    }

    fn set_flag(&mut self, on: bool) {
        self.flag = on;
    }

    fn read_fd(&mut self, prefix: String, file: std::os::fd::OwnedFd) -> Result<String, RemoteError> {
        let mut f = std::fs::File::from(file);
        f.seek(std::io::SeekFrom::Start(0))
            .map_err(|e| RemoteError::handler(e.to_string()))?;
        let mut contents = String::new();
        f.read_to_string(&mut contents)
            .map_err(|e| RemoteError::handler(e.to_string()))?;
        Ok(format!("{prefix}{contents}"))
    }
}

fn test_dir() -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("crownos-ipc-macro-{}-{n}", std::process::id()))
}

/// Serve until `set_flag(false)` arrives (used as the stop signal).
fn spawn_server(dir: PathBuf) -> std::thread::JoinHandle<TestHandler> {
    let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
    let ready2 = ready.clone();
    let handle = std::thread::spawn(move || {
        let mut server = ServiceBuilder::new("macrotest").directory(&dir).build().expect("bind");
        ready2.wait();
        let mut handler = TestHandler { flag: true, pokes: 0 };
        blocking::serve(&mut server, |srv, peer, msg| {
            let selector = msg.selector;
            testproto::dispatch(&mut handler, srv, peer, msg)?;
            if selector == <testproto::set_flag as MethodMsg>::SELECTOR {
                srv.emit(&testproto::FlagChanged { on: handler.flag })?;
                if !handler.flag {
                    return Ok(false);
                }
            }
            Ok(true)
        })
        .expect("serve");
        handler
    });
    ready.wait();
    handle
}

#[test]
fn full_protocol_round_trip() {
    let dir = test_dir();
    let server = spawn_server(dir.clone());
    let mut client = testproto::Client::connect_in(&dir).expect("connect");

    // void request
    let pending = client.poke().expect("poke");
    client.wait(pending, TIMEOUT).expect("poke reply");

    // request with reply
    let pending = client.add(20, 22).expect("add");
    assert_eq!(client.wait(pending, TIMEOUT).expect("add reply"), 42);

    // handler error
    let pending = client.must_fail("boom".into()).expect("must_fail send");
    match client.wait(pending, TIMEOUT) {
        Err(Error::Remote(remote)) => {
            assert_eq!(remote.code, RemoteError::HANDLER);
            assert_eq!(remote.message, "boom");
        }
        other => panic!("expected remote error, got {other:?}"),
    }

    // fd-carrying request
    let memfd = rustix::fs::memfd_create("macro-test", rustix::fs::MemfdFlags::CLOEXEC)
        .expect("memfd");
    let mut file = std::fs::File::from(memfd);
    file.write_all(b"world").expect("write");
    let pending = client.read_fd("hello ".into(), file.into()).expect("read_fd send");
    assert_eq!(client.wait(pending, TIMEOUT).expect("read_fd reply"), "hello world");

    // subscribe, then trigger an event via notify
    client.subscribe_blocking::<testproto::FlagChanged>(TIMEOUT).expect("subscribe");
    client.set_flag(true).expect("notify");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let event = loop {
        if let Some(e) = client.next_event() {
            break e;
        }
        assert!(std::time::Instant::now() < deadline, "event never arrived");
        let _ = client.handle_readable();
        std::thread::sleep(Duration::from_millis(5));
    };
    match event {
        testproto::Event::FlagChanged(e) => assert!(e.on),
        other => panic!("unexpected event {other:?}"),
    }

    // stop the server and verify handler state travelled through dispatch
    client.set_flag(false).expect("stop notify");
    let handler = server.join().expect("server thread");
    assert_eq!(handler.pokes, 1);
    assert!(!handler.flag);
}

#[test]
fn selectors_are_distinct_and_stable() {
    use crownos_ipc::{EventMsg, Kind};
    let selectors = [
        <testproto::poke as MethodMsg>::SELECTOR,
        <testproto::add as MethodMsg>::SELECTOR,
        <testproto::must_fail as MethodMsg>::SELECTOR,
        <testproto::set_flag as MethodMsg>::SELECTOR,
        <testproto::read_fd as MethodMsg>::SELECTOR,
        <testproto::FlagChanged as EventMsg>::SELECTOR,
        <testproto::Counted as EventMsg>::SELECTOR,
    ];
    for (i, a) in selectors.iter().enumerate() {
        for b in &selectors[i + 1..] {
            assert_ne!(a, b);
        }
    }
    assert_eq!(<testproto::poke as MethodMsg>::KIND, Kind::Request);
    assert_eq!(<testproto::set_flag as MethodMsg>::KIND, Kind::Notify);
    assert_eq!(<testproto::read_fd as MethodMsg>::FD_COUNT, 1);
    assert_eq!(<testproto::add as MethodMsg>::FD_COUNT, 0);
    assert_eq!(<testproto::poke as MethodMsg>::SERVICE, "macrotest");
}
