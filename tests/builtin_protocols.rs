use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crownos_ipc::protocols::{compositor, crowncrate, crownotify, crownpad};
use crownos_ipc::{RemoteError, ServiceBuilder, blocking};

const TIMEOUT: Option<Duration> = Some(Duration::from_secs(5));

fn test_dir() -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("crownos-ipc-proto-{}-{n}", std::process::id()))
}

#[test]
fn crownpad_toggle_flow() {
    struct Pad {
        visible: bool,
        stop: bool,
    }
    impl crownpad::Handler for Pad {
        fn show(&mut self) {
            self.visible = true;
        }
        fn hide(&mut self) {
            self.visible = false;
            self.stop = true;
        }
        fn toggle(&mut self) {
            self.visible = !self.visible;
        }
    }

    let dir = test_dir();
    let dir2 = dir.clone();
    let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
    let ready2 = ready.clone();
    let server = std::thread::spawn(move || {
        let mut srv = ServiceBuilder::new("crownpad").directory(&dir2).build().expect("bind");
        ready2.wait();
        let mut pad = Pad { visible: false, stop: false };
        blocking::serve(&mut srv, |srv, peer, msg| {
            crownpad::dispatch(&mut pad, srv, peer, msg)?;
            srv.emit(&crownpad::Visibility { visible: pad.visible })?;
            Ok(!pad.stop)
        })
        .expect("serve");
        pad
    });
    ready.wait();

    let mut client = crownpad::Client::connect_in(&dir).expect("connect");
    client.subscribe_blocking::<crownpad::Visibility>(TIMEOUT).expect("subscribe");
    client.toggle().expect("toggle");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(crownpad::Event::Visibility(v)) = client.next_event() {
            assert!(v.visible);
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no visibility event");
        let _ = client.handle_readable();
        std::thread::sleep(Duration::from_millis(5));
    }
    client.hide().expect("hide");
    let pad = server.join().expect("thread");
    assert!(!pad.visible);
}

#[test]
fn crownotify_notification_flow() {
    struct Notifier {
        next_id: u32,
        stop: bool,
    }
    impl crownotify::Handler for Notifier {
        fn open_notification_center(&mut self) {}
        fn close_notification_center(&mut self) {
            self.stop = true;
        }
        fn send_general_notification(
            &mut self,
            app_name: String,
            _summary: String,
            _body: String,
            _expire_timeout: u32,
            actions: Vec<String>,
        ) -> Result<u32, RemoteError> {
            assert_eq!(app_name, "crowncrate");
            assert_eq!(actions, vec!["reply".to_owned()]);
            self.next_id += 1;
            Ok(self.next_id)
        }
        fn send_call_notification(
            &mut self,
            _call_id: String,
            _app_name: String,
            _contact_avatar: String,
            _contact_name: String,
            _phone_number: String,
        ) -> Result<u32, RemoteError> {
            self.next_id += 1;
            Ok(self.next_id)
        }
        fn send_music_notification(
            &mut self,
            _song_name: String,
            percentage: u8,
        ) -> Result<u32, RemoteError> {
            if percentage > 100 {
                return Err(RemoteError::handler("percentage out of range"));
            }
            self.next_id += 1;
            Ok(self.next_id)
        }
        fn send_chat_notification(
            &mut self,
            _app_name: String,
            _title: String,
            _body: String,
            _timestamp: String,
        ) -> Result<u32, RemoteError> {
            self.next_id += 1;
            Ok(self.next_id)
        }
        fn close_notification(&mut self, _id: u32) {}
    }

    let dir = test_dir();
    let dir2 = dir.clone();
    let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
    let ready2 = ready.clone();
    let server = std::thread::spawn(move || {
        let mut srv = ServiceBuilder::new("crownotify").directory(&dir2).build().expect("bind");
        ready2.wait();
        let mut n = Notifier { next_id: 0, stop: false };
        blocking::serve(&mut srv, |srv, peer, msg| {
            crownotify::dispatch(&mut n, srv, peer, msg)?;
            Ok(!n.stop)
        })
        .expect("serve");
    });
    ready.wait();

    let mut client = crownotify::Client::connect_in(&dir).expect("connect");
    let p = client
        .send_general_notification(
            "crowncrate".into(),
            "New message".into(),
            "hello".into(),
            5000,
            vec!["reply".into()],
        )
        .expect("send");
    assert_eq!(client.wait(p, TIMEOUT).expect("id"), 1);
    let p = client.send_music_notification("song".into(), 250).expect("send music");
    assert!(client.wait(p, TIMEOUT).is_err(), "out-of-range percentage must fail");
    client.close_notification_center().expect("stop");
    server.join().expect("thread");
}

#[test]
fn crowncrate_call_actions() {
    struct Crate {
        picked_up: Vec<String>,
        stop: bool,
    }
    impl crowncrate::Handler for Crate {
        fn pickup_call(&mut self, call_id: String) -> Result<(), RemoteError> {
            self.picked_up.push(call_id);
            Ok(())
        }
        fn decline_call(&mut self, _call_id: String) -> Result<(), RemoteError> {
            Ok(())
        }
        fn set_clipboard(&mut self, _contents: String) {}
        fn set_volume(&mut self, _value: u8) {}
        fn media_toggle(&mut self) {}
        fn open(&mut self, _target: String) {}
        fn otp_sync(&mut self) {
            self.stop = true;
        }
    }

    let dir = test_dir();
    let dir2 = dir.clone();
    let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
    let ready2 = ready.clone();
    let server = std::thread::spawn(move || {
        let mut srv = ServiceBuilder::new("crowncrate").directory(&dir2).build().expect("bind");
        ready2.wait();
        let mut c = Crate { picked_up: vec![], stop: false };
        blocking::serve(&mut srv, |srv, peer, msg| {
            crowncrate::dispatch(&mut c, srv, peer, msg)?;
            Ok(!c.stop)
        })
        .expect("serve");
        c
    });
    ready.wait();

    let mut client = crowncrate::Client::connect_in(&dir).expect("connect");
    let p = client.pickup_call("call-7".into()).expect("pickup");
    client.wait(p, TIMEOUT).expect("pickup ack");
    client.otp_sync().expect("stop");
    let state = server.join().expect("thread");
    assert_eq!(state.picked_up, vec!["call-7".to_owned()]);
}

#[test]
fn compositor_launch_flow() {
    struct Comp {
        launched: Vec<(String, Option<String>)>,
        stop: bool,
    }
    impl compositor::Handler for Comp {
        fn activation_token(&mut self, app_id: String) -> Result<String, RemoteError> {
            Ok(format!("token-for-{app_id}"))
        }
        fn launch(&mut self, exec: String, _app_id: String, token: Option<String>) {
            if exec == "stop" {
                self.stop = true;
            }
            self.launched.push((exec, token));
        }
    }

    let dir = test_dir();
    let dir2 = dir.clone();
    let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
    let ready2 = ready.clone();
    let server = std::thread::spawn(move || {
        let mut srv = ServiceBuilder::new("crownpositor").directory(&dir2).build().expect("bind");
        ready2.wait();
        let mut comp = Comp { launched: vec![], stop: false };
        blocking::serve(&mut srv, |srv, peer, msg| {
            compositor::dispatch(&mut comp, srv, peer, msg)?;
            Ok(!comp.stop)
        })
        .expect("serve");
        comp
    });
    ready.wait();

    let mut client = compositor::Client::connect_in(&dir).expect("connect");
    let p = client.activation_token("org.crownos.settings".into()).expect("token req");
    let token = client.wait(p, TIMEOUT).expect("token");
    assert_eq!(token, "token-for-org.crownos.settings");
    client
        .launch("crownsettings".into(), "org.crownos.settings".into(), Some(token.clone()))
        .expect("launch");
    client.launch("stop".into(), String::new(), None).expect("stop");
    let comp = server.join().expect("thread");
    assert_eq!(comp.launched[0], ("crownsettings".into(), Some(token)));
}
