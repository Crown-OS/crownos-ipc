use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};

use crate::error::Error;

pub const RUNTIME_SUBDIR: &str = "crownos";

fn validate_service_name(service: &str) -> Result<(), Error> {
    let ok = !service.is_empty()
        && service.len() <= 64
        && service
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidServiceName(service.to_owned()))
    }
}

/// `$XDG_RUNTIME_DIR/crownos`, created 0700 if missing. The directory must
/// be owned by us; anything else is a spoofing attempt and is refused.
pub fn socket_dir() -> Result<PathBuf, Error> {
    let base = std::env::var_os("XDG_RUNTIME_DIR").ok_or(Error::RuntimeDir)?;
    let dir = PathBuf::from(base).join(RUNTIME_SUBDIR);
    ensure_dir(&dir)?;
    Ok(dir)
}

pub(crate) fn ensure_dir(dir: &Path) -> Result<(), Error> {
    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(Error::Io(e)),
    }
    let meta = std::fs::metadata(dir)?;
    if !meta.is_dir() || meta.uid() != rustix::process::geteuid().as_raw() {
        return Err(Error::RuntimeDir);
    }
    Ok(())
}

pub fn socket_path(service: &str) -> Result<PathBuf, Error> {
    validate_service_name(service)?;
    Ok(socket_dir()?.join(format!("{service}.sock")))
}

pub(crate) fn socket_path_in(dir: &Path, service: &str) -> Result<PathBuf, Error> {
    validate_service_name(service)?;
    Ok(dir.join(format!("{service}.sock")))
}

/// Bind a service socket with an atomic replace: bind a hidden temp name,
/// restrict its mode, then rename over the final path so connecting clients
/// never observe a missing or half-configured socket.
pub(crate) fn bind_service(dir: &Path, service: &str) -> Result<UnixListener, Error> {
    let final_path = socket_path_in(dir, service)?;
    ensure_dir(dir)?;
    let tmp_path = dir.join(format!(".{service}.sock.{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp_path);
    let listener = UnixListener::bind(&tmp_path)?;
    std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&tmp_path, &final_path)?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

pub(crate) fn connect(dir: &Path, service: &str) -> Result<UnixStream, Error> {
    let path = socket_path_in(dir, service)?;
    match UnixStream::connect(&path) {
        Ok(stream) => {
            stream.set_nonblocking(true)?;
            Ok(stream)
        }
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            Err(Error::ServiceUnavailable {
                service: service.to_owned(),
                path,
            })
        }
        Err(e) => Err(Error::Io(e)),
    }
}

#[cfg(test)]
pub(crate) fn test_dir() -> PathBuf {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("crownos-ipc-test-{}-{n}", std::process::id()));
    ensure_dir(&dir).expect("test dir");
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_bad_service_names() {
        for bad in [
            "",
            "UPPER",
            "with/slash",
            "with.dot",
            "a b",
            &"x".repeat(65),
        ] {
            assert!(matches!(
                validate_service_name(bad),
                Err(Error::InvalidServiceName(_))
            ));
        }
        assert!(validate_service_name("crown-pad_2").is_ok());
    }

    #[test]
    fn dir_and_socket_permissions() {
        let dir = test_dir();
        let _listener = bind_service(&dir, "permtest").expect("bind");
        let dir_mode = std::fs::metadata(&dir)
            .expect("dir meta")
            .permissions()
            .mode();
        assert_eq!(dir_mode & 0o777, 0o700);
        let sock_mode = std::fs::metadata(dir.join("permtest.sock"))
            .expect("sock meta")
            .permissions()
            .mode();
        assert_eq!(sock_mode & 0o777, 0o600);
    }

    #[test]
    fn atomic_replace_has_no_gap() {
        let dir = test_dir();
        let first = bind_service(&dir, "replace").expect("first bind");
        // rebinding replaces the socket; connect must succeed throughout
        connect(&dir, "replace").expect("connect to first");
        let second = bind_service(&dir, "replace").expect("second bind");
        let stream = connect(&dir, "replace").expect("connect after replace");
        // the connection landed on the new listener, not the old one
        second.set_nonblocking(false).expect("blocking accept");
        let (_conn, _) = second.accept().expect("second listener got the connection");
        drop((first, stream));
    }

    #[test]
    fn connect_to_missing_service_fails_cleanly() {
        let dir = test_dir();
        assert!(matches!(
            connect(&dir, "ghost"),
            Err(Error::ServiceUnavailable { .. })
        ));
    }
}
