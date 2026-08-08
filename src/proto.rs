use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::frame::Kind;

/// A callable method of a protocol. Implemented by the [`crate::protocol!`]
/// macro on the per-method argument struct itself.
pub trait MethodMsg {
    type Args: Serialize + DeserializeOwned;
    type Reply: Serialize + DeserializeOwned;
    const SERVICE: &'static str;
    const SELECTOR: u64;
    /// `Kind::Request` for `fn` methods, `Kind::Notify` for `notify` methods.
    const KIND: Kind;
    /// Number of file descriptors this method carries.
    const FD_COUNT: usize;
}

/// A server-push event of a protocol, delivered only to subscribers.
pub trait EventMsg: Serialize + DeserializeOwned {
    const SERVICE: &'static str;
    const SELECTOR: u64;
}

/// Define a protocol once; both ends are generated from it so client and
/// server can never drift apart.
///
/// ```ignore
/// crownos_ipc::protocol! {
///     /// Example service.
///     pub mod example: "example" {
///         /// Fire-and-forget.
///         notify ping();
///         /// Request with a reply.
///         fn add(a: u32, b: u32) -> u32;
///         /// Request carrying one file descriptor.
///         fn set_image(name: String) fds(image);
///         /// Server-push event (delivered after `subscribe`).
///         event Changed { value: u32 };
///     }
/// }
/// ```
///
/// This expands to a module containing:
/// - one serde struct per method (named like the method) implementing
///   [`MethodMsg`], with a compile-time selector derived from the service
///   name, method name and full signature spelling;
/// - `Client`, a typed wrapper over [`crate::Client`] (also reachable via
///   `Deref`) with one method per protocol method and `next_event()`;
/// - `Handler`, a trait with one required method per protocol method —
///   an implementation missing a method does not compile;
/// - `dispatch(handler, server, peer, msg)`, routing one message to a
///   `Handler` and sending the reply/error back;
/// - one serde struct per event implementing [`EventMsg`], plus an `Event`
///   enum over all of them.
///
/// Consumers need `serde` (with `derive`) in their own dependencies.
#[macro_export]
macro_rules! protocol {
    ( $( $(#[$mattr:meta])* $vis:vis mod $mod_name:ident : $service:literal { $($body:tt)* } )+ ) => {
        $(
            $(#[$mattr])*
            $vis mod $mod_name {
                #[allow(unused_imports)]
                use super::*;

                pub const SERVICE: &str = $service;

                $crate::__proto_methods! { $service ; $($body)* }

                /// Server-side implementation surface: every protocol method
                /// must be implemented, so drift is a compile error.
                pub trait Handler {
                    $crate::__proto_handler_fns! { $($body)* }
                }

                /// Route one inbound message to `handler`, replying as needed.
                /// Misbehaving peers are dropped; the returned error is only
                /// ever fatal to the server itself.
                pub fn dispatch<H: Handler>(
                    handler: &mut H,
                    server: &mut $crate::Server,
                    peer: $crate::PeerId,
                    msg: $crate::Message,
                ) -> ::core::result::Result<(), $crate::Error> {
                    let $crate::Message { kind, selector, req_id, payload, mut fds } = msg;
                    let _ = (&payload, &mut fds);
                    $crate::__proto_dispatch_arms! {
                        (handler server peer kind selector req_id payload fds) $($body)*
                    }
                    if kind == $crate::Kind::Request {
                        let _ = server.reply_err(
                            peer,
                            req_id,
                            &$crate::RemoteError::unknown_method(selector),
                        );
                    }
                    ::core::result::Result::Ok(())
                }

                /// Typed connection to this service. `Deref`s to
                /// [`crownos_ipc::Client`](crate::Client) for `wait`,
                /// `subscribe`, event-loop pumping, etc.
                pub struct Client {
                    inner: $crate::Client,
                }

                impl Client {
                    pub fn connect() -> ::core::result::Result<Self, $crate::Error> {
                        ::core::result::Result::Ok(Self { inner: $crate::Client::connect(SERVICE)? })
                    }

                    pub fn connect_in(
                        dir: &::std::path::Path,
                    ) -> ::core::result::Result<Self, $crate::Error> {
                        ::core::result::Result::Ok(Self {
                            inner: $crate::Client::connect_in(dir, SERVICE)?,
                        })
                    }

                    pub fn from_inner(inner: $crate::Client) -> Self {
                        Self { inner }
                    }

                    pub fn into_inner(self) -> $crate::Client {
                        self.inner
                    }

                    $crate::__proto_client_fns! { $($body)* }
                }

                impl ::std::ops::Deref for Client {
                    type Target = $crate::Client;
                    fn deref(&self) -> &$crate::Client {
                        &self.inner
                    }
                }

                impl ::std::ops::DerefMut for Client {
                    fn deref_mut(&mut self) -> &mut $crate::Client {
                        &mut self.inner
                    }
                }

                impl ::std::os::fd::AsFd for Client {
                    fn as_fd(&self) -> ::std::os::fd::BorrowedFd<'_> {
                        ::std::os::fd::AsFd::as_fd(&self.inner)
                    }
                }

                $crate::__proto_events! { $service ; [] $($body)* }
            }
        )+
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __reply_ty {
    () => { () };
    ($t:ty) => { $t };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __count {
    () => { 0usize };
    ($head:ident $($tail:ident)*) => { 1usize + $crate::__count!($($tail)*) };
}

/// Per-method structs + `MethodMsg` impls.
#[doc(hidden)]
#[macro_export]
macro_rules! __proto_methods {
    ( $service:literal ; ) => {};
    ( $service:literal ;
      $(#[$attr:meta])*
      fn $name:ident ( $($a:ident : $t:ty),* $(,)? ) $(fds ( $($fd:ident),* $(,)? ))? $(-> $ret:ty)? ;
      $($rest:tt)*
    ) => {
        $crate::__proto_method_item! {
            $service ; $crate::Kind::Request ;
            $(#[$attr])* $name ( $($a : $t),* ) ( $($($fd)*)? ) ( $($ret)? )
        }
        $crate::__proto_methods! { $service ; $($rest)* }
    };
    ( $service:literal ;
      $(#[$attr:meta])*
      notify $name:ident ( $($a:ident : $t:ty),* $(,)? ) $(fds ( $($fd:ident),* $(,)? ))? ;
      $($rest:tt)*
    ) => {
        $crate::__proto_method_item! {
            $service ; $crate::Kind::Notify ;
            $(#[$attr])* $name ( $($a : $t),* ) ( $($($fd)*)? ) ( )
        }
        $crate::__proto_methods! { $service ; $($rest)* }
    };
    ( $service:literal ;
      $(#[$attr:meta])*
      event $name:ident { $($f:ident : $ft:ty),* $(,)? } ;
      $($rest:tt)*
    ) => {
        $crate::__proto_methods! { $service ; $($rest)* }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __proto_method_item {
    ( $service:literal ; $kind:expr ;
      $(#[$attr:meta])* $name:ident ( $($a:ident : $t:ty),* ) ( $($fd:ident)* ) ( $($ret:ty)? )
    ) => {
        $(#[$attr])*
        #[allow(non_camel_case_types)]
        #[derive(::serde::Serialize, ::serde::Deserialize)]
        pub struct $name {
            $(pub $a: $t,)*
        }

        impl $crate::MethodMsg for $name {
            type Args = Self;
            type Reply = $crate::__reply_ty!($($ret)?);
            const SERVICE: &'static str = $service;
            const SELECTOR: u64 = $crate::hash::selector(
                $service,
                stringify!($name),
                stringify!(( $($a : $t),* ) fds( $($fd),* ) -> ( $($ret)? )),
            );
            const KIND: $crate::Kind = $kind;
            const FD_COUNT: usize = $crate::__count!($($fd)*);
        }
    };
}

/// Required methods inside the generated `Handler` trait.
#[doc(hidden)]
#[macro_export]
macro_rules! __proto_handler_fns {
    () => {};
    ( $(#[$attr:meta])*
      fn $name:ident ( $($a:ident : $t:ty),* $(,)? ) $(fds ( $($fd:ident),* $(,)? ))? $(-> $ret:ty)? ;
      $($rest:tt)*
    ) => {
        $(#[$attr])*
        fn $name(
            &mut self,
            $($a: $t,)*
            $($($fd: ::std::os::fd::OwnedFd,)*)?
        ) -> ::core::result::Result<$crate::__reply_ty!($($ret)?), $crate::RemoteError>;
        $crate::__proto_handler_fns! { $($rest)* }
    };
    ( $(#[$attr:meta])*
      notify $name:ident ( $($a:ident : $t:ty),* $(,)? ) $(fds ( $($fd:ident),* $(,)? ))? ;
      $($rest:tt)*
    ) => {
        $(#[$attr])*
        fn $name(&mut self, $($a: $t,)* $($($fd: ::std::os::fd::OwnedFd,)*)?);
        $crate::__proto_handler_fns! { $($rest)* }
    };
    ( $(#[$attr:meta])*
      event $name:ident { $($f:tt)* } ;
      $($rest:tt)*
    ) => {
        $crate::__proto_handler_fns! { $($rest)* }
    };
}

/// Statement sequence inside the generated `dispatch` function.
#[doc(hidden)]
#[macro_export]
macro_rules! __proto_dispatch_arms {
    ( ($handler:ident $server:ident $peer:ident $kind:ident $selector:ident $req_id:ident $payload:ident $fds:ident) ) => {};
    ( ($handler:ident $server:ident $peer:ident $kind:ident $selector:ident $req_id:ident $payload:ident $fds:ident)
      $(#[$attr:meta])*
      fn $name:ident ( $($a:ident : $t:ty),* $(,)? ) $(fds ( $($fd:ident),* $(,)? ))? $(-> $ret:ty)? ;
      $($rest:tt)*
    ) => {
        if $selector == <$name as $crate::MethodMsg>::SELECTOR {
            if $kind != $crate::Kind::Request {
                $server.drop_peer($peer);
                return ::core::result::Result::Ok(());
            }
            let __args: $name = match $crate::postcard::from_bytes(&$payload) {
                ::core::result::Result::Ok(a) => a,
                ::core::result::Result::Err(_) => {
                    $server.drop_peer($peer);
                    return ::core::result::Result::Ok(());
                }
            };
            let mut __fd_iter = $fds.drain(..);
            $($(
                let $fd = match __fd_iter.next() {
                    ::core::option::Option::Some(fd) => fd,
                    ::core::option::Option::None => {
                        drop(__fd_iter);
                        $server.drop_peer($peer);
                        return ::core::result::Result::Ok(());
                    }
                };
            )*)?
            drop(__fd_iter);
            match Handler::$name($handler, $(__args.$a,)* $($($fd,)*)?) {
                ::core::result::Result::Ok(__reply) => {
                    match $crate::postcard::to_stdvec(&__reply) {
                        ::core::result::Result::Ok(__payload) => {
                            let _ = $server.reply($peer, $req_id, &__payload, ::std::vec::Vec::new());
                        }
                        ::core::result::Result::Err(_) => {
                            let _ = $server.reply_err(
                                $peer,
                                $req_id,
                                &$crate::RemoteError::handler("reply serialization failed"),
                            );
                        }
                    }
                }
                ::core::result::Result::Err(__err) => {
                    let _ = $server.reply_err($peer, $req_id, &__err);
                }
            }
            return ::core::result::Result::Ok(());
        }
        $crate::__proto_dispatch_arms! {
            ($handler $server $peer $kind $selector $req_id $payload $fds) $($rest)*
        }
    };
    ( ($handler:ident $server:ident $peer:ident $kind:ident $selector:ident $req_id:ident $payload:ident $fds:ident)
      $(#[$attr:meta])*
      notify $name:ident ( $($a:ident : $t:ty),* $(,)? ) $(fds ( $($fd:ident),* $(,)? ))? ;
      $($rest:tt)*
    ) => {
        if $selector == <$name as $crate::MethodMsg>::SELECTOR {
            if $kind != $crate::Kind::Notify {
                $server.drop_peer($peer);
                return ::core::result::Result::Ok(());
            }
            let __args: $name = match $crate::postcard::from_bytes(&$payload) {
                ::core::result::Result::Ok(a) => a,
                ::core::result::Result::Err(_) => {
                    $server.drop_peer($peer);
                    return ::core::result::Result::Ok(());
                }
            };
            let mut __fd_iter = $fds.drain(..);
            $($(
                let $fd = match __fd_iter.next() {
                    ::core::option::Option::Some(fd) => fd,
                    ::core::option::Option::None => {
                        drop(__fd_iter);
                        $server.drop_peer($peer);
                        return ::core::result::Result::Ok(());
                    }
                };
            )*)?
            drop(__fd_iter);
            Handler::$name($handler, $(__args.$a,)* $($($fd,)*)?);
            return ::core::result::Result::Ok(());
        }
        $crate::__proto_dispatch_arms! {
            ($handler $server $peer $kind $selector $req_id $payload $fds) $($rest)*
        }
    };
    ( ($handler:ident $server:ident $peer:ident $kind:ident $selector:ident $req_id:ident $payload:ident $fds:ident)
      $(#[$attr:meta])*
      event $name:ident { $($f:tt)* } ;
      $($rest:tt)*
    ) => {
        $crate::__proto_dispatch_arms! {
            ($handler $server $peer $kind $selector $req_id $payload $fds) $($rest)*
        }
    };
}

/// Typed methods inside the generated `Client` impl.
#[doc(hidden)]
#[macro_export]
macro_rules! __proto_client_fns {
    () => {};
    ( $(#[$attr:meta])*
      fn $name:ident ( $($a:ident : $t:ty),* $(,)? ) $(fds ( $($fd:ident),* $(,)? ))? $(-> $ret:ty)? ;
      $($rest:tt)*
    ) => {
        $(#[$attr])*
        pub fn $name(
            &mut self,
            $($a: $t,)*
            $($($fd: ::std::os::fd::OwnedFd,)*)?
        ) -> ::core::result::Result<$crate::Pending<$crate::__reply_ty!($($ret)?)>, $crate::Error> {
            self.inner.start_request::<$name>(
                &$name { $($a,)* },
                ::std::vec![$($($fd),*)?],
            )
        }
        $crate::__proto_client_fns! { $($rest)* }
    };
    ( $(#[$attr:meta])*
      notify $name:ident ( $($a:ident : $t:ty),* $(,)? ) $(fds ( $($fd:ident),* $(,)? ))? ;
      $($rest:tt)*
    ) => {
        $(#[$attr])*
        pub fn $name(
            &mut self,
            $($a: $t,)*
            $($($fd: ::std::os::fd::OwnedFd,)*)?
        ) -> ::core::result::Result<(), $crate::Error> {
            self.inner.notify::<$name>(
                &$name { $($a,)* },
                ::std::vec![$($($fd),*)?],
            )
        }
        $crate::__proto_client_fns! { $($rest)* }
    };
    ( $(#[$attr:meta])*
      event $name:ident { $($f:tt)* } ;
      $($rest:tt)*
    ) => {
        $crate::__proto_client_fns! { $($rest)* }
    };
}

/// Event structs, the `Event` enum, and `Client::next_event`.
#[doc(hidden)]
#[macro_export]
macro_rules! __proto_events {
    ( $service:literal ; [ $($ev:ident)* ] ) => {
        /// All events of this protocol, for [`Client::next_event`].
        #[derive(Debug)]
        #[allow(clippy::enum_variant_names)]
        pub enum Event {
            $( $ev($ev), )*
        }

        impl Client {
            /// Pop the next decoded event pushed by the server.
            #[allow(unreachable_code, unused_variables)]
            pub fn next_event(&mut self) -> ::core::option::Option<Event> {
                loop {
                    let msg = self.inner.next_event_raw()?;
                    $(
                        if msg.selector == <$ev as $crate::EventMsg>::SELECTOR {
                            match $crate::postcard::from_bytes::<$ev>(&msg.payload) {
                                ::core::result::Result::Ok(e) => {
                                    return ::core::option::Option::Some(Event::$ev(e));
                                }
                                ::core::result::Result::Err(_) => continue,
                            }
                        }
                    )*
                }
            }
        }
    };
    ( $service:literal ; [ $($acc:ident)* ]
      $(#[$attr:meta])*
      event $name:ident { $($f:ident : $ft:ty),* $(,)? } ;
      $($rest:tt)*
    ) => {
        $(#[$attr])*
        #[derive(Debug, Clone, PartialEq, ::serde::Serialize, ::serde::Deserialize)]
        pub struct $name {
            $(pub $f: $ft,)*
        }

        impl $crate::EventMsg for $name {
            const SERVICE: &'static str = $service;
            const SELECTOR: u64 = $crate::hash::selector(
                $service,
                concat!("event.", stringify!($name)),
                stringify!({ $($f : $ft),* }),
            );
        }

        $crate::__proto_events! { $service ; [ $($acc)* $name ] $($rest)* }
    };
    ( $service:literal ; [ $($acc:ident)* ]
      $(#[$attr:meta])*
      fn $name:ident ( $($args:tt)* ) $(fds ( $($fd:ident),* $(,)? ))? $(-> $ret:ty)? ;
      $($rest:tt)*
    ) => {
        $crate::__proto_events! { $service ; [ $($acc)* ] $($rest)* }
    };
    ( $service:literal ; [ $($acc:ident)* ]
      $(#[$attr:meta])*
      notify $name:ident ( $($args:tt)* ) $(fds ( $($fd:ident),* $(,)? ))? ;
      $($rest:tt)*
    ) => {
        $crate::__proto_events! { $service ; [ $($acc)* ] $($rest)* }
    };
}
