crate::protocol! {
    /// App launching through the compositor (served by crownpositor).
    ///
    /// Launching via the compositor instead of a bare `exec` lets it mint an
    /// xdg-activation token first, so the new window gets focus and lands on
    /// the workspace the launch originated from.
    pub mod compositor: "crownpositor" {
        /// Mint an xdg-activation token for `app_id`.
        fn activation_token(app_id: String) -> String;
        /// Spawn `exec` with `WAYLAND_DISPLAY` and (when `token` is set)
        /// `XDG_ACTIVATION_TOKEN` in its environment.
        notify launch(exec: String, app_id: String, token: Option<String>);
        /// Emitted when a launched app maps its first window.
        event Launched { app_id: String };
    }
}
