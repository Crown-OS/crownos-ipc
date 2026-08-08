crate::protocol! {
    /// Launcher / launchpad control (served by crownpad).
    ///
    /// Anything on the shell — a bar button, a compositor keybinding — can
    /// open or close the launchpad through this protocol.
    pub mod crownpad: "crownpad" {
        /// Show the launchpad.
        notify show();
        /// Hide the launchpad.
        notify hide();
        /// Toggle launchpad visibility.
        notify toggle();
        /// Emitted whenever launchpad visibility changes.
        event Visibility { visible: bool };
    }
}
