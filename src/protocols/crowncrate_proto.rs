crate::protocol! {
    /// Phone-companion control surface (served by crowncrate-linux).
    ///
    /// crownotify calls back into crowncrate when the user acts on a call
    /// card; other shell pieces can drive phone-originated actions.
    pub mod crowncrate: "crowncrate" {
        /// Answer the incoming call shown in a call notification.
        fn pickup_call(call_id: String);
        /// Decline the incoming call shown in a call notification.
        fn decline_call(call_id: String);
        /// Push clipboard contents to the connected phone.
        notify set_clipboard(contents: String);
        /// Set the phone's media volume (0-100).
        notify set_volume(value: u8);
        /// Toggle media playback on the phone.
        notify media_toggle();
        /// Ask the phone to open a target (url, app id, ...).
        notify open(target: String);
        /// Trigger an OTP re-sync from the phone.
        notify otp_sync();
        /// Emitted when a phone connects or disconnects.
        event DeviceConnected { name: String, connected: bool };
        /// Emitted when the phone pushes new clipboard contents.
        event ClipboardReceived { contents: String };
    }
}
