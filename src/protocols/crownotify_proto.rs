crate::protocol! {
    /// Notification delivery and notification-center control
    /// (served by crownotify).
    pub mod crownotify: "crownotify" {
        /// Open the notification center panel.
        notify open_notification_center();
        /// Close the notification center panel.
        notify close_notification_center();
        /// Post a standard notification card.
        fn send_general_notification(
            app_name: String,
            summary: String,
            body: String,
            expire_timeout: u32,
            actions: Vec<String>,
        ) -> u32;
        /// Post an incoming-call card with answer/decline actions.
        fn send_call_notification(
            call_id: String,
            app_name: String,
            contact_avatar: String,
            contact_name: String,
            phone_number: String,
        ) -> u32;
        /// Post or update a now-playing card.
        fn send_music_notification(song_name: String, percentage: u8) -> u32;
        /// Post a chat message card.
        fn send_chat_notification(
            app_name: String,
            title: String,
            body: String,
            timestamp: String,
        ) -> u32;
        /// Dismiss a previously posted notification by its id.
        notify close_notification(id: u32);
        /// Emitted when a notification is closed (reason: 1 = expired,
        /// 2 = dismissed by user, 3 = closed via close_notification).
        event NotificationClosed { id: u32, reason: u8 };
        /// Emitted when the user invokes an action on a notification.
        event ActionInvoked { id: u32, action: String };
        /// Emitted when the notification center opens or closes.
        event CenterVisibility { open: bool };
    }
}
