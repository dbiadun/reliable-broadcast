use crate::{PlainSender, StableStorage};
use std::collections::HashSet;
use std::time::Duration;
use uuid::Uuid;

pub struct Configuration {
    pub self_process_identifier: Uuid,
    pub processes: HashSet<Uuid>,
    pub stable_storage: Box<dyn StableStorage>,
    pub sender: Box<dyn PlainSender>,
    pub retransmission_delay: Duration,
    pub delivered_callback: Box<dyn Fn(SystemMessage) + Send>,
}

/// the message type to be delivered to upper layers
#[derive(Debug, Clone)]
pub struct SystemMessage {
    pub header: SystemMessageHeader,
    pub data: SystemMessageContent,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct SystemMessageHeader {
    /// identifier of process which issued this message
    pub message_source_id: Uuid,
    /// message identifier, for uniqueness within the process
    pub message_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct SystemMessageContent {
    pub msg: Vec<u8>,
}

/// Acknowledgment response that a message was received
#[derive(Debug, Clone)]
pub struct SystemAcknowledgmentMessage {
    /// process which acknowledges
    pub proc: Uuid,
    /// message being acknowledged
    pub hdr: SystemMessageHeader,
}

/// Single broadcast message send to a single process
#[derive(Debug, Clone)]
pub struct SystemBroadcastMessage {
    /// identifier of the forwarder
    pub forwarder_id: Uuid,
    /// the message itself
    pub message: SystemMessage,
}

/// Sender can send two types of messages
#[derive(Debug, Clone)]
pub enum PlainSenderMessage {
    Acknowledge(SystemAcknowledgmentMessage),
    Broadcast(SystemBroadcastMessage),
}

pub mod system_util {
    use crate::{
        Handler, ReliableBroadcast, ReliableBroadcastModule, StubbornBroadcast,
        StubbornBroadcastModule, SystemAcknowledgmentMessage, SystemBroadcastMessage,
        SystemMessageContent, SystemMessageHeader, Tick,
    };
    use uuid::Uuid;

    impl ReliableBroadcastModule {
        pub fn new(reliable_broadcast: Box<dyn ReliableBroadcast>) -> Self {
            Self { reliable_broadcast }
        }
    }

    impl Handler<SystemMessageContent> for ReliableBroadcastModule {
        fn handle(&mut self, msg: SystemMessageContent) {
            self.reliable_broadcast.broadcast(msg);
        }
    }

    impl Handler<SystemBroadcastMessage> for ReliableBroadcastModule {
        fn handle(&mut self, msg: SystemBroadcastMessage) {
            self.reliable_broadcast.deliver_message(msg);
        }
    }

    impl Handler<SystemAcknowledgmentMessage> for ReliableBroadcastModule {
        fn handle(&mut self, msg: SystemAcknowledgmentMessage) {
            self.reliable_broadcast.receive_acknowledgment(msg);
        }
    }

    impl StubbornBroadcastModule {
        pub fn new(stubborn_broadcast: Box<dyn StubbornBroadcast>) -> Self {
            Self { stubborn_broadcast }
        }
    }

    impl Handler<SystemBroadcastMessage> for StubbornBroadcastModule {
        fn handle(&mut self, msg: SystemBroadcastMessage) {
            self.stubborn_broadcast.broadcast(msg);
        }
    }

    impl Handler<(Uuid, SystemAcknowledgmentMessage)> for StubbornBroadcastModule {
        fn handle(&mut self, msg: (Uuid, SystemAcknowledgmentMessage)) {
            self.stubborn_broadcast.send_acknowledgment(msg.0, msg.1);
        }
    }

    impl Handler<(Uuid, SystemMessageHeader)> for StubbornBroadcastModule {
        fn handle(&mut self, msg: (Uuid, SystemMessageHeader)) {
            self.stubborn_broadcast.receive_acknowledgment(msg.0, msg.1);
        }
    }

    impl Handler<Tick> for StubbornBroadcastModule {
        fn handle(&mut self, _msg: Tick) {
            self.stubborn_broadcast.tick();
        }
    }
}
