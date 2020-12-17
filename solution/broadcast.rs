use crate::{
    PlainSender,
    StubbornBroadcast,
    ReliableBroadcast,
    StableStorage,
    StubbornBroadcastModule,
    SystemAcknowledgmentMessage,
    SystemBroadcastMessage,
    SystemMessageContent,
    SystemMessageHeader,
    SystemMessage,
    ModuleRef,
};
use std::collections::{HashSet, HashMap};
use uuid::Uuid;
use crate::PlainSenderMessage::{Broadcast, Acknowledge};
use std::convert::TryInto;

const DELIVERED: &str = "DELIVERED";
const PENDING: &str = "PENDING";
const NEXT_ID: &str = "NEXT_ID";

struct StubbornBestEffortBroadcast {
    link: Box<dyn PlainSender>,
    processes: HashSet<Uuid>,
    pending: HashSet<(Uuid, SystemMessageHeader)>,
    messages: HashMap<SystemMessageHeader, SystemBroadcastMessage>,
    message_counts: HashMap<SystemMessageHeader, u32>,
}

impl StubbornBestEffortBroadcast {
    fn new(link: Box<dyn PlainSender>, processes: HashSet<Uuid>) -> Self {
        StubbornBestEffortBroadcast {
            link,
            processes,
            pending: HashSet::new(),
            messages: HashMap::new(),
            message_counts: HashMap::new(),
        }
    }
}

impl StubbornBroadcast for StubbornBestEffortBroadcast {
    fn broadcast(&mut self, msg: SystemBroadcastMessage) {
        self.messages.insert(msg.message.header.clone(), msg.clone());
        for p in &self.processes {
            let count = self.message_counts.entry(msg.message.header.clone()).or_insert(0);
            *count += 1;
            self.pending.insert((*p, msg.message.header.clone()));
            self.link.send_to(p, Broadcast(msg.clone()));
        }
    }

    fn receive_acknowledgment(&mut self, proc: Uuid, msg: SystemMessageHeader) {
        if self.pending.contains(&(proc, msg)) {
            self.pending.remove(&(proc, msg));
            let count = self.message_counts.entry(msg.clone()).or_insert(1);
            *count -= 1;
            if *count == 0 {
                self.messages.remove(&msg);
            }
        }
    }

    fn send_acknowledgment(&mut self, proc: Uuid, msg: SystemAcknowledgmentMessage) {
        self.link.send_to(&proc, Acknowledge(msg));
    }

    fn tick(&mut self) {
        for (process, header) in &self.pending {
            if let Some(msg) = self.messages.get(header) {
                self.link.send_to(process, Broadcast(msg.clone()));
            }
        }
    }
}

pub fn build_stubborn_best_effort_broadcast(
    _link: Box<dyn PlainSender>,
    _processes: HashSet<Uuid>,
) -> Box<dyn StubbornBroadcast> {
    Box::new(StubbornBestEffortBroadcast::new(_link, _processes))
}

struct LoggedUniformReliableBroadcast {
    sbeb: ModuleRef<StubbornBroadcastModule>,
    storage: Box<dyn StableStorage>,
    id: Uuid,
    processes_number: usize,
    delivered_callback: Box<dyn Fn(SystemMessage) + Send>,
    pending: HashSet<SystemMessageHeader>,
    // Messages that are pending and not yet delivered
    ack: HashMap<SystemMessageHeader, HashSet<Uuid>>,
    next_id: u128,
}

impl LoggedUniformReliableBroadcast {
    pub fn new(
        sbeb: ModuleRef<StubbornBroadcastModule>,
        mut storage: Box<dyn StableStorage>,
        id: Uuid,
        processes_number: usize,
        delivered_callback: Box<dyn Fn(SystemMessage) + Send>,
    ) -> Self {
        let pending = Self::deserialize_set(&storage.get(PENDING).unwrap_or(Vec::new()));
        if pending.is_empty() {
            storage.put(PENDING, &Self::serialize_set(&pending)).ok();
        }
        let ack = HashMap::new();
        let next_id: u128 = storage.get(NEXT_ID).map(|v| (&v[..16]).try_into()
            .unwrap_or([0; 16])).map_or(0, u128::from_be_bytes);
        if next_id == 0 {
            storage.put(NEXT_ID, &next_id.to_be_bytes()).ok();
        }

        // We only need to iterate over messages that are not yet delivered, so we can use `pending`
        for hdr in &pending {
            if let Some(content) = storage.get(&*Self::header_key(PENDING, hdr)) {
                let msg = SystemBroadcastMessage {
                    forwarder_id: id,
                    message: SystemMessage {
                        header: hdr.clone(),
                        data: SystemMessageContent {
                            msg: content
                        },
                    },
                };
                sbeb.send(msg);
            }
        }

        LoggedUniformReliableBroadcast {
            sbeb,
            storage,
            id,
            processes_number,
            delivered_callback,
            pending,
            ack,
            next_id,
        }
    }

    fn add_to_pending(&mut self, hdr: &SystemMessageHeader, msg: &SystemMessageContent) {
        self.pending.insert(hdr.clone());
        self.storage.put(&*Self::header_key(PENDING, hdr), &msg.msg).ok();
        self.storage.put(PENDING, &Self::serialize_set(&self.pending)).ok();
    }

    fn header_key(set: &str, hdr: &SystemMessageHeader) -> String {
        let src = hdr.message_source_id.to_simple().encode_lower(&mut Uuid::encode_buffer()).to_owned();
        let msg = hdr.message_id.to_simple().encode_lower(&mut Uuid::encode_buffer()).to_owned();
        let mut key = set.to_string();
        key.push_str("_");
        key.push_str(&src);
        key.push_str(&msg);
        key
    }

    fn serialize_header(hdr: &SystemMessageHeader) -> Vec<u8> {
        let src_vec = hdr.message_source_id.as_bytes();
        let msg_vec = hdr.message_id.as_bytes();
        [src_vec.to_vec(), msg_vec.to_vec()].concat()
    }

    fn deserialize_header(v: &[u8]) -> Option<SystemMessageHeader> {
        let src_id = Uuid::from_slice(&v[0..16]);
        let msg_id = Uuid::from_slice(&v[16..32]);

        if let Err(_) = src_id.clone() {
            return None;
        }
        if let Err(_) = msg_id.clone() {
            return None;
        }

        let hdr = SystemMessageHeader {
            message_source_id: src_id.unwrap_or(Uuid::from_u128(0)),
            message_id: msg_id.unwrap_or(Uuid::from_u128(0)),
        };
        Some(hdr)
    }

    fn serialize_set(set: &HashSet<SystemMessageHeader>) -> Vec<u8> {
        set
            .into_iter()
            .flat_map(Self::serialize_header)
            .collect()
    }

    fn deserialize_set(bytes: &Vec<u8>) -> HashSet<SystemMessageHeader> {
        bytes
            .chunks_exact(32)
            .filter_map(Self::deserialize_header)
            .collect()
    }
}

impl ReliableBroadcast for LoggedUniformReliableBroadcast {
    fn broadcast(&mut self, msg: SystemMessageContent) {
        let message_id = self.next_id;
        self.next_id += 1;
        self.storage.put(NEXT_ID, &self.next_id.to_be_bytes()).ok();
        let message = SystemBroadcastMessage {
            forwarder_id: self.id,
            message: SystemMessage {
                header: SystemMessageHeader {
                    message_source_id: self.id,
                    message_id: Uuid::from_u128(message_id),
                },
                data: msg,
            },
        };
        self.add_to_pending(&message.message.header, &message.message.data);

        self.sbeb.send(message);
    }

    fn deliver_message(&mut self, msg: SystemBroadcastMessage) {
        let hdr = msg.message.header;
        let forwarder = msg.forwarder_id;

        // We have to check in stable storage, because once message is delivered it's no longer in `pending`
        if let None = self.storage.get(&*Self::header_key(PENDING, &hdr)) {
            self.add_to_pending(&hdr, &msg.message.data);
            let new_msg = SystemBroadcastMessage {
                forwarder_id: self.id,
                message: msg.message.clone(),
            };
            self.sbeb.send(new_msg);
        }

        let cur_ack = self.ack.entry(hdr).or_insert(HashSet::new());
        if !cur_ack.contains(&forwarder) {
            cur_ack.insert(forwarder);
            let cur_ack_len = cur_ack.len();
            {
                let _ = cur_ack;
            }

            let delivered = self.storage.get(&*Self::header_key(DELIVERED, &hdr)).is_some();
            if delivered {
                self.ack.remove(&hdr);
            }
            if cur_ack_len > self.processes_number / 2 && !delivered {
                (*self.delivered_callback)(msg.message);
                self.storage.put(&*Self::header_key(DELIVERED, &hdr), &[]).ok();
                self.pending.remove(&hdr);
                self.storage.put(PENDING, &Self::serialize_set(&self.pending)).ok();
                self.ack.remove(&hdr);
            }
        }
        self.sbeb.send((forwarder, SystemAcknowledgmentMessage { proc: self.id, hdr }))
    }

    fn receive_acknowledgment(&mut self, msg: SystemAcknowledgmentMessage) {
        let SystemAcknowledgmentMessage { proc, hdr } = msg;
        self.sbeb.send((proc, hdr));
    }
}

pub fn build_logged_uniform_reliable_broadcast(
    _sbeb: ModuleRef<StubbornBroadcastModule>,
    _storage: Box<dyn StableStorage>,
    _id: Uuid,
    _processes_number: usize,
    _delivered_callback: Box<dyn Fn(SystemMessage) + Send>,
) -> Box<dyn ReliableBroadcast> {
    let rb = LoggedUniformReliableBroadcast::new(
        _sbeb,
        _storage,
        _id,
        _processes_number,
        _delivered_callback,
    );
    Box::new(rb)
}
