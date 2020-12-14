mod domain;
mod broadcast;

pub use crate::broadcast_public::*;
pub use crate::executors_public::*;
pub use crate::stable_storage_public::*;
pub use crate::system_setup_public::*;
pub use domain::*;

pub mod broadcast_public {
    use crate::executors_public::ModuleRef;
    use crate::{PlainSenderMessage, StableStorage, StubbornBroadcastModule, SystemAcknowledgmentMessage, SystemBroadcastMessage, SystemMessageContent, SystemMessageHeader, SystemMessage};
    use crate::broadcast;
    use std::collections::HashSet;
    use uuid::Uuid;

    pub trait PlainSender: Send + Sync {
        fn send_to(&self, uuid: &Uuid, msg: PlainSenderMessage);
    }

    pub trait ReliableBroadcast: Send {
        fn broadcast(&mut self, msg: SystemMessageContent);

        fn deliver_message(&mut self, msg: SystemBroadcastMessage);

        fn receive_acknowledgment(&mut self, msg: SystemAcknowledgmentMessage);
    }

    pub fn build_reliable_broadcast(
        _sbeb: ModuleRef<StubbornBroadcastModule>,
        _storage: Box<dyn StableStorage>,
        _id: Uuid,
        _processes_number: usize,
        _delivered_callback: Box<dyn Fn(SystemMessage) + Send>,
    ) -> Box<dyn ReliableBroadcast> {
        broadcast::build_logged_uniform_reliable_broadcast(_sbeb, _storage, _id, _processes_number, _delivered_callback)
    }

    pub trait StubbornBroadcast: Send {
        fn broadcast(&mut self, msg: SystemBroadcastMessage);

        fn receive_acknowledgment(&mut self, proc: Uuid, msg: SystemMessageHeader);

        fn send_acknowledgment(&mut self, proc: Uuid, msg: SystemAcknowledgmentMessage);

        fn tick(&mut self);
    }

    pub fn build_stubborn_broadcast(
        _link: Box<dyn PlainSender>,
        _processes: HashSet<Uuid>,
    ) -> Box<dyn StubbornBroadcast> {
        broadcast::build_stubborn_best_effort_broadcast(_link, _processes)
    }
}

pub mod stable_storage_public {
    use std::path::PathBuf;

    pub trait StableStorage: Send {
        fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

        fn get(&self, key: &str) -> Option<Vec<u8>>;
    }

    pub fn build_stable_storage(_root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
        unimplemented!()
    }
}

pub mod executors_public {
    use std::fmt;
    use std::time::Duration;

    pub trait Message: fmt::Debug + Clone + Send + 'static {}
    impl<T: fmt::Debug + Clone + Send + 'static> Message for T {}

    pub trait Handler<M: Message>
    where
        M: Message,
    {
        fn handle(&mut self, msg: M);
    }

    #[derive(Debug, Clone)]
    pub struct Tick {}

    pub struct System {}

    impl System {
        pub fn request_tick<T: Handler<Tick>>(&mut self, _requester: &ModuleRef<T>, _delay: Duration) {
            unimplemented!()
        }

        pub fn register_module<T: Send + 'static>(&mut self, _module: T) -> ModuleRef<T> {
            unimplemented!()
        }

        pub fn new() -> Self {
            unimplemented!()
        }
    }

    pub struct ModuleRef<T: 'static> {
        // dummy marker to satisfy compiler
        pub(crate) mod_internal: std::marker::PhantomData<T>,
    }

    impl<T> ModuleRef<T> {
        pub fn send<M: Message>(&self, _msg: M)
        where
            T: Handler<M>,
        {
            unimplemented!()
        }
    }

    impl<T> fmt::Debug for ModuleRef<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
            f.write_str("<ModuleRef>")
        }
    }

    impl<T> Clone for ModuleRef<T> {
        fn clone(&self) -> Self {
            unimplemented!()
        }
    }
}

pub mod system_setup_public {
    use crate::{Configuration, ModuleRef, ReliableBroadcast, StubbornBroadcast, System};

    pub fn setup_system(
        _system: &mut System,
        _config: Configuration,
    ) -> ModuleRef<ReliableBroadcastModule> {
        unimplemented!()
    }

    pub struct ReliableBroadcastModule {
        pub(crate) reliable_broadcast: Box<dyn ReliableBroadcast>,
    }

    pub struct StubbornBroadcastModule {
        pub(crate) stubborn_broadcast: Box<dyn StubbornBroadcast>,
    }
}
