use crate::executors_public::*;
pub use executor::Executor;
pub use executor::WorkerMsg;
pub use timer::TimerMsg;
use std::time::Duration;
use executor::NewModReq;
use crossbeam_channel::{Sender, Receiver, unbounded};
use std::thread::spawn;
use std::sync::{Arc, Mutex};
use std::ops::{DerefMut};
use timer::{Timer, TickRequest};

impl<T: Send> ModuleRef<T> {
    pub fn _send<M: Message>(&self, _msg: M)
        where
            T: Handler<M>,
    {
        let f = move |module: &mut T| {
            module.handle(_msg.clone());
        };
        if let Ok(_) = self.mod_tx.send(Box::new(f)) {
            self.ex_tx.send(WorkerMsg::ExecuteModule(self.id)).ok();
        }
    }

    pub fn _clone(&self) -> Self {
        let mut count = Arc::new(Mutex::new(0));
        if let Ok(mut guard) = self.ref_count.lock() {
            let n = guard.deref_mut();
            *n += 1;
            count = self.ref_count.clone();
        }
        Self {
            id: self.id.clone(),
            mod_tx: self.mod_tx.clone(),
            ex_tx: self.ex_tx.clone(),
            ref_count: count,
        }
    }
}

impl<T: Send> Drop for ModuleRef<T> {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.ref_count.lock() {
            if *guard <= 1 {
                self.ex_tx.send(WorkerMsg::RemoveModule(self.id)).ok();
            } else {
                *guard -= 1;
            }
        }
    }
}

impl System {
    pub fn _request_tick<T: Send + Handler<Tick>>(&mut self, _requester: &ModuleRef<T>, _delay: Duration) {
        let req = TickRequest {
            module_ref: _requester.clone(),
            next_tick: Duration::new(0, 0),
            interval: _delay,
        };
        self.timer_tx.send(TimerMsg::RequestTick(Box::new(req))).ok();
    }

    pub fn _register_module<T: Send + 'static>(&mut self, _module: T) -> ModuleRef<T> {
        let (id_tx, id_rx): (Sender<u128>, Receiver<u128>) = unbounded();
        let (mod_tx, mod_rx): (Sender<Box<dyn Fn(&mut T) + Send>>, Receiver<Box<dyn Fn(&mut T) + Send>>) = unbounded();
        let mut id: u128 = 0;
        let req = NewModReq {
            id_tx,
            mod_rx,
            module: Box::new(_module),
        };
        if let Ok(_) = self.ex_tx.send(WorkerMsg::NewModule(Box::new(req))) {
            if let Ok(new_id) = id_rx.recv() {
                id = new_id;
            }
        }
        ModuleRef {
            id,
            mod_tx,
            ex_tx: self.ex_tx.clone(),
            ref_count: Arc::new(Mutex::new(1)),
        }
    }

    pub fn _new() -> Self {
        let (ex_tx, ex_rx): (Sender<WorkerMsg>, Receiver<WorkerMsg>) = unbounded();
        let (timer_tx, timer_rx): (Sender<TimerMsg>, Receiver<TimerMsg>) = unbounded();

        let mut executor = Executor::new();
        let executor_thread = spawn(move || {
            executor.executor_thread_fuction(ex_rx);
        });

        let mut timer = Timer::new();
        let timer_thread = spawn(move || {
            timer.timer_thread_function(timer_rx);
        });

        Self {
            ex_tx,
            timer_tx,
            executor_thread: Some(executor_thread),
            timer_thread: Some(timer_thread),
        }
    }
}

impl Drop for System {
    fn drop(&mut self) {
        if let Some(executor_handle) = self.executor_thread.take() {
            if let Ok(_) = self.ex_tx.send(WorkerMsg::Done) {
                executor_handle.join().ok();
            }
        }

        if let Some(timer_handle) = self.timer_thread.take() {
            if let Ok(_) = self.timer_tx.send(TimerMsg::Done) {
                timer_handle.join().ok();
            }
        }
    }
}

mod executor {
    use std::collections::HashMap;
    use crossbeam_channel::{Receiver, Sender};

    pub trait ModuleTrait: Send {
        fn get_id_tx(&self) -> Sender<u128>;
        fn execute(&mut self);
    }

    pub struct NewModReq<T: Send + 'static + ?Sized> {
        pub id_tx: Sender<u128>,
        pub mod_rx: Receiver<Box<dyn Fn(&mut T) + Send>>,
        pub module: Box<T>,
    }

    impl<T: Send + 'static + ?Sized> ModuleTrait for NewModReq<T> {
        fn get_id_tx(&self) -> Sender<u128> {
            self.id_tx.clone()
        }

        fn execute(&mut self) {
            if let Ok(f) = self.mod_rx.recv() {
                (*f)(&mut *self.module);
            }
        }
    }

    pub enum WorkerMsg {
        NewModule(Box<dyn ModuleTrait>),
        RemoveModule(u128),
        ExecuteModule(u128),
        Done,
    }

    pub struct Executor {
        modules: HashMap<u128, Box<dyn ModuleTrait>>,
        next_id: u128,
    }

    impl Executor {
        pub fn new() -> Self {
            Self {
                modules: HashMap::new(),
                next_id: 0,
            }
        }

        pub fn executor_thread_fuction(&mut self, rx: Receiver<WorkerMsg>) {
            while let Ok(msg) = rx.recv() {
                match msg {
                    WorkerMsg::NewModule(module) => self.new_module(module),
                    WorkerMsg::RemoveModule(id) => self.remove_module(id),
                    WorkerMsg::ExecuteModule(id) => self.execute_module(id),
                    WorkerMsg::Done => break
                }
            }
        }

        fn new_module(&mut self, module: Box<dyn ModuleTrait>) {
            let id = self.next_id;
            self.next_id += 1;
            let id_tx = module.get_id_tx();
            self.modules.insert(id, module);
            id_tx.send(id).ok();
        }

        fn remove_module(&mut self, id: u128) {
            self.modules.remove(&id);
        }

        fn execute_module(&mut self, id: u128) {
            if let Some(module) = self.modules.get_mut(&id) {
                module.execute();
            }
        }
    }
}

mod timer {
    use crate::{Handler, Tick, ModuleRef};
    use std::time::{Duration, Instant};
    use crossbeam_channel::Receiver;

    pub trait TickTrait: Send {
        fn start_timer(&mut self, time: Duration);
        fn tick(&mut self, time: Duration);
    }

    pub struct TickRequest<T: Send + Handler<Tick> + 'static> {
        pub module_ref: ModuleRef<T>,
        pub next_tick: Duration,
        pub interval: Duration,
    }

    impl<T: Send + Handler<Tick>> TickTrait for TickRequest<T> {
        fn start_timer(&mut self, time: Duration) {
            self.next_tick = time;
        }

        fn tick(&mut self, time: Duration) {
            while self.next_tick <= time {
                self.module_ref.send(Tick {});
                self.next_tick += self.interval;
            }
        }
    }

    pub enum TimerMsg {
        RequestTick(Box<dyn TickTrait>),
        Done,
    }

    pub struct Timer {
        requests: Vec<Box<dyn TickTrait>>,
        start_time: Instant,
    }

    impl Timer {
        pub fn new() -> Self {
            Self {
                requests: Vec::new(),
                start_time: Instant::now(),
            }
        }

        pub fn timer_thread_function(&mut self, rx: Receiver<TimerMsg>) {
            while let Ok(msg) = rx.recv() {
                match msg {
                    TimerMsg::RequestTick(req) => self.add_request(req),
                    TimerMsg::Done => break
                }
            }
        }

        fn add_request(&mut self, mut req: Box<dyn TickTrait>) {
            req.start_timer(self.current_time());
            req.tick(self.current_time());
            self.requests.push(req);
        }

        fn current_time(&self) -> Duration {
            Instant::now().duration_since(self.start_time)
        }
    }
}
