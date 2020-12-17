use crate::{System, Configuration, ReliableBroadcastModule, ModuleRef, build_stubborn_broadcast, StubbornBroadcastModule, build_reliable_broadcast};

pub fn _setup_system(
    _system: &mut System,
    _config: Configuration,
) -> ModuleRef<ReliableBroadcastModule> {
    let processes_number = _config.processes.len();

    let sb = build_stubborn_broadcast(
        _config.sender,
        _config.processes,
    );
    let sb_module = StubbornBroadcastModule { stubborn_broadcast: sb};
    let sb_ref = _system.register_module(sb_module);
    _system.request_tick(&sb_ref, _config.retransmission_delay);

    let rb = build_reliable_broadcast(
        sb_ref,
        _config.stable_storage,
        _config.self_process_identifier,
        processes_number,
        _config.delivered_callback,
    );
    let rb_module = ReliableBroadcastModule { reliable_broadcast: rb};
    let rb_ref = _system.register_module(rb_module);

    rb_ref
}