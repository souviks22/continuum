from continuum.raft.persistent_state import RaftPersistentState


def test_fresh_state_starts_at_term_zero_no_vote(tmp_path):
    state = RaftPersistentState(tmp_path)
    assert state.current_term == 0
    assert state.voted_for is None


def test_set_term_and_vote_updates_in_memory(tmp_path):
    state = RaftPersistentState(tmp_path)
    state.set_term_and_vote(5, "node-b")
    assert state.current_term == 5
    assert state.voted_for == "node-b"


def test_reopening_recovers_persisted_term_and_vote(tmp_path):
    RaftPersistentState(tmp_path).set_term_and_vote(3, "node-a")
    reopened = RaftPersistentState(tmp_path)
    assert reopened.current_term == 3
    assert reopened.voted_for == "node-a"


def test_vote_can_be_none_after_a_term_advance(tmp_path):
    state = RaftPersistentState(tmp_path)
    state.set_term_and_vote(1, "node-a")
    state.set_term_and_vote(2, None)
    reopened = RaftPersistentState(tmp_path)
    assert reopened.current_term == 2
    assert reopened.voted_for is None


def test_leftover_tmp_file_from_a_simulated_crash_does_not_affect_recovery(tmp_path):
    state = RaftPersistentState(tmp_path)
    state.set_term_and_vote(1, "node-a")
    # Simulate a crash partway through a second persist: tmp file has
    # garbage, but os.replace never ran.
    with open(state._tmp_path, "w") as f:
        f.write("{not-valid-json")
    reopened = RaftPersistentState(tmp_path)
    assert reopened.current_term == 1
    assert reopened.voted_for == "node-a"
