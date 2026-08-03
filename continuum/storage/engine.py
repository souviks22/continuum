"""Storage engine: ties the WAL, the state machine, and snapshotting
together into the single-node durable store Phase 2's Raft layer sits on
top of.

The core correctness property this module owns: recovery must produce
the right state regardless of exactly where a crash landed during a
*previous* compaction. That's why every WAL record carries its own
logical index (see statemachine.Operation) instead of the index being
implied by physical position within whatever WAL file happens to exist
on disk -- replay compares each record's embedded index against the
snapshot's last_index and only applies what's newer. That comparison
stays correct whether or not a prior compact() actually got as far as
replacing the WAL file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from continuum.storage.snapshot import SnapshotStore
from continuum.storage.statemachine import Operation, StateMachine
from continuum.storage.wal import WriteAheadLog


class StorageEngine:
    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._snapshots = SnapshotStore(self.directory)
        self.state_machine = StateMachine()

        snapshot = self._snapshots.read()
        if snapshot is None:
            self._last_snapshot_index = -1  # -1: "nothing snapshotted yet"
        else:
            state, last_index = snapshot
            self.state_machine.load_state(state)
            self._last_snapshot_index = last_index

        self._wal_path = self.directory / "wal.log"
        self.wal = WriteAheadLog(self._wal_path)
        self._next_index = self._last_snapshot_index + 1
        self._replay_wal()

    def _replay_wal(self) -> None:
        for payload in self.wal.read_all():
            operation = Operation.decode(payload)
            if operation.index <= self._last_snapshot_index:
                continue  # already reflected in the loaded snapshot -- skip, don't double-apply
            self.state_machine.apply(operation)
            self._next_index = max(self._next_index, operation.index + 1)

    # -- writes -----------------------------------------------------------

    def set(self, key: str, value: str) -> int:
        return self._append(
            Operation(index=self._next_index, op="set", key=key, value=value)
        )

    def delete(self, key: str) -> int:
        return self._append(
            Operation(index=self._next_index, op="delete", key=key, value=None)
        )

    def _append(self, operation: Operation) -> int:
        self.wal.append(operation.encode())
        self.state_machine.apply(operation)
        self._next_index += 1
        return operation.index

    # -- reads -----------------------------------------------------------

    def get(self, key: str) -> Optional[str]:
        return self.state_machine.get(key)

    @property
    def last_index(self) -> int:
        return self._next_index - 1

    # -- compaction --------------------------------------------------------

    def compact(self) -> None:
        """Snapshot current state and discard the WAL entries it now
        makes redundant. Safe to crash at any point during this:

        1. Crash mid-snapshot-write: SnapshotStore's write-temp-then-
           rename means the old snapshot (or none) is still what's read
           back; the WAL is untouched. Recovery proceeds exactly as if
           compact() had never been called.
        2. Crash after the new snapshot lands but before the WAL is
           replaced: recovery loads the *new* snapshot (last_index = N)
           but replays a WAL that still has all the old entries,
           including ones with index <= N. Those are skipped by the
           index check in _replay_wal -- no double-apply, no data loss.
        3. Crash during WAL replacement (old file removed, new one not
           yet created, or created but still empty): WriteAheadLog's own
           constructor handles a missing file by creating a fresh one,
           and the new snapshot already covers everything that mattered
           up to last_index.

        None of these require detecting "did compaction fully finish" at
        recovery time -- replay's index comparison makes every one of
        them converge to the same correct state on its own.
        """
        last_index = self._next_index - 1
        if last_index <= self._last_snapshot_index:
            return  # nothing new since the last snapshot

        state = self.state_machine.snapshot_state()
        self._snapshots.write(state, last_index)
        self._last_snapshot_index = last_index

        self.wal.close()
        if self._wal_path.exists():
            self._wal_path.unlink()
        self.wal = WriteAheadLog(self._wal_path)

    def close(self) -> None:
        self.wal.close()
