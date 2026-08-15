"""Durable term/vote state.

Raft's election safety proof depends on a specific durability
requirement: a server must persist currentTerm and votedFor to stable
storage *before* replying to a RequestVote RPC that depends on them.
Skip that, and a node that grants a vote, crashes, and restarts before
the vote reaches disk can grant a second, conflicting vote for the same
term after restart -- silently allowing two leaders to be elected in one
term, exactly the safety property Raft exists to prevent. This is why
persistence is a hard synchronous dependency of the vote-granting path
in node.py, not something that happens lazily afterward.

Uses the same write-temp-then-rename atomicity pattern as
storage.snapshot.SnapshotStore, reimplemented here rather than shared,
since term/vote persistence is called far more frequently (every term
change, every vote) on a much smaller, fixed-shape payload than a full
state snapshot -- worth factoring into a shared helper if a third caller
turns up, not worth the abstraction for two.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional


class RaftPersistentState:
    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._final_path = self.directory / "raft_state.json"
        self._tmp_path = self.directory / "raft_state.json.tmp"

        if self._final_path.exists():
            with open(self._final_path) as f:
                data = json.load(f)
            self.current_term: int = data["current_term"]
            self.voted_for: Optional[str] = data["voted_for"]
        else:
            self.current_term = 0
            self.voted_for = None
            self._persist()

    def _persist(self) -> None:
        payload = json.dumps(
            {"current_term": self.current_term, "voted_for": self.voted_for}
        )
        with open(self._tmp_path, "w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(self._tmp_path, self._final_path)

    def set_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        self.current_term = term
        self.voted_for = voted_for
        self._persist()
