"""Shared atomic JSON file: write-temp-then-rename so a crash mid-write
never leaves a corrupt or partial file on disk -- a reader always sees
either the previous fully-written version or the new one, never
something in between.

Factored out here because a third call site now needs exactly this
pattern (state snapshots and Raft term/vote persistence already had
their own copies; Raft log-compaction metadata, added in Step 2.3, is
the third) -- worth sharing the mechanism now rather than copying the
same handful of lines a third time. `SnapshotStore` and
`RaftPersistentState` are deliberately left as their existing, already-
tested implementations rather than retrofitted onto this to avoid
touching stable code for its own sake; only the new caller uses it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional


class AtomicJsonFile:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._tmp_path = self.path.with_name(self.path.name + ".tmp")

    def write(self, data: dict[str, Any]) -> None:
        payload = json.dumps(data)
        with open(self._tmp_path, "w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(self._tmp_path, self.path)

    def read(self) -> Optional[dict[str, Any]]:
        if not self.path.exists():
            return None
        with open(self.path) as f:
            return json.load(f)
