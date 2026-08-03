"""Snapshot storage: atomic write-then-rename so a crash mid-write can
never leave a corrupt or partial snapshot on disk -- a reader only ever
sees a snapshot that was fully written, or the previous, still-valid
one, never something in between.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional


class SnapshotStore:
    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._final_path = self.directory / "snapshot.json"
        self._tmp_path = self.directory / "snapshot.json.tmp"

    def write(self, state: dict[str, str], last_index: int) -> None:
        payload = json.dumps({"last_index": last_index, "state": state})
        with open(self._tmp_path, "w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        # os.replace is atomic on POSIX: a reader sees either the old
        # file or the fully-written new one, never a half-written result
        # of this write landing at the final path.
        os.replace(self._tmp_path, self._final_path)

    def read(self) -> Optional[tuple[dict[str, str], int]]:
        if not self._final_path.exists():
            return None
        with open(self._final_path) as f:
            data = json.load(f)
        return data["state"], data["last_index"]