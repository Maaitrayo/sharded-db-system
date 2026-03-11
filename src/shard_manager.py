from __future__ import annotations

import sqlite3
from pathlib import Path


class ShardManager:
    """Manages shard file locations, initialization, and connections."""

    def __init__(self, num_shards: int = 3, db_dir: str = "src/db") -> None:
        if num_shards <= 0:
            raise ValueError("num_shards must be greater than 0")
        self.num_shards = num_shards
        self.db_dir = Path(db_dir)

    def initialize_shards(self) -> None:
        """Create shard files and ensure schema exists in each shard."""
        self.db_dir.mkdir(parents=True, exist_ok=True)
        for shard_index in range(self.num_shards):
            with self.get_connection(shard_index) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    )
                    """
                )
                conn.commit()

    def get_shard_path(self, shard_index: int) -> Path:
        """Return the SQLite file path for a shard index."""
        self._validate_shard_index(shard_index)
        return self.db_dir / f"shard{shard_index}.db"

    def get_connection(self, shard_index: int) -> sqlite3.Connection:
        """Open a sqlite3 connection to a specific shard."""
        shard_path = self.get_shard_path(shard_index)
        return sqlite3.connect(shard_path)

    def _validate_shard_index(self, shard_index: int) -> None:
        if not 0 <= shard_index < self.num_shards:
            raise ValueError(
                f"Invalid shard_index={shard_index}. "
                f"Expected range: 0 to {self.num_shards - 1}."
            )
