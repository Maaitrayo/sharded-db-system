from __future__ import annotations

import sqlite3
from pathlib import Path

from src.db_init import initialize_databases


def test_initialize_databases_creates_all_shards_and_users_table(tmp_path: Path) -> None:
    db_dir = tmp_path / "db"
    initialize_databases(num_shards=3, db_dir=str(db_dir))

    for shard_index in range(3):
        shard_path = db_dir / f"shard{shard_index}.db"
        assert shard_path.exists()

        with sqlite3.connect(shard_path) as conn:
            row = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name = 'users'
                """
            ).fetchone()
            assert row is not None
            assert row[0] == "users"
