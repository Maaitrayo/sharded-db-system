from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.shard_manager import ShardManager


def test_initialize_shards_creates_db_files_and_schema(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=3, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()

    for shard_index in range(3):
        shard_path = manager.get_shard_path(shard_index)
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


def test_invalid_shard_index_raises_value_error(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=3, db_dir=str(tmp_path / "db"))

    with pytest.raises(ValueError):
        manager.get_shard_path(-1)

    with pytest.raises(ValueError):
        manager.get_shard_path(3)
