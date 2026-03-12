from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from src.seed_data import populate_db


def _total_users(db_dir: Path, num_shards: int) -> int:
    total = 0
    for shard_index in range(num_shards):
        shard_path = db_dir / f"shard{shard_index}.db"
        with sqlite3.connect(shard_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
            total += int(row[0])
    return total


def test_populate_db_without_config(tmp_path: Path) -> None:
    db_dir = tmp_path / "db"
    distribution = populate_db(count=20, start_id=100, db_dir=str(db_dir), num_shards=3)

    assert sum(distribution.values()) == 20
    assert _total_users(db_dir=db_dir, num_shards=3) == 20


def test_populate_db_with_consistent_hashing_config(tmp_path: Path) -> None:
    db_dir = tmp_path / "db"
    config_path = tmp_path / "sharding.json"
    config_path.write_text(
        json.dumps(
            {
                "sharding": {
                    "strategy": "consistent_hashing",
                    "num_shards": 4,
                    "consistent_hashing": {
                        "virtual_nodes_per_server": 32,
                        "nodes": ["shard0", "shard1", "shard2"],
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    distribution = populate_db(
        count=50,
        start_id=1,
        db_dir=str(db_dir),
        config_path=str(config_path),
    )

    assert sum(distribution.values()) == 50
    assert _total_users(db_dir=db_dir, num_shards=4) == 50
