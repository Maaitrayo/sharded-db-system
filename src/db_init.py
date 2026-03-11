from __future__ import annotations

from src.shard_manager import ShardManager


def initialize_databases(num_shards: int = 3, db_dir: str = "src/db") -> None:
    manager = ShardManager(num_shards=num_shards, db_dir=db_dir)
    manager.initialize_shards()


if __name__ == "__main__":
    initialize_databases()
