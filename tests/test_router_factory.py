from __future__ import annotations

from pathlib import Path

import pytest

from src.config import ShardingConfig
from src.router import ConsistentHashRouter, ModuloShardRouter
from src.router_factory import RouterFactory
from src.shard_manager import ShardManager


def test_factory_builds_modulo_router(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=3, db_dir=str(tmp_path / "db"))
    config = ShardingConfig.from_dict({"sharding": {"strategy": "modulo"}})

    router = RouterFactory.build(config=config, shard_manager=manager)

    assert isinstance(router, ModuloShardRouter)


def test_factory_builds_consistent_hash_router(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=3, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    config = ShardingConfig.from_dict(
        {
            "sharding": {
                "strategy": "consistent_hashing",
                "consistent_hashing": {"nodes": ["shard0", "shard1", "shard2"]},
            }
        }
    )

    router = RouterFactory.build(config=config, shard_manager=manager)

    assert isinstance(router, ConsistentHashRouter)
    router.create_user(user_id=99, name="Carol")
    assert router.get_user(user_id=99) == {"user_id": 99, "name": "Carol"}


def test_consistent_hash_router_rejects_node_count_mismatch(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=2, db_dir=str(tmp_path / "db"))
    config = ShardingConfig.from_dict(
        {
            "sharding": {
                "strategy": "consistent_hashing",
                "consistent_hashing": {"nodes": ["shard0", "shard1", "shard2"]},
            }
        }
    )

    with pytest.raises(ValueError):
        RouterFactory.build(config=config, shard_manager=manager)
