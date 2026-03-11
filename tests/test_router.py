from __future__ import annotations

from pathlib import Path

import pytest

from src.router import ShardRouter
from src.shard_manager import ShardManager


def test_shard_index_is_deterministic(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=3, db_dir=str(tmp_path / "db"))
    router = ShardRouter(shard_manager=manager)

    user_id = 8472831
    first = router.get_shard_index(user_id)
    second = router.get_shard_index(user_id)

    assert first == second
    assert 0 <= first < manager.num_shards


def test_create_and_get_user(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=3, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    router = ShardRouter(shard_manager=manager)

    router.create_user(user_id=25, name="Bob")
    user = router.get_user(user_id=25)

    assert user == {"user_id": 25, "name": "Bob"}


def test_create_user_rejects_invalid_inputs(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=3, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    router = ShardRouter(shard_manager=manager)

    with pytest.raises(ValueError):
        router.create_user(user_id=-1, name="Alice")

    with pytest.raises(ValueError):
        router.create_user(user_id=1, name="   ")


def test_get_user_rejects_negative_user_id(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=3, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    router = ShardRouter(shard_manager=manager)

    with pytest.raises(ValueError):
        router.get_user(user_id=-10)
