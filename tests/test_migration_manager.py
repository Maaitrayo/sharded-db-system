from __future__ import annotations

from pathlib import Path

import pytest

from src.consistent_hash import ConsistentHashRing
from src.migration_manager import MigrationManager
from src.shard_manager import ShardManager


def _seed_users_for_ring(
    manager: ShardManager, nodes: list[str], user_ids: list[int], vnodes: int = 64
) -> None:
    ring = ConsistentHashRing(nodes=nodes, virtual_nodes_per_server=vnodes)
    for user_id in user_ids:
        node_id = ring.get_node(user_id)
        shard_index = int(node_id.replace("shard", "", 1))
        with manager.get_connection(shard_index) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO users (user_id, name)
                VALUES (?, ?)
                """,
                (user_id, f"user-{user_id}"),
            )
            conn.commit()


def _count_rows(manager: ShardManager, shard_index: int) -> int:
    with manager.get_connection(shard_index) as conn:
        row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
    return int(row[0])


def test_simulate_add_server_creates_plan_to_new_node(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=4, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    active_nodes = ["shard0", "shard1", "shard2"]
    _seed_users_for_ring(manager, active_nodes, list(range(300)), vnodes=64)

    migration = MigrationManager(
        shard_manager=manager, active_nodes=active_nodes, virtual_nodes_per_server=64
    )
    plan = migration.simulate_add_server("shard3", apply=False)

    assert plan["event"] == "add_server"
    assert plan["applied"] is False
    assert plan["records_scanned"] == 300
    assert plan["records_planned"] > 0
    assert all(move["to"] == "shard3" for move in plan["moves"])


def test_apply_add_server_moves_records_and_updates_active_nodes(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=4, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    active_nodes = ["shard0", "shard1", "shard2"]
    _seed_users_for_ring(manager, active_nodes, list(range(400)), vnodes=64)

    migration = MigrationManager(
        shard_manager=manager, active_nodes=active_nodes, virtual_nodes_per_server=64
    )
    before_new_node_count = _count_rows(manager, 3)

    result = migration.simulate_add_server("shard3", apply=True)
    after_new_node_count = _count_rows(manager, 3)

    assert result["event"] == "add_server"
    assert result["applied"] is True
    assert result["records_moved"] > 0
    assert after_new_node_count > before_new_node_count
    assert migration.active_nodes == ["shard0", "shard1", "shard2", "shard3"]


def test_simulate_server_failure_moves_from_failed_node(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=4, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    active_nodes = ["shard0", "shard1", "shard2", "shard3"]
    _seed_users_for_ring(manager, active_nodes, list(range(500)), vnodes=64)

    migration = MigrationManager(
        shard_manager=manager, active_nodes=active_nodes, virtual_nodes_per_server=64
    )
    plan = migration.simulate_server_failure("shard1", apply=False)

    assert plan["event"] == "server_failure"
    assert plan["applied"] is False
    assert plan["records_planned"] > 0
    assert all(move["from"] == "shard1" for move in plan["moves"])


def test_apply_server_failure_redistributes_records(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=4, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    active_nodes = ["shard0", "shard1", "shard2", "shard3"]
    _seed_users_for_ring(manager, active_nodes, list(range(500)), vnodes=64)

    migration = MigrationManager(
        shard_manager=manager, active_nodes=active_nodes, virtual_nodes_per_server=64
    )
    before_failed_count = _count_rows(manager, 1)

    result = migration.simulate_server_failure("shard1", apply=True)
    after_failed_count = _count_rows(manager, 1)

    assert before_failed_count > 0
    assert result["event"] == "server_failure"
    assert result["applied"] is True
    assert result["records_moved"] > 0
    assert after_failed_count == 0
    assert migration.active_nodes == ["shard0", "shard2", "shard3"]


def test_dry_run_does_not_move_records(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=4, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    active_nodes = ["shard0", "shard1", "shard2"]
    _seed_users_for_ring(manager, active_nodes, list(range(300)), vnodes=64)

    migration = MigrationManager(
        shard_manager=manager, active_nodes=active_nodes, virtual_nodes_per_server=64
    )
    before_new_node_count = _count_rows(manager, 3)
    plan = migration.simulate_add_server("shard3", apply=False)
    after_new_node_count = _count_rows(manager, 3)

    assert plan["applied"] is False
    assert plan["records_planned"] > 0
    assert after_new_node_count == before_new_node_count
    assert migration.active_nodes == ["shard0", "shard1", "shard2"]


def test_server_failure_rejects_last_remaining_node(tmp_path: Path) -> None:
    manager = ShardManager(num_shards=1, db_dir=str(tmp_path / "db"))
    manager.initialize_shards()
    migration = MigrationManager(
        shard_manager=manager, active_nodes=["shard0"], virtual_nodes_per_server=64
    )

    with pytest.raises(ValueError):
        migration.simulate_server_failure("shard0", apply=False)
