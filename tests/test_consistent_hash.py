from __future__ import annotations

import pytest

from src.consistent_hash import ConsistentHashRing, stable_hash


def test_stable_hash_is_deterministic() -> None:
    assert stable_hash("user:10") == stable_hash("user:10")
    assert stable_hash("user:10") != stable_hash("user:11")


def test_ring_requires_nodes_for_lookup() -> None:
    ring = ConsistentHashRing(nodes=[], virtual_nodes_per_server=3)
    with pytest.raises(ValueError):
        ring.get_node(1)


def test_ring_maps_to_known_nodes() -> None:
    ring = ConsistentHashRing(
        nodes=["shard0", "shard1", "shard2"], virtual_nodes_per_server=5
    )
    node = ring.get_node(42)
    assert node in {"shard0", "shard1", "shard2"}


def test_add_and_remove_node_updates_membership() -> None:
    ring = ConsistentHashRing(nodes=["shard0"], virtual_nodes_per_server=4)
    assert ring.nodes == ("shard0",)

    ring.add_node("shard1")
    assert set(ring.nodes) == {"shard0", "shard1"}

    ring.remove_node("shard0")
    assert ring.nodes == ("shard1",)


def test_snapshot_includes_all_virtual_tokens() -> None:
    ring = ConsistentHashRing(nodes=["shard0", "shard1"], virtual_nodes_per_server=7)
    snapshot = ring.snapshot()

    assert set(snapshot.keys()) == {"shard0", "shard1"}
    assert len(snapshot["shard0"]) == 7
    assert len(snapshot["shard1"]) == 7


def test_wrap_around_selects_first_token_node() -> None:
    # Small ring with deterministic token placement for predictable assertions.
    def fixed_hash(value: str) -> int:
        mapping = {
            "shard0#0": 10,
            "shard1#0": 20,
            "0": 25,   # beyond max token -> wrap to token 10 (shard0)
            "1": 15,   # between 10 and 20 -> token 20 (shard1)
        }
        return mapping[value]

    ring = ConsistentHashRing(
        nodes=["shard0", "shard1"],
        virtual_nodes_per_server=1,
        hash_func=fixed_hash,
    )

    assert ring.get_node(0) == "shard0"
    assert ring.get_node(1) == "shard1"

