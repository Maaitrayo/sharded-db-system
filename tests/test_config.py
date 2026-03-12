from __future__ import annotations

import pytest

from src.config import ShardingConfig


def test_default_config_is_modulo_with_three_shards() -> None:
    config = ShardingConfig.from_dict(None)

    assert config.strategy == "modulo"
    assert config.num_shards == 3
    assert config.node_ids == ("shard0", "shard1", "shard2")


def test_modulo_config_from_nested_shape() -> None:
    config = ShardingConfig.from_dict({"sharding": {"strategy": "modulo", "modulo": {"num_shards": 5}}})

    assert config.strategy == "modulo"
    assert config.num_shards == 5
    assert config.node_ids == ("shard0", "shard1", "shard2", "shard3", "shard4")


def test_consistent_hashing_with_explicit_nodes() -> None:
    config = ShardingConfig.from_dict(
        {
            "sharding": {
                "strategy": "consistent_hashing",
                "consistent_hashing": {
                    "virtual_nodes_per_server": 64,
                    "nodes": ["shard0", "shard1", "shard2"],
                },
            }
        }
    )

    assert config.strategy == "consistent_hashing"
    assert config.virtual_nodes_per_server == 64
    assert config.node_ids == ("shard0", "shard1", "shard2")
    assert config.num_shards == 3


def test_consistent_hashing_defaults_nodes_from_num_shards() -> None:
    config = ShardingConfig.from_dict(
        {"sharding": {"strategy": "consistent_hashing", "num_shards": 2}}
    )
    assert config.node_ids == ("shard0", "shard1")
    assert config.num_shards == 2


def test_invalid_strategy_raises_value_error() -> None:
    with pytest.raises(ValueError):
        ShardingConfig.from_dict({"sharding": {"strategy": "range"}})


def test_consistent_hashing_supports_spare_shards() -> None:
    config = ShardingConfig.from_dict(
        {
            "sharding": {
                "strategy": "consistent_hashing",
                "num_shards": 4,
                "consistent_hashing": {"nodes": ["shard0", "shard1", "shard2"]},
            }
        }
    )

    assert config.num_shards == 4
    assert config.node_ids == ("shard0", "shard1", "shard2")
