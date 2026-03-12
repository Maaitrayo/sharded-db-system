from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

ShardingStrategy = Literal["modulo", "consistent_hashing"]


@dataclass(frozen=True)
class ShardingConfig:
    strategy: ShardingStrategy = "modulo"
    num_shards: int = 3
    virtual_nodes_per_server: int = 100
    nodes: tuple[str, ...] | None = None

    @staticmethod
    def _node_id_to_index(node_id: str) -> int:
        if not node_id.startswith("shard"):
            raise ValueError(
                f"Invalid node id '{node_id}'. Expected format like 'shard0'."
            )
        try:
            return int(node_id.replace("shard", "", 1))
        except ValueError as exc:
            raise ValueError(
                f"Invalid node id '{node_id}'. Expected format like 'shard0'."
            ) from exc

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> ShardingConfig:
        if raw is None:
            return cls()

        # Supports either top-level sharding config or {"sharding": {...}}.
        source = raw.get("sharding", raw)
        strategy = source.get("strategy", "modulo")
        if strategy not in ("modulo", "consistent_hashing"):
            raise ValueError(
                "Invalid sharding strategy. Expected 'modulo' or "
                "'consistent_hashing'."
            )

        if strategy == "modulo":
            modulo = source.get("modulo", {})
            num_shards = int(modulo.get("num_shards", source.get("num_shards", 3)))
            if num_shards <= 0:
                raise ValueError("num_shards must be greater than 0")
            return cls(strategy="modulo", num_shards=num_shards)

        consistent = source.get("consistent_hashing", {})
        virtual_nodes_per_server = int(
            consistent.get(
                "virtual_nodes_per_server", source.get("virtual_nodes_per_server", 100)
            )
        )
        if virtual_nodes_per_server <= 0:
            raise ValueError("virtual_nodes_per_server must be greater than 0")

        nodes_raw = consistent.get("nodes", source.get("nodes"))
        if nodes_raw is None:
            num_shards = int(source.get("num_shards", 3))
            if num_shards <= 0:
                raise ValueError("num_shards must be greater than 0")
            nodes = tuple(f"shard{i}" for i in range(num_shards))
            total_shards = num_shards
        else:
            if not isinstance(nodes_raw, list) or not nodes_raw:
                raise ValueError("nodes must be a non-empty list when provided")
            nodes = tuple(str(node) for node in nodes_raw)
            if len(set(nodes)) != len(nodes):
                raise ValueError("nodes must be unique")
            max_node_index = max(cls._node_id_to_index(node) for node in nodes)
            total_shards = int(source.get("num_shards", max_node_index + 1))
            if total_shards <= 0:
                raise ValueError("num_shards must be greater than 0")
            if total_shards <= max_node_index:
                raise ValueError(
                    "num_shards must be greater than highest active node index"
                )

        return cls(
            strategy="consistent_hashing",
            num_shards=total_shards,
            virtual_nodes_per_server=virtual_nodes_per_server,
            nodes=nodes,
        )

    @property
    def node_ids(self) -> tuple[str, ...]:
        if self.nodes is not None:
            return self.nodes
        return tuple(f"shard{i}" for i in range(self.num_shards))
