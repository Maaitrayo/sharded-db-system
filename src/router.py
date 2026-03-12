from __future__ import annotations

from src.config import ShardingConfig
from src.consistent_hash import ConsistentHashRing
from src.shard_manager import ShardManager


class BaseShardRouter:
    """Shared router behavior for validating user payloads."""

    def _validate_user_id(self, user_id: int) -> None:
        if user_id < 0:
            raise ValueError("user_id must be >= 0")

    def _validate_user_name(self, name: str) -> None:
        if not name.strip():
            raise ValueError("name must not be empty")


class ModuloShardRouter(BaseShardRouter):
    """Routes operations using modulo-based sharding."""

    def __init__(self, shard_manager: ShardManager) -> None:
        self.shard_manager = shard_manager

    def get_shard_index(self, user_id: int) -> int:
        """Compute shard index for a given user_id."""
        self._validate_user_id(user_id)
        return hash(user_id) % self.shard_manager.num_shards

    def create_user(self, user_id: int, name: str) -> None:
        """Insert or replace a user in the routed shard."""
        self._validate_user_id(user_id)
        self._validate_user_name(name)
        shard_index = self.get_shard_index(user_id)
        print(f"Routing user_id {user_id} to shard {shard_index}")

        with self.shard_manager.get_connection(shard_index) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO users (user_id, name)
                VALUES (?, ?)
                """,
                (user_id, name),
            )
            conn.commit()

    def get_user(self, user_id: int) -> dict[str, int | str] | None:
        """Fetch a user by user_id from the routed shard."""
        self._validate_user_id(user_id)
        shard_index = self.get_shard_index(user_id)
        print(f"Fetching user_id {user_id} from shard {shard_index}")

        with self.shard_manager.get_connection(shard_index) as conn:
            row = conn.execute(
                """
                SELECT user_id, name
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()

        if row is None:
            return None

        return {"user_id": row[0], "name": row[1]}


class ConsistentHashRouter(BaseShardRouter):
    """Routes operations using consistent hashing."""

    def __init__(self, shard_manager: ShardManager, config: ShardingConfig) -> None:
        self.shard_manager = shard_manager
        self.node_ids = config.node_ids
        self.node_to_index = self._build_node_to_index(self.node_ids)
        self.ring = ConsistentHashRing(
            nodes=self.node_ids,
            virtual_nodes_per_server=config.virtual_nodes_per_server,
        )

    def refresh_nodes(self, node_ids: list[str] | tuple[str, ...]) -> None:
        self.node_ids = tuple(node_ids)
        self.node_to_index = self._build_node_to_index(self.node_ids)
        self.ring = ConsistentHashRing(
            nodes=self.node_ids,
            virtual_nodes_per_server=self.ring.virtual_nodes_per_server,
        )

    def get_shard_index(self, user_id: int) -> int:
        self._validate_user_id(user_id)
        node_id = self.ring.get_node(user_id)
        return self.node_to_index[node_id]

    def create_user(self, user_id: int, name: str) -> None:
        self._validate_user_id(user_id)
        self._validate_user_name(name)
        shard_index = self.get_shard_index(user_id)
        print(f"Routing user_id {user_id} to shard {shard_index}")

        with self.shard_manager.get_connection(shard_index) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO users (user_id, name)
                VALUES (?, ?)
                """,
                (user_id, name),
            )
            conn.commit()

    def get_user(self, user_id: int) -> dict[str, int | str] | None:
        self._validate_user_id(user_id)
        shard_index = self.get_shard_index(user_id)
        print(f"Fetching user_id {user_id} from shard {shard_index}")

        with self.shard_manager.get_connection(shard_index) as conn:
            row = conn.execute(
                """
                SELECT user_id, name
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()

        if row is None:
            return None

        return {"user_id": row[0], "name": row[1]}

    def _build_node_to_index(
        self, node_ids: tuple[str, ...] | list[str]
    ) -> dict[str, int]:
        mapping: dict[str, int] = {}
        for node_id in node_ids:
            if not node_id.startswith("shard"):
                raise ValueError(
                    f"Invalid node id '{node_id}'. Expected format like 'shard0'."
                )
            try:
                index = int(node_id.replace("shard", "", 1))
            except ValueError as exc:
                raise ValueError(
                    f"Invalid node id '{node_id}'. Expected format like 'shard0'."
                ) from exc
            if not 0 <= index < self.shard_manager.num_shards:
                raise ValueError(
                    f"node_id '{node_id}' maps to invalid shard index {index}"
                )
            mapping[node_id] = index
        return mapping


# Backward-compatible alias for existing imports and tests.
ShardRouter = ModuloShardRouter
