from __future__ import annotations

from dataclasses import dataclass

from src.consistent_hash import ConsistentHashRing
from src.shard_manager import ShardManager


@dataclass(frozen=True)
class UserRecord:
    user_id: int
    name: str
    source_node: str


class MigrationManager:
    """Plans and executes consistent-hash migration events."""

    def __init__(
        self,
        shard_manager: ShardManager,
        active_nodes: list[str] | tuple[str, ...],
        virtual_nodes_per_server: int = 100,
    ) -> None:
        if not active_nodes:
            raise ValueError("active_nodes must not be empty")
        if virtual_nodes_per_server <= 0:
            raise ValueError("virtual_nodes_per_server must be greater than 0")

        self.shard_manager = shard_manager
        self.active_nodes = list(active_nodes)
        self.virtual_nodes_per_server = virtual_nodes_per_server
        self._ensure_nodes_fit_capacity(self.active_nodes)

    def simulate_add_server(self, new_node_id: str, apply: bool = False) -> dict:
        if new_node_id in self.active_nodes:
            raise ValueError(f"node '{new_node_id}' already exists in active_nodes")

        source_nodes = list(self.active_nodes)
        target_nodes = [*self.active_nodes, new_node_id]
        if apply:
            self._ensure_nodes_fit_capacity(target_nodes)
        plan = self._build_plan(
            event="add_server",
            source_nodes=source_nodes,
            target_nodes=target_nodes,
        )

        if apply:
            result = self.apply_migration(plan)
            self.active_nodes = target_nodes
            return result
        return plan

    def simulate_server_failure(self, failed_node_id: str, apply: bool = False) -> dict:
        if failed_node_id not in self.active_nodes:
            raise ValueError(f"node '{failed_node_id}' not found in active_nodes")
        if len(self.active_nodes) == 1:
            raise ValueError("cannot fail the last remaining node")

        source_nodes = list(self.active_nodes)
        target_nodes = [node for node in self.active_nodes if node != failed_node_id]
        plan = self._build_plan(
            event="server_failure",
            source_nodes=source_nodes,
            target_nodes=target_nodes,
        )

        if apply:
            result = self.apply_migration(plan)
            self.active_nodes = target_nodes
            return result
        return plan

    def apply_migration(self, plan: dict) -> dict:
        self._ensure_nodes_fit_capacity(plan["target_nodes"])
        records_moved = 0
        records_failed = 0

        for move in plan["moves"]:
            source_index = self._node_id_to_shard_index(move["from"])
            target_index = self._node_id_to_shard_index(move["to"])
            user_id = move["user_id"]

            try:
                with self.shard_manager.get_connection(source_index) as source_conn:
                    row = source_conn.execute(
                        """
                        SELECT user_id, name
                        FROM users
                        WHERE user_id = ?
                        """,
                        (user_id,),
                    ).fetchone()

                    if row is None:
                        continue

                    with self.shard_manager.get_connection(target_index) as target_conn:
                        target_conn.execute(
                            """
                            INSERT OR REPLACE INTO users (user_id, name)
                            VALUES (?, ?)
                            """,
                            (row[0], row[1]),
                        )
                        target_conn.commit()

                    source_conn.execute(
                        """
                        DELETE FROM users
                        WHERE user_id = ?
                        """,
                        (user_id,),
                    )
                    source_conn.commit()

                records_moved += 1
            except Exception:
                records_failed += 1

        result = dict(plan)
        result["applied"] = True
        result["records_moved"] = records_moved
        result["records_failed"] = records_failed
        return result

    def _build_plan(
        self, event: str, source_nodes: list[str], target_nodes: list[str]
    ) -> dict:
        source_ring = self._build_ring(source_nodes)
        target_ring = self._build_ring(target_nodes)
        records = self._scan_records(source_nodes)

        moves: list[dict[str, int | str]] = []
        for record in records:
            before_owner = source_ring.get_node(record.user_id)
            after_owner = target_ring.get_node(record.user_id)
            if before_owner != after_owner:
                moves.append(
                    {
                        "user_id": record.user_id,
                        "from": record.source_node,
                        "to": after_owner,
                    }
                )

        return {
            "event": event,
            "source_nodes": source_nodes,
            "target_nodes": target_nodes,
            "records_scanned": len(records),
            "records_planned": len(moves),
            "records_moved": 0,
            "records_failed": 0,
            "applied": False,
            "moves": moves,
        }

    def _build_ring(self, nodes: list[str]) -> ConsistentHashRing:
        return ConsistentHashRing(
            nodes=nodes,
            virtual_nodes_per_server=self.virtual_nodes_per_server,
        )

    def _scan_records(self, nodes: list[str]) -> list[UserRecord]:
        records: list[UserRecord] = []
        for node_id in nodes:
            shard_index = self._node_id_to_shard_index(node_id)
            with self.shard_manager.get_connection(shard_index) as conn:
                rows = conn.execute(
                    """
                    SELECT user_id, name
                    FROM users
                    """
                ).fetchall()
                records.extend(
                    UserRecord(user_id=row[0], name=row[1], source_node=node_id)
                    for row in rows
                )
        return records

    def _node_id_to_shard_index(self, node_id: str) -> int:
        if not node_id.startswith("shard"):
            raise ValueError(
                f"Unsupported node_id '{node_id}'. Expected format like 'shard0'."
            )
        try:
            shard_index = int(node_id.replace("shard", "", 1))
        except ValueError as exc:
            raise ValueError(
                f"Unsupported node_id '{node_id}'. Expected format like 'shard0'."
            ) from exc

        if not 0 <= shard_index < self.shard_manager.num_shards:
            raise ValueError(
                self._capacity_error_message(node_id=node_id, shard_index=shard_index)
            )
        return shard_index

    def _ensure_nodes_fit_capacity(self, nodes: list[str] | tuple[str, ...]) -> None:
        required_shards = self._required_shard_count(nodes)
        if self.shard_manager.num_shards < required_shards:
            highest_node = f"shard{required_shards - 1}"
            raise ValueError(
                self._capacity_error_message(
                    node_id=highest_node, shard_index=required_shards - 1
                )
            )

    def _required_shard_count(self, nodes: list[str] | tuple[str, ...]) -> int:
        highest_index = -1
        for node_id in nodes:
            if not node_id.startswith("shard"):
                raise ValueError(
                    f"Unsupported node_id '{node_id}'. Expected format like 'shard0'."
                )
            try:
                index = int(node_id.replace("shard", "", 1))
            except ValueError as exc:
                raise ValueError(
                    f"Unsupported node_id '{node_id}'. Expected format like 'shard0'."
                ) from exc
            highest_index = max(highest_index, index)
        return highest_index + 1

    def _capacity_error_message(self, node_id: str, shard_index: int) -> str:
        required_shards = shard_index + 1
        return (
            f"node_id '{node_id}' maps to shard index {shard_index}, "
            f"but shard_manager has num_shards={self.shard_manager.num_shards}. "
            f"Set sharding.num_shards to at least {required_shards} in your config, "
            "export SHARDING_CONFIG_PATH to that config, then restart the API."
        )
