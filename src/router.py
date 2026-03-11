from __future__ import annotations

from src.shard_manager import ShardManager


class ShardRouter:
    """Routes operations to the correct shard based on user_id."""

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

    def _validate_user_id(self, user_id: int) -> None:
        if user_id < 0:
            raise ValueError("user_id must be >= 0")

    def _validate_user_name(self, name: str) -> None:
        if not name.strip():
            raise ValueError("name must not be empty")
