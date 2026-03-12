from __future__ import annotations

import bisect
import hashlib
from typing import Callable


def stable_hash(value: str) -> int:
    """Return a deterministic integer hash for a string value."""
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    """Consistent hash ring with virtual nodes."""

    def __init__(
        self,
        nodes: list[str] | tuple[str, ...] | None = None,
        virtual_nodes_per_server: int = 100,
        hash_func: Callable[[str], int] = stable_hash,
    ) -> None:
        if virtual_nodes_per_server <= 0:
            raise ValueError("virtual_nodes_per_server must be greater than 0")
        self.virtual_nodes_per_server = virtual_nodes_per_server
        self.hash_func = hash_func
        self._tokens: list[int] = []
        self._token_to_node: dict[int, str] = {}
        self._node_to_tokens: dict[str, list[int]] = {}

        for node in nodes or []:
            self.add_node(node)

    @property
    def nodes(self) -> tuple[str, ...]:
        return tuple(self._node_to_tokens.keys())

    def add_node(self, node_id: str) -> None:
        if not node_id.strip():
            raise ValueError("node_id must not be empty")
        if node_id in self._node_to_tokens:
            raise ValueError(f"node '{node_id}' already exists")

        node_tokens: list[int] = []
        for vnode_index in range(self.virtual_nodes_per_server):
            token = self.hash_func(f"{node_id}#{vnode_index}")
            # Resolve any rare hash collisions deterministically.
            while token in self._token_to_node:
                token += 1
            bisect.insort(self._tokens, token)
            self._token_to_node[token] = node_id
            node_tokens.append(token)

        self._node_to_tokens[node_id] = node_tokens

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._node_to_tokens:
            raise ValueError(f"node '{node_id}' does not exist")

        tokens = self._node_to_tokens.pop(node_id)
        for token in tokens:
            self._token_to_node.pop(token, None)
            index = bisect.bisect_left(self._tokens, token)
            if index < len(self._tokens) and self._tokens[index] == token:
                self._tokens.pop(index)

    def get_node(self, shard_key: int | str) -> str:
        if not self._tokens:
            raise ValueError("ring has no nodes")

        key_hash = self.hash_func(str(shard_key))
        index = bisect.bisect_left(self._tokens, key_hash)
        if index == len(self._tokens):
            index = 0
        token = self._tokens[index]
        return self._token_to_node[token]

    def snapshot(self) -> dict[str, list[int]]:
        return {
            node_id: sorted(tokens)
            for node_id, tokens in self._node_to_tokens.items()
        }

