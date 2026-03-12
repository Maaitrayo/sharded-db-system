# Consistent Hashing Architecture

## Overview
This document defines a consistent-hashing sharding model for this project and adds migration workflows to simulate:
- New server added
- Existing server failure

Compared to `hash(key) % num_shards`, consistent hashing minimizes key movement when topology changes.

## Goals
- Keep shard routing deterministic.
- Reduce data movement during scaling events.
- Simulate operational events (node add/fail) with explicit migration functions.
- Stay compatible with current SQLite-based local shards.

## High-Level Design
Current flow:
```text
Client -> API -> ShardRouter -> ShardManager -> SQLite shard file
```

Target flow with consistent hashing:
```text
Client -> API -> ConsistentHashRouter -> Ring -> ShardManager -> SQLite shard file
```

Target flow with strategy config:
```text
Client -> API -> RouterFactory(config.sharding.strategy) -> SelectedRouter -> ShardManager -> SQLite shard file
```

## Stage Block Diagrams

### Stage 0: Startup and Strategy Selection
```text
                 +-----------------------+
                 |   Sharding Config     |
                 | strategy, nodes, vns  |
                 +-----------+-----------+
                             |
                             v
Client --> API --> RouterFactory --> (ModuloRouter | ConsistentHashRouter) --> ShardManager --> shardN.db
```

### Stage 1: Normal Consistent-Hash Routing
```text
Client request (user_id)
        |
        v
+-------+--------+      +---------------------+      +--------------------+
|      API       | ---> | ConsistentHashRing  | ---> | owner node: shardX |
+----------------+      +---------------------+      +---------+----------+
                                                             |
                                                             v
                                                     +---------------+
                                                     | ShardManager  |
                                                     +-------+-------+
                                                             |
                                                             v
                                                     +---------------+
                                                     |  shardX.db    |
                                                     +---------------+
```

### Stage 2: Add-Server Migration (Before -> Plan -> Apply -> After)
```text
Before:
Ring nodes: [shard0, shard1, shard2]

Dry-run plan:
API --> MigrationManager --> Build before/after rings
                         --> Compare ownership for each record
                         --> moves: [{from: shard0|1|2, to: shard3}, ...]

Apply:
for each move:
  source shard -> read row -> write target shard -> delete source row

After:
Ring nodes: [shard0, shard1, shard2, shard3]
Only affected keys moved to shard3
```

### Stage 3: Server-Failure Migration (Before -> Plan -> Apply -> After)
```text
Before:
Ring nodes: [shard0, shard1, shard2, shard3]

Dry-run plan:
API --> MigrationManager --> Remove failed node from target ring
                         --> Compare ownership
                         --> moves: [{from: shard1, to: shard0|2|3}, ...]

Apply:
for each planned key from failed node:
  failed-node shard -> read row -> write new owner shard -> delete source row

After:
Ring nodes: [shard0, shard2, shard3]
Failed node keys redistributed clockwise
```

## Core Concepts

### Hash Ring
- The ring is a sorted map of `token -> node_id`.
- For a key, compute `key_hash` and choose the first token clockwise.
- If no token is greater than `key_hash`, wrap to the first token.

### Virtual Nodes (vnodes)
- Each physical node owns multiple tokens (`virtual_nodes_per_server`, e.g., 100).
- Vnodes smooth key distribution and reduce hotspot risk.

### Node Identity
- Use stable IDs (for example: `shard0`, `shard1`, `shard2`).
- DB files map to node IDs (for example: `src/db/shard0.db`).

## Proposed Components

### `ShardingConfig`
Responsibilities:
- Define active sharding strategy at runtime.
- Hold strategy-specific parameters.
- Keep one config source for API/router initialization.

Suggested config shape:
```yaml
sharding:
  strategy: "consistent_hashing"  # "modulo" | "consistent_hashing"
  modulo:
    num_shards: 3
  consistent_hashing:
    virtual_nodes_per_server: 100
    nodes: ["shard0", "shard1", "shard2"]
```

Python model example:
```python
from dataclasses import dataclass
from typing import Literal

ShardingStrategy = Literal["modulo", "consistent_hashing"]

@dataclass
class ShardingConfig:
    strategy: ShardingStrategy
    num_shards: int = 3
    virtual_nodes_per_server: int = 100
    nodes: list[str] | None = None
```

### `RouterFactory`
Responsibilities:
- Read `ShardingConfig`.
- Return the correct router implementation.
- Keep API layer independent of strategy details.

Suggested interface:
```python
class RouterFactory:
    @staticmethod
    def build(config: ShardingConfig, shard_manager: ShardManager):
        if config.strategy == "modulo":
            return ModuloShardRouter(shard_manager, config.num_shards)
        return ConsistentHashRouter(shard_manager, config)
```

### `ConsistentHashRing`
Responsibilities:
- Add/remove nodes and vnodes.
- Resolve owner node for a key.
- Provide token range ownership metadata for migration.

Suggested interface:
```python
class ConsistentHashRing:
    def add_node(self, node_id: str) -> None: ...
    def remove_node(self, node_id: str) -> None: ...
    def get_node(self, shard_key: int | str) -> str: ...
    def snapshot(self) -> dict[str, list[int]]: ...
```

### `MigrationManager`
Responsibilities:
- Compare old and new ring ownership.
- Move only affected records.
- Support dry-run simulation and metrics.

Suggested interface:
```python
class MigrationManager:
    def simulate_add_server(self, new_node_id: str) -> dict: ...
    def simulate_server_failure(self, failed_node_id: str) -> dict: ...
    def apply_migration(self, plan: dict) -> dict: ...
```

## Routing Algorithm
Use a stable hash (do not use Python built-in `hash()` for cross-process stability).

Suggested approach:
1. Convert shard key to bytes.
2. Compute hash with SHA-256.
3. Convert to integer.
4. Find clockwise token owner on the ring.

```python
def stable_hash(value: str) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest(), 16)
```

## Migration Workflows

### 1. New Server Added
When `new_node_id` is added:
1. Take ring snapshot `before`.
2. Add node and generate its vnode tokens.
3. Take ring snapshot `after`.
4. For each record, compute owner in `before` and `after`.
5. Migrate records where owner changed.

Expected behavior:
- Only a fraction of keys move.
- Existing nodes mostly keep their data.

Function shape:
```python
def simulate_add_server(new_node_id: str, apply: bool = False) -> dict:
    """Build migration plan and optionally execute it."""
```

### 2. Server Failure
When `failed_node_id` fails:
1. Take ring snapshot `before`.
2. Remove failed node from ring.
3. Take ring snapshot `after`.
4. Reassign failed node keys to new owners.
5. If this is a simulation, do not delete source data.

Expected behavior:
- Keys from failed node redistribute clockwise to surviving nodes.
- No full-cluster rebalance.

Function shape:
```python
def simulate_server_failure(failed_node_id: str, apply: bool = False) -> dict:
    """Plan/execute remapping when a node is unavailable."""
```

## Record Movement Strategy
Because this project uses SQLite files per shard, migration can be implemented as:
1. Scan each shard table (`users`).
2. Recompute destination node via target ring.
3. Insert into destination shard using upsert.
4. Delete from source shard only after successful destination write.
5. Track counters for moved/skipped/failed records.

Suggested migration result schema:
```python
{
    "event": "add_server" | "server_failure",
    "source_nodes": [...],
    "target_nodes": [...],
    "records_scanned": 0,
    "records_moved": 0,
    "records_failed": 0,
    "moves": [
        {"user_id": 42, "from": "shard1", "to": "shard3"}
    ]
}
```

## Failure and Safety Considerations
- Use transaction per move batch for SQLite consistency.
- Keep idempotent upsert semantics (`INSERT OR REPLACE`).
- Add a `dry_run=True` mode by default in simulations.
- Persist migration logs for replay/audit.
- Avoid split-brain simulation by using one ring source of truth in memory.

## Example Topology Change
Initial nodes:
- `shard0`, `shard1`, `shard2`

After adding `shard3`:
- Only keys now mapped to `shard3` move.

After failing `shard1`:
- Only keys owned by `shard1` are remapped to surviving nodes.

## Integration Plan for This Repo
1. Add `src/config.py` with `ShardingConfig` and strategy parsing.
2. Add `src/consistent_hash.py` for ring and hashing primitives.
3. Add `src/migration_manager.py` for migration planning/execution.
4. Split routing into strategy-specific routers:
   - `ModuloShardRouter` (existing behavior)
   - `ConsistentHashRouter` (ring-based behavior)
5. Add `src/router_factory.py` to select router from config.
6. Keep `ShardManager` as storage layer; extend it to resolve connections by `node_id`.
7. Add tests:
   - Deterministic routing across process restarts.
   - Strategy selection works from config.
   - Modulo strategy backward compatibility.
   - Limited key movement on add-server event.
   - Correct remapping on server-failure event.
   - Dry-run vs apply behavior.

## Non-Goals (Current Scope)
- Replication and quorum protocols.
- Multi-datacenter placement.
- Automatic failure detection.

These can be layered on top after consistent hashing and migration simulation are stable.
