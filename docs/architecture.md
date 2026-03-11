# Sharded DB System Architecture

## Overview
This project demonstrates a minimal, realistic sharded database architecture. A client sends requests to an API, the API forwards to a shard router, and the router selects the correct database shard based on a shard key.

Primary goals:
- Show how shard routing works.
- Keep the system small and understandable.
- Use simple, local storage for demonstrability.

## High-Level Architecture
```
Client -> API Server -> Shard Router -> Database Shards
```

ASCII view:
```
               +----------------+
Client ------> |   API Server   |
               +--------+-------+
                        |
                        v
               +----------------+
               |  Shard Router  |
               +---+---+---+----+
                   |   |   |
                   v   v   v
                  DB0 DB1 DB2
```

## Components
- Client: Issues API requests (reads/writes).
- API Server: Validates input and delegates to the router.
- Shard Router: Computes the shard index from the shard key.
- Shards: Separate database files (one per shard).

## Sharding Strategy
Shard selection uses a deterministic hash function:
```
shard_index = hash(shard_key) % num_shards
```

Properties:
- Consistent routing: same key always lands on the same shard.
- Even distribution: expected to spread keys across shards.

Limitations:
- When the number of shards changes, many keys move.
- No awareness of shard load or hot keys.

## Request Flow
Write path:
```
Client -> API Server -> Shard Router -> Shard DB
```

Read path:
```
Client -> API Server -> Shard Router -> Shard DB -> API -> Client
```

## Data Storage
- Each shard is a separate SQLite file on disk.
- Example naming:
  - `db/shard0.db`
  - `db/shard1.db`
  - `db/shard2.db`

## Repository Layout
Current repo structure:
```
sharded-db-system/
├── docs/
│   └── architecture.md
├── main.py
└── pyproject.toml
```

Planned (target) structure for the full system:
```
sharded-db-system/
├── src/
│   ├── api.py
│   ├── router.py
│   ├── shard_manager.py
│   └── db/
│       ├── shard0.db
│       ├── shard1.db
│       └── shard2.db
└── docs/
    └── architecture.md
```

## Example Shard Mapping
```
user_id | shard_index
--------------------
10      | 1
25      | 0
30      | 2
42      | 0
```

## Future Improvements
- Consistent hashing to minimize rebalancing when adding shards.
- Connection pooling and async drivers for higher throughput.
- Read replicas and failover.
- Service discovery for dynamic shard maps.
