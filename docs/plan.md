# Project Plan

## Goals
- Implement a minimal sharded database system with clear routing logic.
- Provide a small API layer for read/write operations.
- Keep the system easy to run locally and understand.

## Milestones
1. Core sharding logic and storage
2. API layer with basic endpoints
3. Basic tests and documentation

## Work Breakdown
1. Project setup
2. Shard manager
3. Shard router
4. API layer
5. Database initialization
6. Tests
7. Documentation polish

## Tasks
- Define shard count and shard key (e.g., `user_id`).
- Implement `ShardManager` to open and initialize SQLite shards.
- Implement `ShardRouter` to map keys to shard indices.
- Implement API endpoints for:
  - `POST /users`
  - `GET /users/{user_id}`
- Initialize schema for each shard (users table).
- Add minimal tests:
  - Router determinism
  - Insert and fetch flow
- Update docs:
  - Architecture
  - Usage and setup

## Risks and Mitigations
- Hot keys unevenly load a shard: document limitation; consider consistent hashing later.
- Shard count change causes key movement: note as future improvement.
- SQLite concurrency limits: acceptable for demo scope.

## Definition of Done
- API can insert and read users correctly.
- Data lands on the expected shard for a given key.
- Tests pass locally.
- Docs explain architecture and how to run.
