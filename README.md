## Sharded DB System

Minimal Python project that demonstrates sharded storage with:
- Modulo sharding
- Consistent hashing
- Migration simulation (add server, server failure)

## What Is Implemented
- `ShardManager` for shard file creation and schema initialization
- Config-driven router selection (`modulo` or `consistent_hashing`)
- Consistent hash ring with virtual nodes
- Migration manager for topology changes
- FastAPI app with:
  - `POST /users`
  - `GET /users/{user_id}`
  - `POST /admin/migrations/add-server`
  - `POST /admin/migrations/server-failure`
- Test suite for config, routing, API, and migration behavior

## Project Structure
```text
sharded-db-system/
|-- src/
|   |-- api.py
|   |-- config.py
|   |-- consistent_hash.py
|   |-- migration_manager.py
|   |-- router.py
|   |-- router_factory.py
|   `-- shard_manager.py
|-- docs/
|   |-- architecture.md
|   `-- consistent_hashing_architecture.md
|-- tests/
|-- main.py
`-- pyproject.toml
```

## Setup
```bash
uv sync
```

## Configure Sharding Strategy
Create a config file (JSON), for example `config/sharding.json`.

Modulo:
```json
{
  "sharding": {
    "strategy": "modulo",
    "modulo": {
      "num_shards": 3
    }
  }
}
```

Consistent hashing:
```json
{
  "sharding": {
    "strategy": "consistent_hashing",
    "num_shards": 4,
    "consistent_hashing": {
      "virtual_nodes_per_server": 100,
      "nodes": ["shard0", "shard1", "shard2"]
    }
  }
}
```

`num_shards` is total shard files; `nodes` are currently active nodes. This lets you keep spare shards for add-server migration.

Set config path:
```bash
export SHARDING_CONFIG_PATH=/absolute/path/to/config/sharding.json
```

PowerShell:
```powershell
$env:SHARDING_CONFIG_PATH="C:\Users\USER\work\PERSONAL PROJECTS\sharded-db-system\config\sharding.json"
```

## Run the API
```bash
uv run python main.py
```

Server starts on `http://127.0.0.1:8000`.

## API Usage
Create user:
```bash
curl -X POST "http://127.0.0.1:8000/users" \
  -H "Content-Type: application/json" \
  -d "{\"user_id\": 10, \"name\": \"Alice\"}"
```

Get user:
```bash
curl "http://127.0.0.1:8000/users/10"
```

### Migration Endpoints (Consistent Hashing Only)
Dry-run add server:
```bash
curl -X POST "http://127.0.0.1:8000/admin/migrations/add-server" \
  -H "Content-Type: application/json" \
  -d "{\"new_node_id\":\"shard3\",\"apply\":false}"
```

Apply add server:
```bash
curl -X POST "http://127.0.0.1:8000/admin/migrations/add-server" \
  -H "Content-Type: application/json" \
  -d "{\"new_node_id\":\"shard3\",\"apply\":true}"
```

Dry-run server failure:
```bash
curl -X POST "http://127.0.0.1:8000/admin/migrations/server-failure" \
  -H "Content-Type: application/json" \
  -d "{\"failed_node_id\":\"shard1\",\"apply\":false}"
```

Apply server failure:
```bash
curl -X POST "http://127.0.0.1:8000/admin/migrations/server-failure" \
  -H "Content-Type: application/json" \
  -d "{\"failed_node_id\":\"shard1\",\"apply\":true}"
```

## Check DB Contents
After creating users, inspect shard files in `src/db/`.

Option 1 (`sqlite3` CLI, if installed):
```bash
sqlite3 src/db/shard0.db "SELECT user_id, name FROM users;"
sqlite3 src/db/shard1.db "SELECT user_id, name FROM users;"
sqlite3 src/db/shard2.db "SELECT user_id, name FROM users;"
```

Option 2 (Python, works anywhere):
```bash
uv run python -c "import sqlite3; [print(i, sqlite3.connect(f'src/db/shard{i}.db').execute('SELECT user_id, name FROM users').fetchall()) for i in range(3)]"
```

## Run Tests
```bash
uv run pytest
```
