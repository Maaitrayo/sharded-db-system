# Migration Simulation Guide

## Goal
Simulate and verify both migration events through API endpoints:
1. Add server (`shard3`)
2. Server failure (`shard1`)

## Use This Exact Flow
Use two terminals:
- Terminal A: run API
- Terminal B: run curl/test commands

## 1. Create Consistent-Hash Config (with spare shard)
Create `config/sharding.json`:

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

Important:
- `num_shards` is total shard DB files.
- `nodes` are active nodes at startup.
- `shard3` is spare capacity for add-server apply.

## 2. Set Config Path in the Same Shell That Starts API
### Git Bash
```bash
export SHARDING_CONFIG_PATH="$PWD/config/sharding.json"
```

### PowerShell
```powershell
$env:SHARDING_CONFIG_PATH="C:\Users\USER\work\PERSONAL PROJECTS\sharded-db-system\config\sharding.json"
```

## 3. Install Dependencies
```bash
uv sync
```

## 4. Seed Data
Run once before starting API (or while API is stopped):

```bash
uv run python -m src.seed_data --count 500 --start-id 1 --db-dir src/db --config-path config/sharding.json
```

## 5. Start API (Terminal A)
```bash
uv run python main.py
```

Base URL: `http://127.0.0.1:8000`

## 6. Baseline Check (Terminal B)
```bash
curl "http://127.0.0.1:8000/users/10"
curl "http://127.0.0.1:8000/users/250"
```

Expected: `200` responses with user payloads.

## 7. Add-Server Simulation
### 7.1 Dry run
```bash
curl -X POST "http://127.0.0.1:8000/admin/migrations/add-server" -H "Content-Type: application/json" -d "{\"new_node_id\":\"shard3\",\"apply\":false}"
```

Expected:
- `event = add_server`
- `applied = false`
- `records_planned > 0`

### 7.2 Apply
```bash
curl -X POST "http://127.0.0.1:8000/admin/migrations/add-server" -H "Content-Type: application/json" -d "{\"new_node_id\":\"shard3\",\"apply\":true}"
```

Expected:
- `event = add_server`
- `applied = true`
- `records_moved > 0`

## 8. Server-Failure Simulation
### 8.1 Dry run
```bash
curl -X POST "http://127.0.0.1:8000/admin/migrations/server-failure" -H "Content-Type: application/json" -d "{\"failed_node_id\":\"shard1\",\"apply\":false}"
```

Expected:
- `event = server_failure`
- `applied = false`
- `records_planned > 0`

### 8.2 Apply
```bash
curl -X POST "http://127.0.0.1:8000/admin/migrations/server-failure" -H "Content-Type: application/json" -d "{\"failed_node_id\":\"shard1\",\"apply\":true}"
```

Expected:
- `event = server_failure`
- `applied = true`
- `records_moved > 0`

## 9. Post-Migration Verification
### 9.1 Read checks still pass
```bash
curl "http://127.0.0.1:8000/users/10"
curl "http://127.0.0.1:8000/users/250"
```

### 9.2 Check row counts per shard
```bash
uv run python -c "import sqlite3; [print(i, sqlite3.connect(f'src/db/shard{i}.db').execute('select count(*) from users').fetchone()[0]) for i in range(4)]"
```

## 10. Automated Verification
```bash
uv run pytest tests/test_migration_manager.py tests/test_api.py
```

## Common Failure and Fix
If apply returns a capacity error like:
`node_id 'shard3' maps to shard index 3, but shard_manager has num_shards=3`

Do this:
1. Set `sharding.num_shards` to at least `4` in `config/sharding.json`.
2. Re-export `SHARDING_CONFIG_PATH`.
3. Restart API (`Ctrl+C`, then `uv run python main.py`).
4. Retry apply endpoint.
