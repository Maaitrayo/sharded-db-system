## Sharded DB System

Minimal Python project that demonstrates hash-based database sharding with a FastAPI service and SQLite shard files.

## What Is Implemented
- `ShardManager` for shard file creation and schema initialization
- `ShardRouter` for deterministic shard selection and routed CRUD
- FastAPI app with:
  - `POST /users`
  - `GET /users/{user_id}`
- Basic test suite for shard manager, router, and API

## Project Structure
```text
sharded-db-system/
├── src/
│   ├── api.py
│   ├── router.py
│   └── shard_manager.py
├── tests/
│   ├── test_api.py
│   ├── test_router.py
│   └── test_shard_manager.py
├── docs/
│   ├── architecture.md
│   └── plan.md
├── main.py
└── pyproject.toml
```

## Setup
```bash
uv sync
```

## Run The API
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
