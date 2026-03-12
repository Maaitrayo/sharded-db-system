from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from src.api import create_app, load_sharding_config_from_env


def test_create_user_and_get_user(tmp_path: Path) -> None:
    app = create_app(num_shards=3, db_dir=str(tmp_path / "db"))
    with TestClient(app) as client:
        create_response = client.post("/users", json={"user_id": 10, "name": "Alice"})
        assert create_response.status_code == 200
        assert create_response.json()["message"] == "user stored"

        get_response = client.get("/users/10")
        assert get_response.status_code == 200
        assert get_response.json() == {"user_id": 10, "name": "Alice"}


def test_get_user_not_found(tmp_path: Path) -> None:
    app = create_app(num_shards=3, db_dir=str(tmp_path / "db"))
    with TestClient(app) as client:
        response = client.get("/users/9999")
        assert response.status_code == 404
        assert response.json() == {"detail": "user not found"}


def test_create_user_and_get_user_with_consistent_hashing_config(tmp_path: Path) -> None:
    app = create_app(
        db_dir=str(tmp_path / "db"),
        sharding_config={
            "sharding": {
                "strategy": "consistent_hashing",
                "consistent_hashing": {
                    "virtual_nodes_per_server": 32,
                    "nodes": ["shard0", "shard1", "shard2"],
                },
            }
        },
    )
    with TestClient(app) as client:
        create_response = client.post("/users", json={"user_id": 21, "name": "Eve"})
        assert create_response.status_code == 200
        payload = create_response.json()
        assert payload["message"] == "user stored"
        assert 0 <= payload["shard_index"] <= 2

        get_response = client.get("/users/21")
        assert get_response.status_code == 200
        assert get_response.json() == {"user_id": 21, "name": "Eve"}


def test_load_sharding_config_from_env(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "sharding.json"
    config_path.write_text(
        json.dumps(
            {
                "sharding": {
                    "strategy": "consistent_hashing",
                    "consistent_hashing": {
                        "virtual_nodes_per_server": 50,
                        "nodes": ["shard0", "shard1"],
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SHARDING_CONFIG_PATH", str(config_path))

    config = load_sharding_config_from_env()

    assert config is not None
    assert config.strategy == "consistent_hashing"
    assert config.virtual_nodes_per_server == 50
    assert config.node_ids == ("shard0", "shard1")


def test_migration_endpoints_reject_modulo_strategy(tmp_path: Path) -> None:
    app = create_app(num_shards=3, db_dir=str(tmp_path / "db"))
    with TestClient(app) as client:
        response = client.post(
            "/admin/migrations/add-server",
            json={"new_node_id": "shard3", "apply": False},
        )
        assert response.status_code == 400
        assert "consistent_hashing strategy" in response.json()["detail"]


def test_add_server_migration_dry_run_and_apply(tmp_path: Path) -> None:
    app = create_app(
        db_dir=str(tmp_path / "db"),
        sharding_config={
            "sharding": {
                "strategy": "consistent_hashing",
                "num_shards": 4,
                "consistent_hashing": {
                    "virtual_nodes_per_server": 64,
                    "nodes": ["shard0", "shard1", "shard2"],
                },
            }
        },
    )
    with TestClient(app) as client:
        for user_id in range(150):
            create_response = client.post(
                "/users", json={"user_id": user_id, "name": f"user-{user_id}"}
            )
            assert create_response.status_code == 200

        dry_run = client.post(
            "/admin/migrations/add-server",
            json={"new_node_id": "shard3", "apply": False},
        )
        assert dry_run.status_code == 200
        dry_payload = dry_run.json()
        assert dry_payload["event"] == "add_server"
        assert dry_payload["applied"] is False
        assert dry_payload["records_planned"] > 0

        apply_response = client.post(
            "/admin/migrations/add-server",
            json={"new_node_id": "shard3", "apply": True},
        )
        assert apply_response.status_code == 200
        apply_payload = apply_response.json()
        assert apply_payload["event"] == "add_server"
        assert apply_payload["applied"] is True
        assert apply_payload["records_moved"] > 0


def test_server_failure_migration_apply(tmp_path: Path) -> None:
    app = create_app(
        db_dir=str(tmp_path / "db"),
        sharding_config={
            "sharding": {
                "strategy": "consistent_hashing",
                "num_shards": 4,
                "consistent_hashing": {
                    "virtual_nodes_per_server": 64,
                    "nodes": ["shard0", "shard1", "shard2", "shard3"],
                },
            }
        },
    )
    with TestClient(app) as client:
        for user_id in range(150):
            create_response = client.post(
                "/users", json={"user_id": user_id, "name": f"user-{user_id}"}
            )
            assert create_response.status_code == 200

        response = client.post(
            "/admin/migrations/server-failure",
            json={"failed_node_id": "shard1", "apply": True},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["event"] == "server_failure"
        assert payload["applied"] is True
        assert payload["records_moved"] > 0
