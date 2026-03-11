from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from src.api import create_app


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
