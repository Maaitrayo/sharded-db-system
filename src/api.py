from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

from src.config import ShardingConfig
from src.migration_manager import MigrationManager
from src.models import (
    AddServerMigrationRequest,
    CreateUserResponse,
    ServerFailureMigrationRequest,
    UserCreate,
    UserResponse,
)
from src.router import ConsistentHashRouter
from src.router_factory import RouterFactory
from src.shard_manager import ShardManager

load_dotenv()


def _resolve_sharding_config(
    num_shards: int, sharding_config: ShardingConfig | dict[str, Any] | None
) -> ShardingConfig:
    if sharding_config is None:
        return ShardingConfig(strategy="modulo", num_shards=num_shards)
    if isinstance(sharding_config, ShardingConfig):
        return sharding_config
    return ShardingConfig.from_dict(sharding_config)


def load_sharding_config_from_env() -> ShardingConfig | None:
    path = os.getenv("SHARDING_CONFIG_PATH")
    if not path:
        return None

    with open(path, "r", encoding="utf-8") as file_handle:
        raw = json.load(file_handle)
    return ShardingConfig.from_dict(raw)


def create_app(
    num_shards: int = 3,
    db_dir: str = "src/db",
    sharding_config: ShardingConfig | dict[str, Any] | None = None,
) -> FastAPI:
    config = _resolve_sharding_config(
        num_shards=num_shards, sharding_config=sharding_config
    )
    print(f"Using sharding config: {config.strategy} with {config.num_shards} shards")
    shard_manager = ShardManager(num_shards=config.num_shards, db_dir=db_dir)
    router = RouterFactory.build(config=config, shard_manager=shard_manager)
    migration_manager: MigrationManager | None = None
    if config.strategy == "consistent_hashing":
        migration_manager = MigrationManager(
            shard_manager=shard_manager,
            active_nodes=list(config.node_ids),
            virtual_nodes_per_server=config.virtual_nodes_per_server,
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        shard_manager.initialize_shards()
        yield

    app = FastAPI(title="Sharded DB System", lifespan=lifespan)

    @app.post("/users", response_model=CreateUserResponse)
    def create_user(payload: UserCreate) -> CreateUserResponse:
        router.create_user(user_id=payload.user_id, name=payload.name)
        return CreateUserResponse(
            message="user stored",
            user_id=payload.user_id,
            shard_index=router.get_shard_index(payload.user_id),
        )

    @app.get("/users/{user_id}", response_model=UserResponse)
    def get_user(user_id: int) -> UserResponse:
        user = router.get_user(user_id=user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="user not found")
        return UserResponse(**user)

    @app.post("/admin/migrations/add-server")
    def migrate_add_server(payload: AddServerMigrationRequest) -> dict[str, Any]:
        if migration_manager is None or not isinstance(router, ConsistentHashRouter):
            raise HTTPException(
                status_code=400,
                detail="migration endpoints require consistent_hashing strategy",
            )
        try:
            result = migration_manager.simulate_add_server(
                new_node_id=payload.new_node_id, apply=payload.apply
            )
            if payload.apply:
                router.refresh_nodes(migration_manager.active_nodes)
            return result
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/admin/migrations/server-failure")
    def migrate_server_failure(
        payload: ServerFailureMigrationRequest,
    ) -> dict[str, Any]:
        if migration_manager is None or not isinstance(router, ConsistentHashRouter):
            raise HTTPException(
                status_code=400,
                detail="migration endpoints require consistent_hashing strategy",
            )
        try:
            result = migration_manager.simulate_server_failure(
                failed_node_id=payload.failed_node_id, apply=payload.apply
            )
            if payload.apply:
                router.refresh_nodes(migration_manager.active_nodes)
            return result
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app


app = create_app(sharding_config=load_sharding_config_from_env())
