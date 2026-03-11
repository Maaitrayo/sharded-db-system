from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from src.models import CreateUserResponse, UserCreate, UserResponse
from src.router import ShardRouter
from src.shard_manager import ShardManager


def create_app(num_shards: int = 3, db_dir: str = "src/db") -> FastAPI:
    shard_manager = ShardManager(num_shards=num_shards, db_dir=db_dir)
    router = ShardRouter(shard_manager=shard_manager)

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

    return app


app = create_app()
