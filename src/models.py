from __future__ import annotations

from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    user_id: int = Field(..., ge=0)
    name: str = Field(..., min_length=1, max_length=255)


class UserResponse(BaseModel):
    user_id: int
    name: str


class CreateUserResponse(BaseModel):
    message: str
    user_id: int
    shard_index: int


class AddServerMigrationRequest(BaseModel):
    new_node_id: str = Field(..., min_length=1, max_length=128)
    apply: bool = False


class ServerFailureMigrationRequest(BaseModel):
    failed_node_id: str = Field(..., min_length=1, max_length=128)
    apply: bool = False
