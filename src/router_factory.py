from __future__ import annotations

from src.config import ShardingConfig
from src.router import ConsistentHashRouter, ModuloShardRouter
from src.shard_manager import ShardManager


class RouterFactory:
    @staticmethod
    def build(
        config: ShardingConfig, shard_manager: ShardManager
    ) -> ModuloShardRouter | ConsistentHashRouter:
        if config.strategy == "modulo":
            return ModuloShardRouter(shard_manager=shard_manager)
        return ConsistentHashRouter(shard_manager=shard_manager, config=config)
