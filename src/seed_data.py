from __future__ import annotations

import os
import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

from src.config import ShardingConfig
from src.router_factory import RouterFactory
from src.shard_manager import ShardManager

load_dotenv()


def _load_config(config_path: str | None, num_shards: int) -> ShardingConfig:
    if config_path is None:
        return ShardingConfig(strategy="modulo", num_shards=num_shards)

    with open(config_path, "r", encoding="utf-8") as file_handle:
        raw = json.load(file_handle)
    return ShardingConfig.from_dict(raw)


def populate_db(
    count: int,
    start_id: int = 1,
    db_dir: str = "src/db",
    config_path: str | None = None,
    name_prefix: str = "User",
    num_shards: int = 3,
) -> dict[str, int]:
    if count <= 0:
        raise ValueError("count must be greater than 0")
    if start_id < 0:
        raise ValueError("start_id must be >= 0")

    config = _load_config(config_path=config_path, num_shards=num_shards)
    shard_manager = ShardManager(num_shards=config.num_shards, db_dir=db_dir)
    shard_manager.initialize_shards()
    router = RouterFactory.build(config=config, shard_manager=shard_manager)

    shard_counts: dict[str, int] = {}
    for offset in range(count):
        user_id = start_id + offset
        name = f"{name_prefix}-{user_id}"
        router.create_user(user_id=user_id, name=name)
        shard_index = router.get_shard_index(user_id)
        key = f"shard{shard_index}"
        shard_counts[key] = shard_counts.get(key, 0) + 1

    return shard_counts


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate shard databases with synthetic users."
    )
    parser.add_argument("--count", type=int, default=100, help="Number of users to add.")
    parser.add_argument("--start-id", type=int, default=1, help="First user_id to use.")
    parser.add_argument("--db-dir", type=str, default="src/db", help="Shard DB directory.")
    parser.add_argument(
        "--name-prefix",
        type=str,
        default="User",
        help="Prefix used for generated user names.",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=3,
        help="Shard count used when config-path is not provided.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    distribution = populate_db(
        count=args.count,
        start_id=args.start_id,
        db_dir=args.db_dir,
        config_path=os.getenv("SHARDING_CONFIG_PATH"),
        name_prefix=args.name_prefix,
        num_shards=args.num_shards,
    )

    print(f"Inserted {args.count} users")
    print(f"DB directory: {Path(args.db_dir).resolve()}")
    print("Distribution by shard:")
    for shard_name in sorted(distribution):
        print(f"- {shard_name}: {distribution[shard_name]}")


if __name__ == "__main__":
    main()
