import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

NEON_URL = os.getenv("NEON_URL")

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    if not NEON_URL:
        raise RuntimeError("NEON_URL is required")
    _pool = await asyncpg.create_pool(dsn=NEON_URL, min_size=1, max_size=5)
    return _pool


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        return await init_pool()
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None
