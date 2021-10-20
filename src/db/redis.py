from aioredis import Redis

redis: Redis or None


async def get_redis() -> Redis:
    return redis
