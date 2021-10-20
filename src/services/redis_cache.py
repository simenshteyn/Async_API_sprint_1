import json
from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from pydantic import parse_raw_as
from pydantic.json import pydantic_encoder

from models.models import Film

FILM_CACHE_EXPIRE_IN_SECONDS = 10 #60 * 5


class RedisCache:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def _get_film_sorted_from_cache(self, key: str) -> list[Film]:
        data = await self.redis.get(key)
        if not data:
            return None
        try:
            return parse_raw_as(list[Film], data)
        except:
            return Film.parse_raw(data)

    async def _put_film_to_cache(self, key: str, film_list: list[Film]):
        await self.redis.set(key, json.dumps(film_list, default=pydantic_encoder), expire=FILM_CACHE_EXPIRE_IN_SECONDS)
