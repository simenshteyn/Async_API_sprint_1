from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Genre
from services.base import BaseService

GENRE_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class GenreService(BaseService):
    es_index = 'genre'
    model = Genre

    async def get_by_id(self, genre_id: str) -> Genre:
        return await self._get_by_id(genre_id, GENRE_CACHE_EXPIRE_IN_SECONDS)

    async def get_genre_list(
            self, page_number: int, page_size: int) -> list[Genre]:
        return await self._get_list(page_number, page_size,
                                    GENRE_CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
