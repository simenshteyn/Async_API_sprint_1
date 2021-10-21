from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Person
from services.base import BaseService

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class PersonService(BaseService):
    es_index = 'person'
    model = Person

    async def get_by_id(self, person_id: str) -> Person:
        return await self._get_by_id(person_id, PERSON_CACHE_EXPIRE_IN_SECONDS)

    async def get_person_list(
            self, page_number: int, page_size: int) -> list[Person]:
        return await self._get_list(page_number, page_size,
                                    PERSON_CACHE_EXPIRE_IN_SECONDS)

    async def get_by_search(self, search_string: str) -> list[Person]:
        return await self._get_by_search(search_string, 'full_name',
                                         PERSON_CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
