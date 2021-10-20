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

    async def get_by_id(self, person_id: str) -> Person:
        person = await self._get_by_id_from_cache(person_id, Person)
        if not person:
            person = await self._get_by_id_from_elastic(person_id,
                                                        'person', Person)
            if not person:
                return None
            await self._put_by_id_to_cache(person,
                                           PERSON_CACHE_EXPIRE_IN_SECONDS)
        return person

    async def get_person_list(self, page_number: int, page_size: int) -> \
            list[Person]:
        person_list = await self._get_list_from_cache(page_number, page_size,
                                                      'persons', Person)
        if not person_list:
            person_list = await self._get_list_from_elastic(page_number,
                                                            page_size,
                                                            'person',
                                                            Person)
            if not person_list:
                return None
            await self._put_list_to_cache(page_number, page_size, 'persons',
                                          person_list,
                                          PERSON_CACHE_EXPIRE_IN_SECONDS)
        return person_list

    async def get_by_search(self, search_string: str) -> list[Person]:
        person_list = await self._get_by_search_from_cache('person',
                                                           search_string,
                                                           Person)
        if not person_list:
            person_list = await self._get_by_search_from_elastic('person',
                                                                 search_string,
                                                                 'full_name',
                                                                 Person)
            if not person_list:
                return None
            await self._put_by_search_to_cache('person', search_string,
                                               person_list,
                                               PERSON_CACHE_EXPIRE_IN_SECONDS)
        return person_list


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
