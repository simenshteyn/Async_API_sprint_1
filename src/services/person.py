import json
from functools import lru_cache
from typing import Optional, List

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from pydantic import parse_raw_as
from pydantic.json import pydantic_encoder

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Person

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class PersonService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, person_id: str) -> Optional[Person]:
        person = await self._person_from_cache(person_id)
        if not person:
            person = await self._get_person_from_elastic(person_id)
            if not person:
                return None
            await self._put_person_to_cache(person)
        return person

    async def _get_person_from_elastic(self, person_id: str) -> Optional[
        Person]:
        doc = await self.elastic.get('person', person_id)
        return Person(**doc['_source'])

    async def _person_from_cache(self, person_id: str) -> Optional[Person]:
        data = await self.redis.get(person_id)
        if not data:
            return None
        person = Person.parse_raw(data)
        return person

    async def _put_person_to_cache(self, person: Person):
        await self.redis.set(person.id, person.json(),
                             expire=PERSON_CACHE_EXPIRE_IN_SECONDS)

    async def get_person_list(self, page_number: int, page_size: int) -> \
    Optional[List[Person]]:
        person_list = await self._person_list_from_cache(
            page_number=page_number,
            page_size=page_size)
        if not person_list:
            person_list = await self._person_list_from_elastic(
                page_number=page_number,
                page_size=page_size
            )
            if not person_list:
                return None
            await self._put_person_list_to_cache(
                page_number=page_number,
                page_size=page_size,
                person_list=person_list
            )
        return person_list

    async def _person_list_from_elastic(self,
                                        page_number: int,
                                        page_size: int) -> Optional[
        List[Person]]:
        docs = await self.elastic.search(
            index='person',
            body={"from": page_number * page_size,
                  "size": page_size}
        )
        result = []
        for person in docs['hits']['hits']:
            result.append(Person(**person['_source']))
        return result

    async def _person_list_from_cache(self,
                                      page_number: int,
                                      page_size: int) -> Optional[
        List[Person]]:
        data = await self.redis.get(f'person_list:{page_number}:{page_size}')
        if not data:
            return None
        return parse_raw_as(List[Person], data)

    async def _put_person_list_to_cache(self, page_number: int, page_size: int,
                                        person_list: List[Person]):
        person_list_json = json.dumps(person_list, default=pydantic_encoder)
        await self.redis.set(f'person_list:{page_number}:{page_size}',
                             person_list_json)


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
