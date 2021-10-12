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
from models.models import Film

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> Optional[Film]:
        film = await self._film_from_cache(film_id)
        if not film:
            film = await self._get_film_from_elastic(film_id)
            if not film:
                return None
            await self._put_film_to_cache(film)
        return film

    async def get_by_search(self, search_string: str) -> Optional[List[Film]]:
        film_list = await self._film_search_from_cache(search_string)
        if not film_list:
            film_list = await self._search_film_from_elastic(search_string)
            if not film_list:
                return None
            await self._put_film_search_to_cache(search_string, film_list)
        return film_list

    async def get_sorted_by_field(self, sort_field: str,
                                  sort_type: str = "desc"
                                  ) -> Optional[List[Film]]:
        film_list = await self._sorted_film_search_from_elastic(sort_field,
                                                                sort_type)
        if not film_list:
            return None
        return film_list


    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:
        doc = await self.elastic.get('movies', film_id)
        return Film(**doc['_source'])

    async def _search_film_from_elastic(self, search_string: str) -> Optional[
        List[Film]]:
        doc = await self.elastic.search(
            index='movies',
            body={"query": {
                "match": {
                    "title": {
                        "query": search_string,
                        "fuzziness": "auto"
                    }
                }
            }})
        result = []
        for movie in doc['hits']['hits']:
            result.append(Film(**movie['_source']))
        return result

    async def _sorted_film_search_from_elastic(self,
                                               sort_field: str,
                                               sort_type: str = "desc"
                                               ) -> Optional[List[Film]]:
        docs = await self.elastic.search(
            index='movies',
            body={"sort": {sort_field: sort_type}}
        )
        result = []
        for movie in docs['hits']['hits']:
            result.append(Film(**movie['_source']))
        return result

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        data = await self.redis.get(film_id)
        if not data:
            return None
        film = Film.parse_raw(data)
        return film

    async def _film_search_from_cache(self, search_string: str) -> Optional[
        List[Film]]:
        data = await self.redis.get(search_string)
        if not data:
            return None
        film_list = parse_raw_as(List[Film], data)
        return film_list

    async def _put_film_to_cache(self, film: Film):
        await self.redis.set(film.id, film.json(),
                             expire=FILM_CACHE_EXPIRE_IN_SECONDS)

    async def _put_film_search_to_cache(self,
                                        search_string: str,
                                        film_list: List[Film]):
        film_list_json = json.dumps(film_list, default=pydantic_encoder)
        await self.redis.set(search_string, film_list_json)


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
