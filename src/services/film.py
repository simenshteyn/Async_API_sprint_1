import json
from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from pydantic import parse_raw_as
from pydantic.json import pydantic_encoder

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Film
from redis_cache import RedisCache

FILM_CACHE_EXPIRE_IN_SECONDS = 10 #60 * 5


class FilmService(RedisCache):
    async def get_film_by_id(self, film_id: str) -> Film:
        film = await self._get_film_sorted_from_cache(film_id)
        if not film:
            body = {'query': {"match": {'_id': film_id}}}
            film = await self._get_film_by_search_from_elastic(body=body)
            if not film:
                return None
            await self._put_film_to_cache(key=film_id, film_list=film)
        return film[0]

    async def get_film_by_search(self, search_string: str) -> list[Film] or None:
        film_list = await self._get_film_sorted_from_cache(search_string)
        if not film_list:
            body = {'query': {"match": search_string}}
            film_list = await self._get_film_by_search_from_elastic(body=body)
            if not film_list:
                return None
            await self._put_film_to_cache(key=search_string, film_list=film_list)
        return film_list

    async def get_film_sorted(self, query: dict) -> list[Film] or None:
        key = ''.join([str(b) for i, b in query.items()])
        film_list = await self._get_film_sorted_from_cache(key)
        if not film_list:
            if query.get('filter_genre'):
                body = {'query': {"match_all": {}}}
            else:
                body = {"query": {"match": {"genre.id": {"query": query.get('filter_genre')}}}}
            film_list = await self._get_film_by_search_from_elastic(query, body)
            if not film_list:
                return None
            await self._put_film_to_cache(key=key, film_list=film_list)
        return film_list

    async def get_film_alike(self, film_id: str) -> list[Film] or None:
        film_list = await self._get_film_sorted_from_cache('alike'+film_id)
        if not film_list:

            get_film_id = await self.get_film_by_id(film_id)
            query = {
                'sort_field': 'imdb_rating',
                'sort_type': 'desc',
                'filter_genre': get_film_id.id,
                'page_number': 0,
                'page_size': 1
            }
            film_list = await self.get_film_sorted(query)
            if not film_list:
                return None
            await self._put_film_to_cache(key='alike'+film_id, film_list=film_list)

        return film_list

    async def get_popular_in_genre(self, genre_id: str,) -> list[Film]:
        query = {
            'sort_field': 'imdb_rating',
            'sort_type': 'desc',
            'filter_genre': genre_id,
            'page_number': 0,
            'page_size': 30
        }
        film_list = await self.get_film_sorted(query)
        return film_list

    async def _get_film_by_search_from_elastic(
            self, query: dict = None, body = None) -> list[Film]:
        doc = await self.elastic.search(
            index='movies',
            body = body,
            size = query.get('page_size') if query else None,
            from_ = query.get('page_number') * query.get('page_size') if query else None,
            sort = f'{query.get("sort_field")}:{query.get("sort_type")}' if query else None,
        )
        result = []
        for movie in doc['hits']['hits']:
            result.append(Film(**movie['_source']))
        return result




@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)) -> FilmService:
    return FilmService(redis, elastic)
