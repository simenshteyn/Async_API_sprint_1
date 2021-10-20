import json
from functools import lru_cache
# from typing import Optional, List

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from pydantic import parse_raw_as
from pydantic.json import pydantic_encoder

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Film

FILM_CACHE_EXPIRE_IN_SECONDS = 10 #60 * 5


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    """################### 1 фильм ##################"""
    async def get_film_by_id(self, film_id: str) -> Film:
        film = await self._get_film_sorted_from_cache(film_id)
        if not film:
            film = await self._get_film_by_search_from_elastic(search_string=film_id)
            if not film:
                return None
            film = film[0]  # все что придумал
            await self._put_film_to_cache(key=film_id, film_list=list(film))
        return film

    """################### Поиск фильма ##################"""
    async def get_film_by_search(self, search_string: str) -> list[Film]:
        film_list = await self._get_film_sorted_from_cache(search_string)
        if not film_list:
            film_list = await self._get_film_by_search_from_elastic(search_string=search_string)
            if not film_list:
                return None
            await self._put_film_to_cache(key=search_string, film_list=film_list)
        return film_list

    """################### Все фильмы ##################"""
    async def get_film_sorted(self, query: dict) -> list[Film]:
        key = ''.join([str(b) for i, b in query.items()])
        film_list = await self._get_film_sorted_from_cache(key)
        if not film_list:
            film_list = await self._get_film_by_search_from_elastic(query)
            if not film_list:
                return None
            await self._put_film_to_cache(key=key, film_list=film_list)
        return film_list

    """################### Похожий фильм ##################"""
    async def get_film_alike(self, film_id: str) -> list[Film]:
        film_list = await self._get_film_sorted_from_cache('alike'+film_id)
        if not film_list:
            film_list = await self.get_film_by_id(film_id)
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
        film_list = await self.get_film_sorted(query)  # тут тоже как по мне хрень
        return film_list

    """############## Вынес сюда cache ############"""

    async def _get_film_sorted_from_cache(self, key: str) -> list[Film]:
        data = await self.redis.get(key)
        if not data:
            return None
        try:
            return parse_raw_as(list[Film], data)
        except:
            return Film.parse_raw(data)

    """put"""
    async def _put_film_to_cache(self, key: str, film_list: list[Film]):
        await self.redis.set(key, json.dumps(film_list, default=pydantic_encoder))

    """############## Вынес сюда search ############"""

    async def _get_film_by_search_from_elastic(self, query: dict = None, search_string: str = None) -> list[Film]:
        body = {'query': {"match_all" : {}}}  # просто все фильмы
        if query.get('filter_genre'):  # по жанру
            body = {"query": {"match": {"genre.id": {"query": query.get('filter_genre')}}}}
        if search_string:
            body = {'query': {"match": {search_string}}}  # по id
        doc = await self.elastic.search(index='movies',
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
