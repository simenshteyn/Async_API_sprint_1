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

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    """################### 1 фильм ##################"""
    async def get_film_by_id(self, film_id: str) -> Film:
        film = await self._get_film_by_id_from_cache(film_id)
        if not film:
            film = await self._get_film_by_id_from_elastic(film_id)
            if not film:
                return None
#            await self._put_film_by_id_to_cache(film)  # Предлагаю удалить у всех кроме all films
        return film

    async def _get_film_by_id_from_cache(self, film_id: str) -> Film:
        data = await self.redis.get(film_id)
        if not data:
            return None
        film = Film.parse_raw(data)
        return film

    async def _get_film_by_id_from_elastic(self,
                                           film_id: str) -> Film:
        doc = await self.elastic.get('movies', film_id)
        return Film(**doc['_source'])

    # async def _put_film_by_id_to_cache(self, film: Film):
    #     await self.redis.set(film.id, film.json(),
    #                          expire=FILM_CACHE_EXPIRE_IN_SECONDS)

    """################### Поиск фильма ##################"""
    async def get_film_by_search(self,
                                 search_string: str) -> list[Film]:
        film_list = await self._get_film_by_search_from_cache(search_string)
        if not film_list:
            film_list = await self._get_film_by_search_from_elastic(search_string)
            if not film_list:
                return None
            # await self._put_film_by_search_to_cache(search_string, film_list)
        return film_list

    async def _get_film_by_search_from_cache(self, search_string: str) -> list[Film]:
        data = await self.redis.get(search_string)
        if not data:
            return None
        return parse_raw_as(list[Film], data)

    async def _get_film_by_search_from_elastic(
            self,
            search_string: str) -> list[Film]:
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

    # async def _put_film_by_search_to_cache(self,
    #                                        search_string: str,
    #                                        film_list: list[Film]):
    #     film_list_json = json.dumps(film_list, default=pydantic_encoder)
    #     await self.redis.set(search_string, film_list_json)

    """################### Все фильмы ##################"""
    async def get_film_sorted(self, query: dict) -> list[Film]:
        film_list = await self._get_film_sorted_from_cache(query)
        if not film_list:
            film_list = await self._get_film_sorted_from_elastic(query)
            if not film_list:
                return None
            await self._put_film_sorted_to_cache(query, film_list=film_list
            )
        return film_list

    async def _get_film_sorted_from_cache(self, query: dict) -> list[Film]:
        data = await self.redis.get(
            f'{query.sort_field}:{query.sort_type}:{query.page_number}:{query.page_size}:{query.filter_genre}'
        )
        if not data:
            return None
        return parse_raw_as(list[Film], data)

    async def _get_film_sorted_from_elastic(self, query) -> list[Film]:
        body = {"sort": {query.sort_field: query.sort_type},
                "from": query.page_number * query.page_size,
                "size": query.page_size}
        if query.filter_genre:
            body = body | {
                "query": {"match": {"genre.id": {"query": query.filter_genre}}}}
        docs = await self.elastic.search(
            index='movies',
            body=body
        )
        result = []
        for movie in docs['hits']['hits']:
            result.append(Film(**movie['_source']))
        return result

    # async def _put_film_sorted_to_cache(self,
    #                                     sort_field: str,
    #                                     sort_type: str,
    #                                     filter_genre: str,
    #                                     page_number: int,
    #                                     page_size: int,
    #                                     film_list: list[Film]):
    #     film_list_json = json.dumps(film_list, default=pydantic_encoder)
    #     await self.redis.set(
    #         f'{sort_field}:{sort_type}:{page_number}:{page_size}:{filter_genre}',
    #         film_list_json)

    """################### Похожий фильм ##################"""
    async def get_film_alike(self, film_id: str) -> list[Film]:
        film_list = await self._get_film_alike_from_cache(film_id)
        if not film_list:
            film_list = await self._get_film_alike_from_elastic(film_id)
            if not film_list:
                return None
            # await self._put_film_alike_to_cache(film_id, film_list)
        return film_list

    async def _get_film_alike_from_cache(self, film_id: str) -> list[Film]:
        data = await self.redis.get(f'alike:{film_id}')
        if not data:
            return None
        return parse_raw_as(list[Film], data)

    async def _get_film_alike_from_elastic(self, film_id: str) -> list[Film]:
        film = await self.get_film_by_id(film_id)
        if not film or not film.genre:
            return None
        result = []
        for genre in film.genre:
            query = {
                'sort_field': 'imdb_rating',
                'sort_type': 'desc',
                'filter_genre': genre['id'],
                'page_number': 0,
                'page_size': 10
            }
            alike_films = await self.get_film_sorted(query)  # Тут переделал и еще бы переделать так как хрень какая-то
            if alike_films:
                result.extend(alike_films)
        return result

    # async def _put_film_alike_to_cache(self,
    #                                    film_id: str,
    #                                    film_list: list[Film]):
    #     film_list_json = json.dumps(film_list, default=pydantic_encoder)
    #     await self.redis.set(f'alike:{film_id}', film_list_json)

    async def get_popular_in_genre(self, genre_id: str, ) -> list[Film]:
        query = {
            'sort_field': 'imdb_rating',
            'sort_type': 'desc',
            'filter_genre': genre_id,
            'page_number': 0,
            'page_size': 30
        }
        film_list = await self.get_film_sorted(query)  # тут тоже как по мне хрень
        return film_list


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)) -> FilmService:
    return FilmService(redis, elastic)
