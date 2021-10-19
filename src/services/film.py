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

FILM_CACHE_EXPIRE_IN_SECONDS = 10  # 60 * 5


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
        return film

    async def _get_film_by_id_from_cache(self, film: str) -> Film:  # переименовал переменную
        data = await self.redis.get(film)
        if not data:
            return None
        film = Film.parse_raw(data)
        return film

    async def _get_film_by_id_from_elastic(self, film_id: str) -> Film:
        doc = await self.elastic.get('movies', film_id)
        return Film(**doc['_source'])

    """################### Поиск фильма ##################"""
    async def get_film_by_search(self, search_string: str) -> list[Film]:
        film_list = await self._get_film_by_id_from_cache(search_string)  # заменил
        if not film_list:
            film_list = await self._get_film_by_search_from_elastic(search_string=search_string)
            if not film_list:
                return None
        return film_list

    """################### Все фильмы ##################"""
    async def get_film_sorted(self, query: dict) -> list[Film]:
        film_list = await self._get_film_sorted_from_cache(query)
        if not film_list:
            film_list = await self._get_film_by_search_from_elastic(query, query.get('filter_genre'))
            if not film_list:
                return None
            await self._put_film_sorted_to_cache(query, film_list=film_list)  # Предлагаю заменить
        return film_list

    async def _get_film_sorted_from_cache(self, query: dict) -> list[Film]:
        data = await self.redis.get(
            f'{query.get("sort_field")}:{query.get("sort_type")}'
            f':{query.get("page_number")}:{query.get("page_size")}:{query.get("filter_genre")}'
        )  # страшная запись
        if not data:
            return None
        return parse_raw_as(list[Film], data)

    async def _put_film_sorted_to_cache(self, query, film_list: list[Film]):
        film_list_json = json.dumps(film_list, default=pydantic_encoder)
        await self.redis.set(
            f'{query.get("sort_field")}:{query.get("sort_type")}:'
            f'{query.get("page_number")}:{query.get("page_size")}:{query.get("filter_genre")}',
            film_list_json)  # просто ужас))

    """################### Похожий фильм ##################"""
    async def get_film_alike(self, film_id: str) -> list[Film]:
        film_list = await self._get_film_alike_from_cache(film_id)
        if not film_list:
            film_list = await self._get_film_alike_from_elastic(film_id)
            if not film_list:
                return None
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

    """############## Вынес сюда search ############
    ### переделать, найти как избавиться от if Серега если прочтешь это раньше чем я сделаю, можно тут 
    Сделать твой запрос через body####"""

    async def _get_film_by_search_from_elastic(self, query: dict = None, search_string: str = None) -> list[Film]:
        if search_string:
            doc = await self.elastic.search(index='movies',
                                            q = search_string,
                                            size = query.get('page_size') if query else None,
                                            from_ = query.get('page_number') * query.get('page_size') if query else None,
                                            sort = f'{query.get("sort_field")}:{query.get("sort_type")}' if query else None,
                                            )
        else:
            doc = await self.elastic.search(index='movies',
                                            size = query.get('page_size'),
                                            from_ = query.get('page_number') * query.get('page_size'),
                                            sort = f'{query.get("sort_field")}:{query.get("sort_type")}',
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
