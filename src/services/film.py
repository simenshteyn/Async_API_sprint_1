from functools import lru_cache
from typing import Optional, List

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Film
from services.base import BaseService

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class FilmService(BaseService):

    async def get_film_by_id(self, film_id: str) -> Optional[Film]:
        film = await self._get_by_id_from_cache(film_id, Film)
        if not film:
            film = await self._get_by_id_from_elastic(film_id, 'movies', Film)
            if not film:
                return None
            await self._put_by_id_to_cache(film, FILM_CACHE_EXPIRE_IN_SECONDS)
        return film

    async def get_film_by_search(self,
                                 search_string: str) -> Optional[List[Film]]:
        film_list = await self._get_by_search_from_cache('film',
                                                         search_string, Film)
        if not film_list:
            film_list = await self._get_by_search_from_elastic(
                'movies', search_string, 'title', Film)
            if not film_list:
                return None
            await self._put_by_search_to_cache('film',
                                               search_string,
                                               film_list,
                                               FILM_CACHE_EXPIRE_IN_SECONDS)
        return film_list

    async def get_film_sorted(self, sort_field: str, sort_type: str,
                              filter_genre: str, page_number: int,
                              page_size: int) -> Optional[List[Film]]:
        query = {"sort": {sort_field: sort_type}}
        if filter_genre:
            query = query | {
                "query": {"match": {"genre.id": {"query": filter_genre}}}}
        film_list = await self._get_list_from_cache(
            page_number,
            page_size,
            f'{sort_field}:{sort_type}:{filter_genre}:films',
            Film
        )
        if not film_list:
            film_list = await self._get_list_from_elastic(page_number,
                                                          page_size, 'movies',
                                                          Film, query=query)
            if not film_list:
                return None
            await self._put_list_to_cache(
                page_number,
                page_size,
                f'{sort_field}:{sort_type}:{filter_genre}:films',
                film_list,
                FILM_CACHE_EXPIRE_IN_SECONDS
            )
        return film_list

    async def get_film_alike(self, film_id: str) -> Optional[List[Film]]:
        film_list = await self._get_list_from_cache(page_number=-1,
                                                    page_size=-1,
                                                    prefix=f'alike:{film_id}',
                                                    model=Film)
        if not film_list:
            film_list = await self._get_film_alike_from_elastic(film_id)
            if not film_list:
                return None
            await self._put_list_to_cache(page_number=-1, page_size=-1,
                                          prefix=f'alike:{film_id}',
                                          model_list=film_list,
                                          expire=FILM_CACHE_EXPIRE_IN_SECONDS)
        return film_list

    async def _get_film_alike_from_elastic(self, film_id: str
                                           ) -> Optional[List[Film]]:
        film = await self.get_film_by_id(film_id)
        if not film or not film.genre:
            return None
        result = []
        for genre in film.genre:
            alike_films = await self.get_film_sorted(
                sort_field='imdb_rating',
                sort_type='desc',
                filter_genre=genre['id'],
                page_number=0,
                page_size=10)
            if alike_films:
                result.extend(alike_films)
        return result

    async def get_popular_in_genre(self, genre_id: str, ) -> Optional[
        List[Film]]:
        film_list = await self.get_film_sorted(sort_field='imdb_rating',
                                               sort_type='desc',
                                               filter_genre=genre_id,
                                               page_number=0,
                                               page_size=30)
        return film_list


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)) -> FilmService:
    return FilmService(redis, elastic)
