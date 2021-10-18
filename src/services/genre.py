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
from models.models import Genre

GENRE_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_genre_by_id(self, genre_id: str) -> Optional[Genre]:
        genre = await self._get_genre_by_id_from_cache(genre_id)
        if not genre:
            genre = await self._get_genre_by_id_from_elastic(genre_id)
            if not genre:
                return None
            await self._put_genre_by_id_to_cache(genre)
        return genre

    async def _get_genre_by_id_from_cache(self,
                                          genre_id: str) -> Optional[Genre]:
        data = await self.redis.get(genre_id)
        if not data:
            return None
        genre = Genre.parse_raw(data)
        return genre

    async def _get_genre_by_id_from_elastic(self,
                                            genre_id: str) -> Optional[Genre]:
        doc = await self.elastic.get('genre', genre_id)
        return Genre(**doc['_source'])

    async def _put_genre_by_id_to_cache(self, genre: Genre):
        await self.redis.set(genre.id, genre.json(),
                             expire=GENRE_CACHE_EXPIRE_IN_SECONDS)

    async def get_genre_list(self, page_number: int,
                             page_size: int) -> Optional[List[Genre]]:
        genre_list = await self._get_genre_list_from_cache(
            page_number=page_number,
            page_size=page_size)
        if not genre_list:
            genre_list = await self._get_genre_list_from_elastic(
                page_number=page_number,
                page_size=page_size
            )
            if not genre_list:
                return None
            await self._put_genre_list_to_cache(
                page_number=page_number,
                page_size=page_size,
                genre_list=genre_list
            )
        return genre_list

    async def _get_genre_list_from_cache(self,
                                         page_number: int,
                                         page_size: int) -> Optional[
        List[Genre]]:
        data = await self.redis.get(f'genre_list:{page_number}:{page_size}')
        if not data:
            return None
        return parse_raw_as(List[Genre], data)

    async def _get_genre_list_from_elastic(self,
                                           page_number: int,
                                           page_size: int) -> Optional[
        List[Genre]]:
        docs = await self.elastic.search(
            index='genre',
            body={"from": page_number * page_size,
                  "size": page_size}
        )
        result = []
        for genre in docs['hits']['hits']:
            if 'name' in genre['_source']:
                result.append(Genre(**genre['_source']))
        return result

    async def _put_genre_list_to_cache(self, page_number: int, page_size: int,
                                       genre_list: List[Genre]):
        genre_list_json = json.dumps(genre_list, default=pydantic_encoder)
        await self.redis.set(f'genre_list:{page_number}:{page_size}',
                             genre_list_json)


@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
