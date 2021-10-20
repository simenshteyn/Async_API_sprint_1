from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Film
from .redis_cache import RedisCache


class FilmService(RedisCache):

    async def get_film(self, key: str, query: dict = None, body: dict = None) -> Film or list[Film] or None:
        film = await self._get_film_sorted_from_cache(key)
        if not film:
            if not body:
                body = {'query': {"match_all": {}}}
            film = await self._get_film_by_search_from_elastic(query=query, body=body)
            if not film:
                return None
            await self._put_film_to_cache(key=key, film_list=film)
        return film

    async def get_film_alike(self, film_id: str, key: str) -> list[Film] or None:
        film_list = await self._get_film_sorted_from_cache(key)
        if not film_list:
            body = {'query': {"match": {'_id': film_id}}}
            get_films = await self.get_film(key=film_id, body=body)
            film = get_films[0]
            film_list = []
            for genre in film.genre:
                query = {
                    'sort_field': 'imdb_rating',
                    'sort_type': 'desc',
                    'page_number': 0,
                    'page_size': 10
                }
                body = {"query": {"match": {"genre.id": {"query": genre['id']}}}}
                alike_films = await self._get_film_by_search_from_elastic(query=query, body=body)
                if alike_films:
                    film_list.extend(alike_films)
            await self._put_film_to_cache(key=key, film_list=list(film_list))

        return film_list

    async def _get_film_by_search_from_elastic(self, query: dict = None, body: dict = None) -> list[Film]:
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
