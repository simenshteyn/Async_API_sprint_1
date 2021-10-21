import json
from typing import Optional

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel, parse_raw_as
from pydantic.json import pydantic_encoder


class BaseService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def _get_by_id(
            self, id: str, cache_expire: int, model: BaseModel, es_index: str
    ) -> Optional[BaseModel]:
        some_obj = await self._get_by_id_from_cache(id, model)
        if not some_obj:
            some_obj = await self._get_by_id_from_elastic(id, model, es_index)
            if not some_obj:
                return None
            await self._put_by_id_to_cache(some_obj, cache_expire)
        return some_obj

    async def _get_by_id_from_cache(
            self, id: str, model: BaseModel) -> Optional[BaseModel]:
        data = await self.redis.get(id)
        if not data:
            return None
        return model.parse_raw(data)

    async def _get_by_id_from_elastic(
            self, id: str, model: BaseModel, es_index: str) -> BaseModel:
        doc = await self.elastic.get(es_index, id)
        return model(**doc['_source'])

    async def _put_by_id_to_cache(self, model: BaseModel, expire: int):
        await self.redis.set(model.id, model.json(), expire=expire)

    async def _get_by_search(self, search_string: str, search_field: str,
                             expire: int, es_index: str, model: BaseModel
                             ) -> Optional[list[BaseModel]]:
        obj_list = await self._get_by_search_from_cache(
            es_index, search_string, model)
        if not obj_list:
            obj_list = await self._get_by_search_from_elastic(
                search_string, search_field, es_index, model)
            if not obj_list:
                return None
            await self._put_by_search_to_cache(es_index,
                                               search_string,
                                               obj_list,
                                               expire)
        return obj_list

    async def _get_by_search_from_cache(
            self, prefix: str, search_string: str, model: BaseModel
    ) -> Optional[list[BaseModel]]:
        data = await self.redis.get(f'{prefix}:{search_string}')
        if not data:
            return None
        return parse_raw_as(list[model], data)

    async def _get_by_search_from_elastic(
            self, search_string: str, search_field: str,
            es_index: str, model: BaseModel) -> list[BaseModel]:
        doc = await self.elastic.search(
            index=es_index,
            body={"query": {
                "match": {
                    search_field: {
                        "query": search_string,
                        "fuzziness": "auto"
                    }
                }
            }})
        return [model(**d['_source']) for d in doc['hits']['hits']]

    async def _put_by_search_to_cache(
            self, prefix: str, search_string: str, model_list: list[BaseModel],
            expire: int):
        list_json = json.dumps(model_list, default=pydantic_encoder)
        await self.redis.set(f'{prefix}:{search_string}', list_json,
                             expire=expire)

    async def _get_list(
            self, page_number: int, page_size: int,
            expire: int, es_index: str, model: BaseModel
    ) -> Optional[list[BaseModel]]:
        obj_list = await self._get_list_from_cache(
            page_number, page_size, es_index, model)
        if not obj_list:
            obj_list = await self._get_list_from_elastic(page_number,
                                                         page_size,
                                                         es_index,
                                                         model)
            if not obj_list:
                return None
            await self._put_list_to_cache(
                page_number, page_size, es_index, obj_list, expire)
        return obj_list

    async def _get_list_from_cache(
            self, page_number: int, page_size: int, prefix: str, model: BaseModel
    ) -> Optional[list[BaseModel]]:
        data = await self.redis.get(f'{prefix}:{page_number}:{page_size}')
        if not data:
            return None
        return parse_raw_as(list[model], data)

    async def _get_list_from_elastic(
            self, page_number: int, page_size: int, es_index: str,
            model: BaseModel, query: dict = None) -> list[BaseModel]:
        body = {"from": page_number * page_size, "size": page_size}
        if query:
            body = body | query
        docs = await self.elastic.search(
            index=es_index,
            body=body
        )
        return [model(**d['_source']) for d in docs['hits']['hits']]

    async def _put_list_to_cache(
            self, page_number: int, page_size: int, prefix: str,
            model_list: list[BaseModel], expire: int):
        list_json = json.dumps(model_list, default=pydantic_encoder)
        await self.redis.set(f'{prefix}:{page_number}:{page_size}', list_json,
                             expire=expire)
