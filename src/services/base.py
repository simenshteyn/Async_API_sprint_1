import json

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel, parse_raw_as
from pydantic.json import pydantic_encoder


class BaseService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch,
                 es_index: str, model: BaseModel):
        self.redis = redis
        self.elastic = elastic
        self.es_index = es_index
        self.model = model

    async def _get_by_id(
            self, id: str, cach_expire: int) -> BaseModel:
        obj = await self._get_by_id_from_cache(id)
        if not obj:
            obj = await self._get_by_id_from_elastic(id)
            if not obj:
                return None
            await self._put_by_id_to_cache(obj, cach_expire)
        return obj

    async def _get_by_id_from_cache(
            self, id: str) -> BaseModel:
        data = await self.redis.get(id)
        if not data:
            return None
        return self.model.parse_raw(data)

    async def _get_by_id_from_elastic(
            self, id: str) -> BaseModel:
        doc = await self.elastic.get(self.es_index, id)
        return self.model(**doc['_source'])

    async def _put_by_id_to_cache(self, model: BaseModel, expire: int):
        await self.redis.set(model.id, model.json(), expire=expire)

    async def _get_by_search(self, search_string: str, search_field: str, expire: int) -> list[BaseModel]:
        obj_list = await self._get_by_search_from_cache(
            self.es_index, search_string, self.model)
        if not obj_list:
            obj_list = await self._get_by_search_from_elastic(
                search_string, search_field)
            if not obj_list:
                return None
            await self._put_by_search_to_cache(self.es_index,
                                               search_string,
                                               obj_list,
                                               expire)
        return obj_list

    async def _get_by_search_from_cache(
            self, prefix: str, search_string: str, model: BaseModel
    ) -> list[BaseModel]:
        data = await self.redis.get(f'{prefix}:{search_string}')
        if not data:
            return None
        return parse_raw_as(list[model], data)

    async def _get_by_search_from_elastic(
            self, search_string: str, search_field: str) -> list[BaseModel]:
        doc = await self.elastic.search(
            index=self.es_index,
            body={"query": {
                "match": {
                    search_field: {
                        "query": search_string,
                        "fuzziness": "auto"
                    }
                }
            }})
        result = []
        for d in doc['hits']['hits']:
            result.append(self.model(**d['_source']))
        return result

    async def _put_by_search_to_cache(
            self, prefix: str, search_string: str, model_list: list[BaseModel],
            expire: int):
        list_json = json.dumps(model_list, default=pydantic_encoder)
        await self.redis.set(f'{prefix}:{search_string}', list_json,
                             expire=expire)
    async def _get_list(
            self, page_number: int, page_size: int, expire: int
    ) -> list[BaseModel]:
        obj_list = await self._get_list_from_cache(
            page_number, page_size, self.es_index)
        if not obj_list:
            obj_list = await self._get_list_from_elastic(page_number,
                                                         page_size)
            if not obj_list:
                return None
            await self._put_list_to_cache(
                page_number, page_size, self.es_index, obj_list, expire)
        return obj_list


    async def _get_list_from_cache(
            self, page_number: int, page_size: int, prefix: str
    ) -> list[BaseModel]:
        data = await self.redis.get(f'{prefix}:{page_number}:{page_size}')
        if not data:
            return None
        return parse_raw_as(list[self.model], data)

    async def _get_list_from_elastic(
            self, page_number: int, page_size: int, query: dict = None) -> list[BaseModel]:
        body = {"from": page_number * page_size, "size": page_size}
        if query:
            body = body | query
        docs = await self.elastic.search(
            index=self.es_index,
            body=body
        )
        result = []
        for d in docs['hits']['hits']:
            result.append(self.model(**d['_source']))
        return result

    async def _put_list_to_cache(
            self, page_number: int, page_size: int, prefix: str,
            model_list: list[BaseModel], expire: int):
        list_json = json.dumps(model_list, default=pydantic_encoder)
        await self.redis.set(f'{prefix}:{page_number}:{page_size}', list_json,
                             expire=expire)
