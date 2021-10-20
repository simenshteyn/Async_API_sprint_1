import json
from typing import Optional, List

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel, parse_raw_as
from pydantic.json import pydantic_encoder


class BaseService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def _get_by_id_from_cache(self, id: str,
                                    model: BaseModel) -> Optional[BaseModel]:
        data = await self.redis.get(id)
        if not data:
            return None
        return model.parse_raw(data)

    async def _get_by_id_from_elastic(self, id: str,
                                      es_index: str,
                                      model: BaseModel) -> Optional[BaseModel]:
        doc = await self.elastic.get(es_index, id)
        return model(**doc['_source'])

    async def _put_by_id_to_cache(self, model: BaseModel, expire: int):
        await self.redis.set(model.id, model.json(), expire=expire)

    async def _get_by_search_from_cache(self, prefix: str, search_string: str,
                                        model: BaseModel) -> Optional[
        List[BaseModel]]:
        data = await self.redis.get(f'{prefix}:{search_string}')
        if not data:
            return None
        return parse_raw_as(List[model], data)

    async def _get_by_search_from_elastic(
            self,
            es_index: str,
            search_string: str,
            search_field: str,
            model: BaseModel) -> Optional[List[BaseModel]]:
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
        result = []
        for d in doc['hits']['hits']:
            result.append(model(**d['_source']))
        return result

    async def _put_by_search_to_cache(self,
                                      prefix: str,
                                      search_string: str,
                                      model_list: List[BaseModel],
                                      expire: int):
        list_json = json.dumps(model_list, default=pydantic_encoder)
        await self.redis.set(f'{prefix}:{search_string}', list_json,
                             expire=expire)

    async def _get_list_from_cache(self, page_number: int, page_size: int,
                                   prefix: str, model: BaseModel) -> Optional[
        List[BaseModel]]:
        data = await self.redis.get(f'{prefix}:{page_number}:{page_size}')
        if not data:
            return None
        return parse_raw_as(List[model], data)

    async def _get_list_from_elastic(
            self,
            page_number: int,
            page_size: int,
            es_index: str,
            model: BaseModel,
            query: dict = None) -> Optional[List[BaseModel]]:
        body = {"from": page_number * page_size, "size": page_size}
        if query:
            body = body | query
        docs = await self.elastic.search(
            index=es_index,
            body=body
        )
        result = []
        for d in docs['hits']['hits']:
            result.append(model(**d['_source']))
        return result

    async def _put_list_to_cache(self, page_number: int, page_size: int,
                                 prefix: str, model_list: List[BaseModel],
                                 expire: int):
        list_json = json.dumps(model_list, default=pydantic_encoder)
        await self.redis.set(f'{prefix}:{page_number}:{page_size}', list_json,
                             expire=expire)
