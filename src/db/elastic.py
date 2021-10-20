from elasticsearch import AsyncElasticsearch

es: AsyncElasticsearch or None


async def get_elastic() -> AsyncElasticsearch:
    return es
