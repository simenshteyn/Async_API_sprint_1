import psycopg2
import logging

from datetime import datetime
from contextlib import closing

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from src.config import dsl, es_conf
from pq_loader_film_work import PostgresLoader as pq_load_film
from pq_loader_genre import PostgresLoader as pq_load_genre
from utils import backoff
from es import EsSaver


logger = logging.getLogger('LoaderStart')


def load_from_postgres(pg_conn: _connection, func: callable) -> list:
    """Основной метод загрузки данных из Postgres"""
    postgres_loader = func(pg_conn)
    data = postgres_loader.loader()
    return data


if __name__ == '__main__':
    @backoff()
    def query_postgres_film(func: callable) -> list:
        with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            logger.info(f'{datetime.now()}\n\nPostgreSQL connection is open. Start load {func} data')
            load_pq = load_from_postgres(pg_conn, func)
        return load_pq


    def save_elastic(pq_func: callable, schemas: str, name_index: str) -> None:
        logger.info(f'{datetime.now()}\n\nElasticSearch connection is open. Start load {name_index} data')
        EsSaver(es_conf).create_index(schemas, name_index=name_index)
        EsSaver(es_conf).load(query_postgres_film(pq_func), name_index=name_index)

    save_elastic(pq_func=pq_load_film, schemas='schemas_es/schemas_film.json', name_index='movies')
    save_elastic(pq_func=pq_load_genre, schemas='schemas_es/schemas_genre.json', name_index='genre')

