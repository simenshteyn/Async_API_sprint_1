import psycopg2
import logging

from datetime import datetime
from contextlib import closing

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from config import dsl, es_conf
from postgresloader import PostgresLoader
from utils import backoff
from es import EsSaver


logger = logging.getLogger('LoaderStart')


def load_from_postgres(pg_conn: _connection) -> list:
    """Основной метод загрузки данных из Postgres"""
    postgres_loader = PostgresLoader(pg_conn)
    data = postgres_loader.loader()
    return data


if __name__ == '__main__':
    @backoff()
    def query_postgres() -> list:
        with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            logger.info(f'{datetime.now()}\n\nPostgreSQL connection is open. Start load data')
            load_pq = load_from_postgres(pg_conn)
        return load_pq

    def save_elastic() -> None:
        logger.info(f'{datetime.now()}\n\nElasticSearch connection is open. Start load data')
        EsSaver(es_conf).create_index('schemas.json')
        EsSaver(es_conf).load(query_postgres())

    save_elastic()
