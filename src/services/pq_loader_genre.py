from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from state import JsonFileStorage, State
from db_query import query_all_genre
from src.models.film import Genre


class PostgresLoader:
    """Класс для выгрузки данных из postgres"""
    def __init__(self, pg_conn: _connection, state_key='my_key'):
        self.conn = pg_conn
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.batch_size = 100
        self.state_key = State(JsonFileStorage('states/PostgresData.txt')).get_state(state_key)
        self.data = []

    def load_genre(self) -> str:
        if self.state_key is None:
            return query_all_genre
        inx = query_all_genre.rfind(f'content.genre')
        return f"{query_all_genre[:inx]} WHERE updated_at > '{self.state_key}' {query_all_genre[inx:]}"

    def loader(self) -> list:
        """Запрос на получение всех данных"""
        self.cursor.execute(self.load_genre())

        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                d = Genre(
                    id              = dict(row).get('id'),
                    name            = dict(row).get('name'),
                    description     = dict(row).get('description'),
                )
                self.data.append(d.dict())

        return self.data
