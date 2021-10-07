from datetime import datetime
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from state import JsonFileStorage, State
from db_query import big_request, load_person_id, load_film_id, full_load
from src.models.film import Film


class PostgresLoader:
    """Класс для выгрузки данных из postgres"""
    def __init__(self, pg_conn: _connection, state_key='my_key'):
        self.conn = pg_conn
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.batch_size = 100
        self.key = state_key
        self.state_key = State(JsonFileStorage('states/PostgresData.txt')).get_state(state_key)
        self.data = []

    def load_person_id(self) -> str:
        """Вложенный запрос на получение id персон, думаю функция тут лишняя """
        return load_person_id

    def load_film_work_id(self) -> str:
        """Вложенный запрос на получение id фильмворков"""
        if self.state_key is None:
            return load_film_id

        inx = load_film_id.rfind(f'WHERE pfw.person_id IN ({self.load_person_id()})')
        return f"{load_film_id[:inx]} AND updated_at > '{self.state_key}' {load_film_id[inx:]}"

    def loader(self) -> list:
        """Запрос на получение всех данных"""
        self.cursor.execute((full_load, self.load_film_work_id()))

        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                d = Film(
                    id              = dict(row).get('id'),
                    imdb_rating     = dict(row).get('rating'),
                    title           = dict(row).get('title'),
                    description     = dict(row).get('description'),
                    actors_names    = dict(row).get('actors_names'),
                    writers_names   = dict(row).get('writers_names'),
                    directors_names = dict(row).get('directors_names'),
                    genres_names    = dict(row).get('genre'),
                    actors          = dict(row).get('actors'),
                    writers         = dict(row).get('writers'),
                    directors       = dict(row).get('directors'),
                )
                self.data.append(d.dict())

        return self.data
