import re

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from state import JsonFileStorage, State
from db_query import load_person_q, load_film_id, full_load, query_all_genre, load_person_role
from schemas import Film, Genre, Person


class PostgresLoader:
    """Класс для выгрузки данных из postgres"""
    def __init__(self, pg_conn: _connection, state_key='my_key'):
        self.conn = pg_conn
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.batch_size = 100
        self.key = state_key
        self.state_key = State(JsonFileStorage('PostgresDataState.txt')).get_state(state_key)
        self.data = []

    def load_person_id(self) -> str:
        """Вложенный запрос на получение id персон, думаю функция тут лишняя """
        return load_person_q

    def load_film_work_id(self) -> str:
        """Вложенный запрос на получение id film_work"""
        query = load_film_id % self.load_person_id()
        if self.state_key is None:
            return query
        inx = query.rfind(
            f'WHERE pfw.person_id IN ({self.load_person_id()})'
        )
        return f"{query[:inx]} AND updated_at > '{self.state_key}' {query[inx:]}"

    def load_all_film_work_person(self) -> str:
        return full_load % self.load_film_work_id()

    def load_genre(self) -> str:
        if self.state_key is None:
            return query_all_genre
        inx = re.search('FROM content.genre', query_all_genre).end()
        return f"{query_all_genre[:inx]} WHERE updated_at > '{self.state_key}' {query_all_genre[inx:]}"

    def load_person(self) -> str:
        inx = load_person_q.find('id')
        query = load_person_role
        # if self.state_key is None:
        return query
        # inx = re.search('FROM content.person', query).end()
        # return f"{query[:inx]} WHERE updated_at > '{self.state_key}' {query[inx:]}"

    def loader_movies(self) -> list:
        """Запрос на получение всех данных по фильмам"""
        self.cursor.execute(self.load_all_film_work_person())

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

    def loader_genre(self) -> list:
        """Запрос на получение всех жанров"""
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

    def loader_person(self) -> list:
        """Запрос на получение всех жанров"""
        self.cursor.execute(self.load_person())

        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                print(row)
                d = Person(
                    id              = dict(row).get('id'),
                    full_name       = dict(row).get('full_name'),
                    birth_date      = dict(row).get('birth_date'),
                    roles           = dict(row).get('roles')
                    # {
                    #                     'actor'    : (dict(row).get('actor')),  #.replace('{', '').replace('}', ''),
                    #                     'writer'   : dict(row).get('writer'),  #.replace('{', '').replace('}', ''),
                    #                     'director' : dict(row).get('director')  #.replace('{', '').replace('}', ''),
                    #                   }
                )
                self.data.append(d.dict())

        return self.data
