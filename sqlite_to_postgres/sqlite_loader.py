import sqlite3

from schemas import (
    FilmWorkWithoutField,
    GenreWithoutField,
    GenreFilmWorkWithoutField,
    PersonWithoutField,
    PersonFilmWorkWithoutField
)


class SQLiteLoader:
    """load_movies - метод загрузки всех таблиц из sqllite
    Остальные методы отвечают каждый за свою таблицу (название после load_ - имя таблицы в бд)
    """
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()

    def load_film_work(self) -> list:
        film_work = []
        for row in self.cur.execute('SELECT * FROM film_work'):
            film_work.append(FilmWorkWithoutField(
                title=row['title'],
                description=row['description'],
                creation_date=row['creation_date'],
                certificate=row['certificate'],
                file_path=row['file_path'],
                type=row['type'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                rating=row['rating'],
                id=row['id']
            ))
        return film_work

    def load_genre(self) -> list:
        genre = []
        for row in self.cur.execute('SELECT * FROM genre'):
            genre.append(GenreWithoutField(
                name=row['name'],
                description=row['description'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                id=row['id']
            ))
        return genre

    def load_genre_film_fork(self) -> list:
        genre_film_fork = []
        for row in self.cur.execute('SELECT * FROM genre_film_work'):
            genre_film_fork.append(GenreFilmWorkWithoutField(
                film_work_id=row['film_work_id'],
                genre_id=row['genre_id'],
                created_at=row['created_at'],
                id=row['id'],
            ))
        return genre_film_fork

    def load_person(self) -> list:
        person = []
        for row in self.cur.execute('SELECT * FROM person'):
            person.append(PersonWithoutField(
                full_name=row['full_name'],
                birth_date=row['birth_date'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                id=row['id'],
            ))
        return person

    def load_person_film_work(self) -> list:
        person_film_work = []
        for row in self.cur.execute('SELECT * FROM person_film_work'):
            person_film_work.append(PersonFilmWorkWithoutField(
                film_work_id=row['film_work_id'],
                person_id=row['person_id'],
                role=row['role'],
                created_at=row['created_at'],
                id=row['id'],
            ))
        return person_film_work

    def load_movies(self) -> dict:
        return {
            'film_work': self.load_film_work(),
            'genre': self.load_genre(),
            'genre_film_work': self.load_genre_film_fork(),
            'person': self.load_person(),
            'person_film_work': self.load_person_film_work()
        }
