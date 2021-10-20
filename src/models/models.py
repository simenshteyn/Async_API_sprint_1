from datetime import date

import orjson

from pydantic import BaseModel


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class Orjson(BaseModel):

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


class Film(Orjson):
    id: str
    imdb_rating: float
    genre: list[dict[str, str]] = None
    title: str
    description: str = None
    director: list[dict[str, str]] = None
    actors_names: list[str] = None
    writers_names: list[str] = None
    actors: list[dict[str, str]] = None
    writers: list[dict[str, str]] = None


class FilmShort(BaseModel):
    id: str
    title: str
    imdb_rating: float = None


class Genre(Orjson):
    id: str
    name: str
    description: str = None


class Person(Orjson):
    id: str
    full_name: str
    birth_date: date = None
    role: str = None
    film_ids: list[str]
