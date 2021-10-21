from datetime import date
from typing import Optional

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
    genre: Optional[list[dict[str, str]]] = None
    title: str
    description: Optional[str] = None
    director: Optional[list[dict[str, str]]] = None
    actors_names: Optional[list[str]] = None
    writers_names: Optional[list[str]] = None
    actors: Optional[list[dict[str, str]]] = None
    writers: Optional[list[dict[str, str]]] = None


class FilmShort(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float] = None


class Genre(Orjson):
    id: str
    name: str
    description: Optional[str] = None


class Person(Orjson):
    id: str
    full_name: str
    birth_date: Optional[date] = None
    role: Optional[str] = None
    film_ids: list[str]
