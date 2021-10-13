from uuid import UUID
from datetime import date

import orjson

from typing import Union, Optional, List, Dict
from pydantic import BaseModel


OBJ_ID   = Union[str, UUID]
OBJ_NAME = Union[str, str]


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class Orjson(BaseModel):

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


class Film(Orjson):
    id              : Union[int, str, UUID]
    imdb_rating     : float = None
    genre           : List[Dict[OBJ_NAME, OBJ_ID]] = None
    title           : str
    description     : str = None
    director        : List[Dict[OBJ_ID, OBJ_NAME]] = None
    actors_names    : List[str] = None
    writers_names   : List[str] = None
    actors          : List[Dict[OBJ_ID, OBJ_NAME]] = None
    writers         : List[Dict[OBJ_ID, OBJ_NAME]] = None


class Genre(Orjson):
    id              : Union[int, str, UUID]
    name            : str
    description     : Optional[str] = None


class Person(Orjson):
    id              : Union[int, str, UUID]
    full_name       : str
    birth_date      : date = None
    role            : str = None
    film_ids        : list[Union[int, str, UUID]]
