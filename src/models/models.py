from uuid import UUID
from datetime import date

from typing import Union, Optional, List, Dict
from pydantic import BaseModel


OBJ_ID   = Union[str, str, UUID]
OBJ_NAME = Union[str, str, UUID]


class Film(BaseModel):
    id              : Union[int, str, UUID]
    imdb_rating     : Optional[float] = None
    genres          : Optional[List[str]] = None
    title           : str
    description     : Optional[str] = None
    directors       : Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None
    actors_names    : Optional[List[str]] = None
    writers_names   : Optional[List[str]] = None
    actors          : Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None
    writers         : Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None


class Genre(BaseModel):
    id              : Union[int, str, UUID]
    name            : str
    description     : Optional[str] = None


class Person(BaseModel):
    id              : Union[int, str, UUID]
    full_name       : str
    birth_date      : date
    roles           : List[str]  # под вопросом
