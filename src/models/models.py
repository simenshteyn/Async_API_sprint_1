from uuid import UUID
from datetime import date

# import orjson

from typing import Union, Optional, List, Dict
from pydantic import BaseModel


OBJ_ID   = Union[str, str, UUID]
OBJ_NAME = Union[str, str, UUID]


# def orjson_dumps(v, *, default):
#     return orjson.dumps(v, default=default).decode()


class Film(BaseModel):
    id              : Union[int, str, UUID]
    imdb_rating     : Optional[float] = None
    title           : str
    description     : Optional[str] = None
    actors_names    : Optional[List[str]] = None
    writers_names   : Optional[List[str]] = None
    directors_names : Optional[List[str]] = None
    genres_names    : Optional[List[str]] = None
    actors          : Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None
    writers         : Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None
    directors       : Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None
