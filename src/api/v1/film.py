from http import HTTPStatus
from typing import Union, Optional, List, Dict
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from src.services.film import FilmService, get_film_service


router = APIRouter()


OBJ_ID = Union[str, str, UUID]
OBJ_NAME = Union[str, str, UUID]

class Film(BaseModel):
    id: Union[int, str, UUID]
    imdb_rating: Optional[float] = None
    genre: Optional[List[str]] = None
    title: str
    description: Optional[str] = None
    director: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None
    actors: Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None
    writers: Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None


@router.get('/{film_id}', response_model=Film)
async def film_details(film_id: str,
                       film_service: FilmService = Depends(get_film_service)) -> Film:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film not found')
    return Film(id=film.id,
                imdb_rating=film.imdb_rating,
                genre=film.genre,
                title=film.title,
                description=film.description,
                director=film.director,
                actors_names=film.actors_names,
                writers_names=film.writers_names,
                actors=film.actors,
                writers=film.writers)