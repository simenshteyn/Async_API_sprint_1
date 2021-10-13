from http import HTTPStatus
from typing import Union, Optional, List, Dict
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.film import FilmService, get_film_service

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


class FilmShort(BaseModel):
    id: Union[int, str, UUID]
    title: str
    imdb_rating: Optional[float] = None


@router.get('/', response_model=List[Film], response_model_exclude_unset=True)
async def films_sorted(sort: Optional[str] = None,
                       page_number: int = 0,
                       page_size: int = 20,
                       film_service: FilmService = Depends(get_film_service)):
    if sort == "imdb_rating":
        film_list = await film_service.get_sorted_by_field(sort_field='imdb_rating',
                                                           sort_type='asc',
                                                           page_number=page_number,
                                                           page_size=page_size)
        if not film_list:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                                detail='film not found')
        result = []
        for film in film_list:
            result.append(FilmShort(id=film.id,
                                    title=film.title,
                                    imdb_rating=film.imdb_rating, ))
        return result
    elif sort == "-imdb_rating" or not sort:
        film_list = await film_service.get_sorted_by_field(
            sort_field='imdb_rating',
            sort_type='desc',
            page_number=page_number,
            page_size=page_size)
        if not film_list:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                                detail='film not found')
        result = []
        for film in film_list:
            result.append(FilmShort(id=film.id,
                                    title=film.title,
                                    imdb_rating=film.imdb_rating, ))
        return result
    else:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='sorting not found')


@router.get('/search/{film_search_string}', response_model=List[FilmShort],
            response_model_exclude_unset=True)
async def films_search(film_search_string: str,
                       film_service: FilmService = Depends(
                           get_film_service)) -> List[FilmShort]:
    film_list = await film_service.get_by_search(film_search_string)
    if not film_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film not found')
    result = []
    for film in film_list:
        result.append(FilmShort(id=film.id,
                                title=film.title,
                                imdb_rating=film.imdb_rating, ))
    return result


@router.get('/{film_id}', response_model=Film,
            response_model_exclude_unset=True)
async def film_details(film_id: str,
                       film_service: FilmService = Depends(
                           get_film_service)) -> Film:
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
