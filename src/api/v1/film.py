from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from models.models import Film, FilmShort
from services.film import FilmService, get_film_service

router = APIRouter()


@router.get('/', response_model=list[Film], response_model_exclude_unset=True)
async def films_sorted(sort: str = None,
                       filter_genre: str = None,
                       page_number: int = 0,
                       page_size: int = 20,
                       film_service: FilmService = Depends(get_film_service)):
    if not sort:
        sort_field = 'imdb_rating'
        sort_type = 'desc'
    else:
        sort_field = 'imdb_rating' if sort.endswith('imdb_rating') else None
        if not sort_field:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                                detail='sorting not found')
        sort_type = 'desc' if sort.startswith('-') else 'asc'
    film_list = await film_service.get_film_sorted(
        sort_field=sort_field,
        sort_type=sort_type,
        filter_genre=filter_genre,
        page_number=page_number,
        page_size=page_size)
    if not film_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film not found')
    return [FilmShort(id=film.id,
                      title=film.title,
                      imdb_rating=film.imdb_rating) for film in film_list]


@router.get('/search/{film_search_string}', response_model=list[FilmShort],
            response_model_exclude_unset=True)
async def films_search(film_search_string: str,
                       film_service: FilmService = Depends(
                           get_film_service)) -> list[FilmShort]:
    film_list = await film_service.get_film_by_search(film_search_string)
    if not film_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film not found')
    return [FilmShort(id=film.id,
                      title=film.title,
                      imdb_rating=film.imdb_rating) for film in film_list]


@router.get('/{film_id}', response_model=Film,
            response_model_exclude_unset=True)
async def film_details(film_id: str,
                       film_service: FilmService = Depends(
                           get_film_service)) -> Film:
    film = await film_service.get_film_by_id(film_id)
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


@router.get('/{film_id}/alike', response_model=list[FilmShort],
            response_model_exclude_unset=True)
async def film_alike(film_id: str,
                     film_service: FilmService = Depends(
                         get_film_service)) -> list[FilmShort]:
    film_list = await film_service.get_film_alike(film_id)
    if not film_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film alike not found')
    return [FilmShort(id=film.id,
                      title=film.title,
                      imdb_rating=film.imdb_rating) for film in film_list]


@router.get('/genre/{genre_id}', response_model=list[FilmShort],
            response_model_exclude_unset=True)
async def popular_in_genre(genre_id: str,
                           film_service: FilmService = Depends(
                               get_film_service)) -> list[FilmShort]:
    film_list = await film_service.get_popular_in_genre(genre_id)
    if not film_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film alike not found')
    return [FilmShort(id=film.id,
                      title=film.title,
                      imdb_rating=film.imdb_rating) for film in film_list]
