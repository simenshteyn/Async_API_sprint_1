from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from models.models import Film, FilmShort
from services.film import FilmService, get_film_service

router = APIRouter()


@router.get('/', response_model=list[Film], response_model_exclude_unset=True)
async def films_sorted(sort: str = 'imdb_rating',
                       filter_genre: str = None,
                       page_number: int = 0,
                       page_size: int = 20,
                       film_service: FilmService = Depends(get_film_service)):
    query = {
         'sort_field': 'imdb_rating',
         'sort_type': 'asc' if sort == 'imdb_rating' else 'desc',
         'filter_genre': filter_genre,
         'page_number': page_number,
         'page_size': page_size
    }
    key = ''.join([str(b) for i, b in query.items()])
    if filter_genre:
        body = {"query": {"match": {"genre.id": {"query": query.get('filter_genre')}}}}
    else:
        body = {'query': {"match_all": {}}}
    film_list = await film_service.get_film(key=key, query=query, body=body)
    if not film_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film not found')
    result = []
    for film in film_list:
        result.append(FilmShort(id=film.id,
                                title=film.title,
                                imdb_rating=film.imdb_rating, ))
    return result


@router.get('/search/{film_search_string}', response_model=list[FilmShort],
            response_model_exclude_unset=True)
async def films_search(film_search_string: str,
                       film_service: FilmService = Depends(
                           get_film_service)) -> list[FilmShort]:
    body = {"query": {
                "match": {
                    "title": {
                        "query": film_search_string,
                        "fuzziness": "auto"
                    }
                }
            }}
    film_list = await film_service.get_film(key=film_search_string, body=body)
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
    body = {'query': {"match": {'_id': film_id}}}
    film = await film_service.get_film(key=film_id, body=body)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film not found')
    film = film[0]
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
async def film_alike(film_id: str, film_service: FilmService = Depends(get_film_service)) -> list[FilmShort]:
    key = 'alike'+film_id
    film_list = await film_service.get_film_alike(film_id=film_id, key=key)
    if not film_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film alike not found')
    result = []
    for film in film_list:
        result.append(FilmShort(id=film.id,
                                title=film.title,
                                imdb_rating=film.imdb_rating,))
    return result


@router.get('/genre/{genre_id}', response_model=list[FilmShort],
            response_model_exclude_unset=True)
async def popular_in_genre(genre_id: str,
                           film_service: FilmService = Depends(
                               get_film_service)) -> list[FilmShort]:
    query = {
                'sort_field': 'imdb_rating',
                'sort_type': 'desc',
                'filter_genre': genre_id,
                'page_number': 0,
                'page_size': 30
            }
    key = ''.join([str(b) for i, b in query.items()])
    film_list = await film_service.get_film(key=key, query=query)
    if not film_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film alike not found')
    result = []
    for film in film_list:
        result.append(FilmShort(id=film.id,
                                title=film.title,
                                imdb_rating=film.imdb_rating, ))
    return result
