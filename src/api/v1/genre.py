from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from models.models import Genre
from services.genre import GenreService, get_genre_service

router = APIRouter()


@router.get('/{genre_id}', response_model=Genre,
            response_model_exclude_unset=True)
async def genre_details(genre_id: str,
                        genre_service: GenreService = Depends(
                            get_genre_service)) -> Genre:
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='genre not found')
    return Genre(id=genre.id,
                 name=genre.name,
                 description=genre.description
                 )


@router.get('/', response_model=list[Genre], response_model_exclude_unset=True)
async def genre_list(
        page_number: int = 0,
        page_size: int = 50,
        genre_service: GenreService = Depends(get_genre_service)) -> list[
    Genre]:
    genre_list = await genre_service.get_genre_list(page_number=page_number,
                                                    page_size=page_size)
    if not genre_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='genres not found')
    return [Genre(id=genre.id,
                  name=genre.name,
                  description=genre.description) for genre in genre_list]
