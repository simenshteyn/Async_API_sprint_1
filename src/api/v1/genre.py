from http import HTTPStatus
from typing import Union, Optional, List, Dict
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.genre import GenreService, get_genre_service

router = APIRouter()

OBJ_ID = Union[str, str, UUID]
OBJ_NAME = Union[str, str, UUID]


class Genre(BaseModel):
    id: Union[int, str, UUID]
    name: str
    description: Optional[str] = None


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
