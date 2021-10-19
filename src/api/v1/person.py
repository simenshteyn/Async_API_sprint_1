from datetime import date
from http import HTTPStatus
from typing import Union, Optional, List, Dict
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.person import PersonService, get_person_service

router = APIRouter()

OBJ_ID = Union[str, str, UUID]
OBJ_NAME = Union[str, str, UUID]


class Person(BaseModel):
    id: Union[int, str, UUID]
    full_name: str
    birth_date: Optional[date] = None
    role: Optional[str] = None
    film_ids: Optional[List[Union[int, str, UUID]]] = None


@router.get('/{person_id}', response_model=Person,
            response_model_exclude_unset=True)
async def person_details(person_id: str,
                         person_service: PersonService = Depends(
                             get_person_service)) -> Person:
    person = await person_service.get_by_id(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='person not found')
    return Person(id=person.id,
                  full_name=person.full_name,
                  birth_date=person.birth_date,
                  role=person.role,
                  film_ids=person.film_ids)


@router.get('/', response_model=List[Person],
            response_model_exclude_unset=True)
async def person_list(
        page_number: int = 0,
        page_size: int = 20,
        person_service: PersonService = Depends(get_person_service)) -> List[
    Person]:
    person_list = await person_service.get_person_list(page_number=page_number,
                                                       page_size=page_size)
    if not person_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='persons not found')
    result = []
    for person in person_list:
        result.append(Person(id=person.id,
                             full_name=person.full_name,
                             birth_date=person.birth_date,
                             role=person.role,
                             film_ids=person.film_ids))
    return result


@router.get('/search/{person_search_string}', response_model=List[Person],
            response_model_exclude_unset=True)
async def films_search(person_search_string: str,
                       person_service: PersonService = Depends(
                           get_person_service)) -> List[Person]:
    person_list = await person_service.get_by_search(person_search_string)
    if not person_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='person not found')
    result = []
    for person in person_list:
        result.append(Person(id=person.id,
                             full_name=person.full_name,
                             birth_date=person.birth_date,
                             role=person.role,
                             film_ids=person.film_ids))
    return result
