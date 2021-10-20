# Проектное задание: Async_API_sprint_1


# Проектное задание: ETL

Для запуска сервиса выполнить следующие шаги:

### Авто запуск
1. Клонируем репозиторий
2. В консоле запускаем ./up.sh (Файл должен быть исполняемым chmod +x ./up.sh)
3. Скрипт запустит сервисы - Postgres, ElasticSearch, ETL, Redis
4. Сервис ETL проверяет наличие обновлений в базе каждые 10-12 сек
5. Пользуемся и радуемся)

####  API сервисы

OpenAPI: [http://localhost:8000/api/openapi](http://localhost:8000/api/openapi)

1. Список фильмов: [http://localhost:8000/api/v1/film/](http://localhost:8000/api/v1/film/)
2. Фильм по UUID: [http://localhost:8000/api/v1/film/2a090dde-f688-46fe-a9f4-b781a985275e](http://localhost:8000/api/v1/film/2a090dde-f688-46fe-a9f4-b781a985275e)
3. Похожие фильмы: [http://localhost:8000/api/v1/film/2a090dde-f688-46fe-a9f4-b781a985275e/alike](http://localhost:8000/api/v1/film/2a090dde-f688-46fe-a9f4-b781a985275e/alike)
4. Нечёткий поиск по фильмам: [http://localhost:8000/api/v1/film/search/dog](http://localhost:8000/api/v1/film/search/dog)
5. Сортировка фильмов по рейтингу: [http://localhost:8000/api/v1/film/?sort=-imdb_rating](http://localhost:8000/api/v1/film/?sort=-imdb_rating)
6. Сортировка фильмов с пагинацией: [http://localhost:8000/api/v1/film/?sort=-imdb_rating&page_size=10&page_number=3](http://localhost:8000/api/v1/film/?sort=-imdb_rating&page_size=10&page_number=3)
7. Сортировка фильмов с пагинацией и фильтрацией по жанру: [http://localhost:8000/api/v1/film/?sort=-imdb_rating&page_size=10&page_number=5&filter_genre=120a21cf-9097-479e-904a-13dd7198c1dd](http://localhost:8000/api/v1/film/?sort=-imdb_rating&page_size=10&page_number=5&filter_genre=120a21cf-9097-479e-904a-13dd7198c1dd)
8. Популярные фильмы в жанре: [http://localhost:8000/api/v1/film/genre/120a21cf-9097-479e-904a-13dd7198c1dd](http://localhost:8000/api/v1/film/genre/120a21cf-9097-479e-904a-13dd7198c1dd)
9. Список персон: [http://localhost:8000/api/v1/person/](http://localhost:8000/api/v1/person/)
10. Информация о персоне по UUID: [http://localhost:8000/api/v1/person/05d92f4a-b55c-45f6-9200-41f153a72a7a](http://localhost:8000/api/v1/person/05d92f4a-b55c-45f6-9200-41f153a72a7a)
11. Поиск по персонам: [http://localhost:8000/api/v1/person/search/adam](http://localhost:8000/api/v1/person/search/adam)
12. Список жанров: [http://localhost:8000/api/v1/genre/](http://localhost:8000/api/v1/genre/)
13. Жанр по UUID: [http://localhost:8000/api/v1/genre/c020dab2-e9bd-4758-95ca-dbe363462173](http://localhost:8000/api/v1/genre/c020dab2-e9bd-4758-95ca-dbe363462173)

[Ссылка на репозиторий](https://github.com/simenshteyn/Async_API_sprint_1)