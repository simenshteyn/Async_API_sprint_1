# Проектное задание: Async_API_sprint_1


# Проектное задание: ETL

Для запуска сервиса выполнить следующие шаги:

### Авто запуск
1. Клонируем репозиторий
2. Создаем виртуальное окружение python -m venv venv
3. Выполняем команду ```pip install -r requirements.txt```
4. В консоле запускаем ./up.sh (Файл должен быть исполняемым chmod +x ./up.sh)
5. Скрипт запустит сервисы - Postgres, movies_admin, nginx, ElasticSearch, ETL
6. Сервис ETL проверяет наличие обновлений в базе каждые 10-12 сек
7. Пользуемся и радуемся)

### Ручной запуск
1. Клонируем репозиторий
2. Создаем виртуальное окружение python -m venv venv
 
Далее выполняем компанды:
```
- echo "Запуск postgres"
- docker-compose up --build -d ma_postgres
- sleep 5
- echo "Начало загрузки данных в postgres"
- cp .env.sample sqlite_to_postgres/.env
- cp .env.sample movies_admin/.env
- cp .env.sample postgres_to_es/.env
- cd sqlite_to_postgres/ || exit
- ../venv/bin/python load_data.py
- cd ..
- echo "Старт Админки"
- docker-compose up --build -d ma_web
- docker exec -it ma_web bash -c "python manage.py makemigrations && python manage.py migrate && python manage.py collectstatic --noinput"
- echo "Запуск Nginx"
- docker-compose up --build -d ma_nginx
- echo "Запуск ETL"
- mkdir postgres_to_es/volumes
- mkdir ./elasticdb
- touch postgres_to_es/volumes/PostgresData.txt
- touch postgres_to_es/volumes/etl.log
- docker-compose up --build -d ma_es01
- docker-compose up --build -d ma_etl


[Ссылка на репозиторий](https://github.com/simenshteyn/Async_API_sprint_1)