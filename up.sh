#!/bin/bash
echo "Создание структуры папок для запуска ETL/ES. Запуск elastic"
mkdir volumes
mkdir volumes/elasticdb
mkdir volumes/ETL
mkdir volumes/postgres_data
touch volumes/ETL/etl.log
cp requirements.txt src/
cp .env.sample .env
echo "Запуск postgres"
docker-compose up --build -d ma_postgres
echo 'Start Redis'
docker-compose up --build -d ma_redis
docker-compose up --build -d ma_es01
echo "Загрузка схем индексов в elastic"
sleep 30
docker-compose --profile=init up --build -d es_init
echo "Перенос данных в elastic"
sleep 30
docker-compose up --build -d ma_etl
echo 'Start FastAPI'
docker-compose up --build -d ma_fastapi
rm src/requirements.txt
