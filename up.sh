#!/bin/bash
echo "Создание структуры папок для запуска ETL/ES. Запуск elastic"
mkdir volumes
mkdir volumes/elasticdb
mkdir volumes/ETL
mkdir volumes/postgres_data
touch volumes/ETL/etl.log
cp requirements.txt src/
cp .env.sample .env
docker-compose up --build -d ma_postgres
sleep 25
echo "Начало загрузки данных в postgres"
cp .env.sample sqlite_to_postgres/.env
cd sqlite_to_postgres/ || exit
../venv/bin/python load_data.py
cd ..
#echo "Запуск Nginx"
#docker-compose up --build -d ma_nginx
echo 'Start Redis'
docker-compose up --build -d ma_redis
docker-compose up --build -d ma_es01
sleep 25
echo "ETL"
docker-compose up --build -d ma_etl
echo 'Start FastAPI'
sleep 25
docker-compose up --build -d ma_fastapi
rm src/requirements.txt
