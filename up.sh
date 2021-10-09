#!/bin/bash
python -m venv venv
cp .env.sample .env
docker-compose up --build -d ma_postgres
sleep 5
echo "Начало загрузки данных в postgres"
cp .env.sample sqlite_to_postgres/.env
cd sqlite_to_postgres/ || exit
../venv/bin/python load_data.py
cd ..
#echo "Запуск Nginx"
#docker-compose up --build -d ma_nginx
echo "Создание структуры папок для запуска ETL/ES. Запуск elastic"
mkdir volumes
mkdir volumes/elasticdb
mkdir volumes/ETL
touch volumes/ETL/etl.log
docker-compose up --build -d ma_es01
sleep 5
echo "Копирование папок и файлов для сборки образа ETL"
mkdir src/services/etl/src
cp -r src/models src/services/etl/src/
cp -r src/services/schemas_es src/services/etl/src/
cp src/config.py src/services/etl/src/
cp requirements.txt src/services/
docker-compose up --build -d ma_etl
sleep 20
rm -r src/services/etl/src
rm src/services/requirements.txt