-- Подключение к базе
\c movies

-- Создаем схему
CREATE SCHEMA IF NOT EXISTS content;

-- Добавляем нумерованные типы для ролей
CREATE TYPE content.film_team_role AS ENUM ('director', 'writer', 'actor');

-- Устанавливаем расширение для генерации UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Создаем таблицу film_work
CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    certificate TEXT,
    file_path TEXT,
    rating FLOAT,
    type VARCHAR(30) NOT NULL,
    created_at TIMESTAMP with time zone,
    updated_at TIMESTAMP with time zone
);

-- Создаем таблицу genre
CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(30) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP with time zone,
    updated_at TIMESTAMP with time zone
);

-- Создаем таблицу genre_film_work
CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    film_work_id uuid REFERENCES content.film_work (id) NOT NULL,
    genre_id uuid REFERENCES content.genre (id) NOT NULL,
    created_at TIMESTAMP with time zone
);

-- Создаем таблицу person
CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    full_name VARCHAR(40) NOT NULL,
    birth_date DATE,
    created_at TIMESTAMP with time zone,
    updated_at TIMESTAMP with time zone
);

-- Создаем таблицу person_film_work
CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    film_work_id uuid REFERENCES content.film_work (id) NOT NULL,
    person_id uuid REFERENCES content.person (id) NOT NULL,
    role content.film_team_role NOT NULL,
    created_at TIMESTAMP with time zone
);

-- Создаем индексы
CREATE UNIQUE INDEX film_work_person_role ON content.person_film_work (film_work_id, person_id, role);
CREATE UNIQUE INDEX film_work_genre ON content.genre_film_work (film_work_id, genre_id);
