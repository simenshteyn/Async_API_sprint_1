# Техническое задание

Предлагается выполнить проект «Асинхронное API». Этот сервис будет точкой входа для всех клиентов. В первой итерации в сервисе будут только анонимные пользователи. Функции авторизации и аутентификации запланированы в модуле «Auth».

## Используемые технологии

- Код приложения пишется на **Python + FastAPI**.
- Приложение запускается под управлением сервера **ASGI**(uvicorn).
- Хранилище – **ElasticSearch**.
- За кеширование данных отвечает – **redis cluster**.
- Все компоненты системы запускаются через **docker**.

## Описание
В папке tasks ваша команда сможет найти задачи, которые необходимо выполнить в первом спринте второго модуля. Обратите внимание на задачи 00_create_repo и 01_create_basis – они являются блокирующими для командной работы, их необходимо выполнить как можно раньше.

Мы оценили задачи в story point'ах, значения которых брались из [последовательности Фибоначчи](https://ru.wikipedia.org/wiki/Числа_Фибоначчи) (1,2,3,5,8,…).
Вы можете разбить имеющиеся задачи на более маленькие – например, чтобы распределять между участниками команды не большие куски задания, а маленькие подзадачи. В таком случае не забудьте зафиксировать изменения в issues в репозитории.

**От каждого разработчика ожидается выполнение минимум 40% от общего числа story points в спринте.**
