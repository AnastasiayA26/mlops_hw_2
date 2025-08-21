# db/Dockerfile
FROM postgres:15

RUN apt-get update && apt-get install -y locales && rm -rf /var/lib/apt/lists/*
ENV TZ=UTC

# Кладём init-скрипты внутрь образа
COPY 001_init.sql /docker-entrypoint-initdb.d/001_init.sql
