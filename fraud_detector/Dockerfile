FROM python:3.12-slim

WORKDIR /app

# Создание директории для логов
RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs

# директория для батчей сообщений
RUN mkdir -p /app/tmp_data && \
    chmod -R 777 /app/tmp_data

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

CMD ["python", "-m", "app.service"]