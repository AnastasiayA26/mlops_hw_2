# Real-Time Fraud Detection System

Сервис подготовлен в рамках ДЗ 2 по MLOps в МТС ШАД 2025. Датасеты представлены в [соревновании](https://www.kaggle.com/competitions/teta-ml-1-2025)

Система для обнаружения мошеннических транзакций в реальном времени с использованием ML-модели и Kafka для потоковой обработки данных.

## Архитектура

Компоненты системы:
1. **`interface`** (Streamlit UI):
   
   Создан для удобной симуляции потоковых данных с транзакциями. Реальный продукт использовал бы прямой поток данных из других систем.
    - Имитирует отправку транзакций в Kafka через CSV-файлы.
    - Генерирует уникальные ID для транзакций.
    - Загружает транзакции отдельными сообщениями формата JSON в топик kafka `transactions`.
    

2. **`fraud_detector`** (ML Service):
   Детали работы:
   - Обработка данных `preprocessing.py`
   1. Временные признаки:
      - Час, день, день недели, месяца, года из `transaction_time` + удаление признака `transaction_time`
      - Добавила признак `is_night`

   2. Категориальные переменные:
   - Обработка категориальных признаков через Pool в CatBoost
   3. Сделала обработку батчами:
   - собирает транзакции в json файлы в app/tmp_data для асинхронности
   - Создаются батчи произвольного размера (16)
   - Проводит препроцессинг данных


   - Модель `scorer.py`
      - Порог классификации: 0.98
      - Модель в формате .gz (так она занимает меньше места)
      - Автоматическая загрузка модели при инициализации
      - `predict_proba`: обработка батчами
      - `save_feature_importance` для получения топ признаков модели, записывается в .json
      - `save_prediction_plot`: сохранение распределения вероятностей графиком в .png

4. **Kafka Infrastructure**:
   - Zookeeper + Kafka брокер
   - `kafka-setup`: автоматически создает топики `transactions` и `scoring`
   - Kafka UI: веб-интерфейс для мониторинга сообщений (порт 8080)


### Требования
- Docker 20.10+
- Docker Compose 2.0+

### Запуск
```bash
git clone https://github.com/AnastasiayA26/mlops_hw_2
cd mlops_hw_2
docker-compose up --build
```
После запуска:
- **Streamlit UI**: http://localhost:8501
- **Kafka UI**: http://localhost:8080
- **Логи сервисов**: 
  ```bash
  docker-compose logs <service_name>  # fraud_detector, kafka, interface

## Использование

### 1. Загрузка данных:

 - Загрузите CSV через интерфейс Streamlit. Для тестирования работы проекта используется файл формата `test.csv` из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025
 - Пример структуры данных:
    ```csv
    transaction_time,amount,lat,lon,merchant_lat,merchant_lon,gender,...
    2023-01-01 12:30:00,150.50,40.7128,-74.0060,40.7580,-73.9855,M,...
    ```
 - Для первых тестов рекомендуется загружать небольшой семпл данных (до 100 транзакций) за раз, чтобы исполнение кода не заняло много времени.

### 2. Мониторинг:
 - **Kafka UI**: Просматривайте сообщения в топиках transactions и scoring
 - **Логи обработки**: /app/logs/service.log внутри контейнера fraud_detector

### 3. Результаты:

 - Скоринговые оценки пишутся в топик scoring в формате:
    ```json
    {
    "score": 0.995, 
    "fraud_flag": 1, 
    "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"
    }
    ```

## Структура проекта
```
.

├── bd/
│   ├── 001_init.sql
│   └── Dockerfile
├── fraud_detector/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── service.py 
│   │   └── batch_split.py
│   ├── models/
│   │   └── model_catboost.cbm.gz
│   ├── src/
│   │   ├── __init__.py
│   │   ├── preprocessing.py
│   │   └── scorer.py   
│   ├── requirements.txt
│   └── Dockerfile
├── interface/
│   ├── app.py   # Streamlit UI
│   ├── requirements.txt
│   └── Dockerfile
├── scoring_consumer/
│   ├── app.py   
│   ├── requirements.txt
│   └── Dockerfile
├── docker-compose.yaml
└── README.md
```

## Настройки Kafka
```yml
Топики:
- transactions (входные данные)
- scoring (результаты скоринга)

Репликация: 1 (для разработки)
Партиции: 3
```

*Примечание:* 

Для полной функциональности убедитесь, что:
1. Архив `model_catboost.cbm.gz` размещена в `fraud_detector/models/`
2. Тренировочные данные находятся в `fraud_detector/train_data/`

3. Порты 8080, 8501 и 9095 свободны на хосте



