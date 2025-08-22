import gzip
import json
import logging
import os
import shutil
import threading
import uuid

from confluent_kafka import Consumer, Producer

from .batch_proc import batch_processor
from src.preprocessing import load_train_data

DATA_DIR = '/app/tmp_data'
os.makedirs(DATA_DIR, exist_ok=True)

with gzip.open('/app/models/model_catboost.cbm.gz', 'rb') as f_in:
    with open('/app/models/model_catboost.cbm', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")

class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.train = load_train_data()

    def save_message_to_file(self, data):
        file_id = str(uuid.uuid4())
        path = os.path.join(DATA_DIR, f"{file_id}.json")
        with open(path, 'w') as f:
            json.dump(data, f)

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                self.save_message_to_file(data)
            except Exception as e:
                logger.error(f"Error saving message: {e}")

if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()

    threading.Thread(
        target=batch_processor,
        args=(service.train, service.producer),
        daemon=True
    ).start()

    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped')
