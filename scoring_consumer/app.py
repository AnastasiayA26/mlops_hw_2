import json
import os
import signal
import sys
import time
from confluent_kafka import Consumer, KafkaException
import psycopg2
import psycopg2.extras

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "transactions")
GROUP = os.getenv("KAFKA_GROUP_ID", "scoring-consumer")
DB_URL = os.getenv("DATABASE_URL", "postgresql://ml:ml@postgres:5432/ml")

running = True
def handle_sig(*_):
    global running
    running = False

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

def get_conn():
    return psycopg2.connect(DB_URL)

def ensure_table(conn):
    with conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.score_events (
          id BIGSERIAL PRIMARY KEY,
          transaction_id TEXT NOT NULL,
          score DOUBLE PRECISION NOT NULL,
          fraud_flag BOOLEAN NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_score_events_tid        ON public.score_events (transaction_id);
        CREATE INDEX IF NOT EXISTS idx_score_events_created_at ON public.score_events (created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_score_events_fraud      ON public.score_events (fraud_flag, created_at DESC);
        """)

def flush_batch(conn, rows):
    if not rows:
        return
    with conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO public.score_events (transaction_id, score, fraud_flag) VALUES %s",
            rows,
            template="(%s,%s,%s)"
        )
    print(f"[db] inserted {len(rows)} rows", flush=True)

def main():
    # Подключение к БД (с ретраями) и создание таблицы
    conn = None
    for i in range(30):
        try:
            conn = get_conn()
            ensure_table(conn)
            break
        except Exception as e:
            print(f"[db] connection retry {i+1}/30: {e}", flush=True)
            time.sleep(2)
    if conn is None:
        print("[fatal] cannot connect to DB", file=sys.stderr)
        sys.exit(1)

    # Kafka consumer
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 45000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    batch, BATCH_SIZE = [], 50
    last_commit = time.time()
    print(f"[consumer] listening on topic '{TOPIC}'", flush=True)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch and (time.time() - last_commit > 5):
                    flush_batch(conn, batch)
                    consumer.commit(asynchronous=False)
                    batch.clear()
                    last_commit = time.time()
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                txid = payload["transaction_id"]
                score = float(payload["score"])
                fraud = bool(payload["fraud_flag"])
                batch.append((txid, score, fraud))
            except Exception as e:
                print(f"[skip] bad message: {e}; value={msg.value()!r}", flush=True)

            if len(batch) >= BATCH_SIZE:
                flush_batch(conn, batch)
                consumer.commit(asynchronous=False)
                batch.clear()
                last_commit = time.time()
    finally:
        if batch:
            flush_batch(conn, batch)
            consumer.commit(asynchronous=False)
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
