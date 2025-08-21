import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import psycopg2
import matplotlib.pyplot as plt
import json
import time
import os
import uuid
import numpy as np

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("Список загруженных файлов")
    
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")
        
        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")


st.divider()
st.header("Посмотреть результаты")

DB_URL = os.getenv("DATABASE_URL", "postgresql://ml:ml@postgres:5432/ml")

def read_sql_df(query: str) -> pd.DataFrame:
    """Простой helper: открыть соединение → прочитать → закрыть"""
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        df = pd.read_sql(query, conn)
        return df
    finally:
        if conn is not None:
            conn.close()

if st.button("Посмотреть результаты", type="primary"):
    # 1) 10 последних транзакций с fraud_flag = 1
    st.subheader("Последние 10 транзакций с fraud_flag = 1")
    try:
        fraud_df = read_sql_df(
            """
            SELECT transaction_id, score, fraud_flag, created_at
            FROM public.score_events
            WHERE fraud_flag = TRUE
            ORDER BY created_at DESC
            LIMIT 10;
            """
        )
        if fraud_df.empty:
            st.info("Записей с fraud_flag=1 пока нет.")
        else:
            st.dataframe(fraud_df, use_container_width=True)
    except Exception as e:
        st.error(f"Ошибка запроса к БД (fraud list): {e}")

    # 2) Гистограмма скоров последних 100 ТРАНЗАКЦИЙ
    st.subheader("Гистограмма скоров последних 100 транзакций")
    try:
        scores_df = read_sql_df(
            """
            WITH latest_per_tx AS (
              SELECT DISTINCT ON (transaction_id)
                     transaction_id, score, created_at
              FROM public.score_events
              ORDER BY transaction_id, created_at DESC
            )
            SELECT score, created_at
            FROM latest_per_tx
            ORDER BY created_at DESC
            LIMIT 100;
            """
        )

        if scores_df.empty:
            st.info("Недостаточно данных для построения гистограммы.")
        else:

            values = scores_df["score"].dropna().values
            mean_val = values.mean()
            med_val = np.median(values)

            fig, ax = plt.subplots(figsize=(8, 4))
            # гистограмма
            ax.hist(
                values,
                bins=20,
                color="#5B8FF9",       
                edgecolor="#1F3B73",   
                alpha=0.85,
                label="Распределение score",
            )
            ax.axvline(mean_val, linestyle="--", linewidth=2, color="#FF7C43",
                    label=f"Среднее = {mean_val:.3f}")
            ax.axvline(med_val, linestyle="-.", linewidth=2, color="#F95D6A",
                    label=f"Медиана = {med_val:.3f}")

            ax.set_title("Гистограмма предсказанных score")
            ax.set_xlabel("score")
            ax.set_ylabel("количество")
            ax.grid(True, alpha=0.3)
            ax.legend(loc="best")
            plt.tight_layout()
            st.pyplot(fig, clear_figure=True)
    except Exception as e:
        st.error(f"Ошибка запроса к БД (hist): {e}")
else:
    st.caption("Загрузить результаты из базы.")
