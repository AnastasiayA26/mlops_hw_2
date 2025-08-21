import json
import os
import time

import pandas as pd

from src.preprocessing import preprocess_data
from src.scorer import make_pred

DATA_DIR = '/app/tmp_data'

def batch_processor(train_df, producer, batch_size=16, interval=5):
    os.makedirs(DATA_DIR, exist_ok=True)
    while True:
        files = sorted(os.listdir(DATA_DIR))
        if len(files) < batch_size:
            time.sleep(interval)
            continue

        batch = files[:batch_size]
        data, ids = [], []

        for fname in batch:
            path = os.path.join(DATA_DIR, fname)
            if os.path.getsize(path) == 0:
                continue  # файл пустой, пропускаем
            with open(path, 'r') as f:
                obj = json.load(f)
                data.append(obj['data'])
                ids.append(obj['transaction_id'])


        df = pd.DataFrame(data)
        processed, cat_cols = preprocess_data(train_df, df)
        preds = make_pred(processed, cat_cols, source_info='batch')
        preds['transaction_id'] = ids

        producer.produce('scoring', value=preds.to_json(orient='records'))
        producer.flush()

        for fname in batch:
            os.remove(os.path.join(DATA_DIR, fname))
