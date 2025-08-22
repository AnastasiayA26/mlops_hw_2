import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import os

logger = logging.getLogger(__name__)
RANDOM_STATE = 42
TARGET_COL = 'target'

scaler = None
numerical_columns = []

def add_time_features(df):

    logger.info("Adding time features")
    
    if 'transaction_time' in df.columns:
        df['transaction_time'] = pd.to_datetime(df['transaction_time'], errors='coerce')
        dt = df['transaction_time'].dt
        df['transaction_month'] = dt.month
        df['transaction_year'] = dt.year
        df['transaction_hour'] = dt.hour
        
        df['is_time_target'] = df['transaction_hour'].isin([22, 23, 0, 1, 2, 3]).astype(int)
        df['is_month_target'] = df['transaction_month'].isin([12, 1, 2, 3]).astype(int)
    else:
        df['transaction_month'] = -1
        df['transaction_year'] = -1
        df['transaction_hour'] = -1
        df['is_time_target'] = 0
        df['is_month_target'] = 0
        
    return df

def preprocess_data(train_df, input_df):

    global scaler, numerical_columns
    logger.info("Starting preprocessing")
    input_df = add_time_features(input_df)
    
    cols_drop = [
        'address', 'street', 'one_city', 'name_1', 'name_2', 
        'lat', 'lon', 'post_code', 'merchant_lat', 'merchant_lon',
        'transaction_time', 'transaction_year'
    ]

    input_df = input_df.drop(columns=cols_drop, errors='ignore')
    logger.info(f"After dropping columns: {input_df.shape}")
    cats = ['merch', 'cat_id', 'gender', 'us_state', 'jobs']

    if scaler is None:
        logger.info("Initializing scaler from training data")
        numerical_columns = input_df.select_dtypes(include=np.number).columns.tolist()
        if TARGET_COL in numerical_columns:
            numerical_columns.remove(TARGET_COL)
        scaler = MinMaxScaler()
        if numerical_columns:
            scaler.fit(train_df[numerical_columns])
    
    if scaler and numerical_columns:
        cols_to_scale = [col for col in numerical_columns if col in input_df.columns]
        if cols_to_scale:
            input_df[cols_to_scale] = scaler.transform(input_df[cols_to_scale])
    
    logger.info("Preprocessing complete")
    return input_df, cats

def load_train_data(path='./train_data/train.csv'):

    global scaler
    logger.info(os.getcwd())
    logger.info('Loading training data...')
    train_df = pd.read_csv(path)
    logger.info('Raw train data imported. Shape: %s', train_df.shape)
    processed_df, cats = preprocess_data(train_df, train_df)
    
    logger.info('Processed train data. Shape: %s', processed_df.shape)
    return processed_df