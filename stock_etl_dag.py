from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

SYMBOLS = ['AAPL', 'GOOGL']
DB_CONFIG = {
    'dbname': 'stock_data_db',
    'user': 'airflow_user',
    'password': '####',
    'host': 'localhost'
}

def extract_data(**kwargs):
    data = {}
    for symbol in SYMBOLS:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="7d", interval="1h")
        hist.reset_index(inplace=True)
        hist['symbol'] = symbol
        data[symbol] = hist
    kwargs['ti'].xcom_push(key='stock_data', value=data)

def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='stock_data', task_ids='extract_stock_data')
    for symbol, df in data.items():
        df['moving_avg_7d'] = df['Close'].rolling(window=7).mean()
        data[symbol] = df
    kwargs['ti'].xcom_push(key='transformed_data', value=data)

def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_stock_data')
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for symbol, df in data.items():
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume, moving_avg_7d)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                row['symbol'], row['Datetime'], row['Open'], row['High'], row['Low'],
                row['Close'], row['Volume'], row['moving_avg_7d']
            ))
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='stock_price_etl',
    default_args=default_args,
    description='ETL pipeline for stock prices',
    start_date=datetime(2025, 5, 15),
    schedule_interval='*/15 * * * *',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_stock_data',
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_stock_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_stock_data',
        python_callable=load_data,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
