
import re
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'tantd',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


def get_data():
    import requests
    # deploy cái ml này lên server
    res = requests.get("http://127.0.0.1:8085/random")
    res = res.json()
    print(res)
    return res


def format_data(res):
    if 'sentence' in res and isinstance(res['sentence'], str):
        data = {}
        pattern = r'[^a-zA-Z\sàáảãạăắằẳẵặâấầẩẫậèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵđ]'
        filtered_sentence = re.sub(pattern, '', res['sentence'])
        data['sentence'] = filtered_sentence
        return data
    else:
        return {"error": "Invalid data format"}


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('voz', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


with DAG('UIT-BIGDATA',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:


    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )