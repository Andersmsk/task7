from datetime import datetime
import pymongo
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.task_group import TaskGroup
import pandas as pd
import re


def clean_data():
    df = pd.read_csv(r'/opt/airflow/data/tiktok_google_play_reviews.csv')
    df.fillna('-', inplace=True)
    df['at'] = pd.to_datetime(df['at'])
    df.sort_values(by='at', inplace=True)
    df['content'] = df['content'].apply(lambda x: re.sub(r'[^\w\s.,?!]', '', x))
    df.to_csv(r'/opt/airflow/data/cleaned_tiktok_reviews.csv', index=False)


def load_to_mongodb():
    connection_url = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
    database_name = connection_url["tik-tok"]
    print(database_name)

    collection_name = database_name["statistic"]
    print(collection_name)
    df = pd.read_csv(r'/opt/airflow/data/cleaned_tiktok_reviews.csv')
    data = df.to_dict(orient='records')
    collection_name.insert_many(data)

    connection_url.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 30),
}

with DAG(
        'tiktok_reviews_dag',
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
) as dag:
    # Создаем Sensor, который будет отслеживать появление файла

    file_sensor = FileSensor(
        task_id='check_for_file',
        filepath='tiktok_google_play_reviews.csv',
        poke_interval=10,
    )

    # Создаем TaskGroup для задач обработки данных
    with TaskGroup('data_processing') as data_processing:
        # Задача для очистки данных
        clean_data_task = PythonOperator(
            task_id='clean_data',
            python_callable=clean_data,
        )

        # Задача для загрузки данных в MongoDB
        load_to_mongodb_task = PythonOperator(
            task_id='load_to_mongodb',
            python_callable=load_to_mongodb,
        )

        clean_data_task >> load_to_mongodb_task

    # Задача завершения
    finish_task = DummyOperator(task_id='finish')

    # Определение последовательности задач
    file_sensor >> data_processing >> finish_task
