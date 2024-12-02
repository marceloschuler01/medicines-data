from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from medication_etl_src.usecase.get_medicines_info_and_store_as_csv import GetMedicinesInfoAndStoreAsCsv


def extract_and_transform():

    GetMedicinesInfoAndStoreAsCsv().get_medicines_info_and_store_as_csv()

def load_to_database():

    df = pd.read_csv("/opt/airflow/dags/csvs/TEMP.csv")

    print("È o novo")

    print("coniectandoo postgres")

    # Connect to PostgreSQL
    '''conn = psycopg2.connect(
        host="postgres-container",  # Use container name here
        database="staging-db",
        user="root",
        password="1234"
    )'''
    #cur = conn.cursor()

    engine = create_engine('postgresql+psycopg2://root:1234@postgres-container/staging-db?client_encoding=utf8')

    df.to_sql(name="medicamentos", con=engine, if_exists="replace", index=False)

    #conn.commit()
    #cur.close()
    #conn.close()

    engine.dispose()

    print("FInalizado Integração 2!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 19),  # Replace with the current date
    'retries': 1,
}

# Create the DAG
with DAG(
    dag_id='medicine-data-etl-dag',
    default_args=default_args,
    schedule_interval='@daily',  # Set the schedule interval (here: daily)
    catchup=False
) as dag:
    
    # Define the task using PythonOperator
    task_extract_and_transform = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform
    )

    # Define the task using PythonOperator
    task_load_to_database = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database
    )

    task_extract_and_transform >> task_load_to_database
