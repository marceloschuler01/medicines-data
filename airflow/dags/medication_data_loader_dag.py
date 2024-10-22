from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

COLUMN_MAPPER = {
    'TIPO_PRODUTO': 'tipo_produto',
    'NOME_PRODUTO': 'nome',
    'CATEGORIA_REGULATORIA': 'categoria_regulatoria',
    'CLASSE_TERAPEUTICA': 'classe_terapeutica',
    'NUMERO_REGISTRO_PRODUTO' : 'numero_registro',
    'PRINCIPIO_ATIVO': 'principio_ativo',
    'codigo_laboratorio': 'codigo_laboratorio',
    'nome_laboratorio': 'nome_laboratorio',
}
SITUACAO_REGISTRO_VALIDO = 'VÁLIDO'

def extract_and_transform():

    print("Lendo CSV")

    df = pd.read_csv('/opt/airflow/dags/csvs/DADOS_ABERTOS_MEDICAMENTOS.csv', encoding="ISO-8859-1", delimiter=";", decimal=",")

    df = df[df['SITUACAO_REGISTRO'] == SITUACAO_REGISTRO_VALIDO]

    df['codigo_laboratorio'] = df['EMPRESA_DETENTORA_REGISTRO'].apply(_extract_laboratory_code_from_enterprise_value)
    df['nome_laboratorio'] =  df['EMPRESA_DETENTORA_REGISTRO'].apply(_extract_laboratory_name_from_enterprise_value)
    df = df[[k for k in COLUMN_MAPPER.keys()]]
    df = df.rename(columns=COLUMN_MAPPER)

    df.to_csv('temp.csv', encoding="UTF-8", sep=";" ,decimal=",", index=False)

def _extract_laboratory_code_from_enterprise_value(enterprise_value: str):
    if pd.isnull(enterprise_value):
        return
    if '-' not in enterprise_value:
        return None
    return enterprise_value.split('-')[0].strip()

def _extract_laboratory_name_from_enterprise_value(enterprise_value: str):
    if pd.isnull(enterprise_value):
        return
    if '-' not in enterprise_value:
        return enterprise_value
    return enterprise_value.split('-')[1].strip()

def load_to_database():

    df = pd.read_csv('temp.csv', encoding="UTF-8", delimiter=";", decimal=",")

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

    print("FInalizado Integração!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 19),  # Replace with the current date
    'retries': 1,
}

# Create the DAG
with DAG(
    dag_id='hello_world_dag',
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
