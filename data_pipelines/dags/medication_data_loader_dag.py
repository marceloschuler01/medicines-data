from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# ---------------------------------------------------------------------------
# Task callables
# Each function creates its own dependencies so that Airflow workers
# (which may run in separate processes) get fresh connections every time.
# ---------------------------------------------------------------------------

def _extract_medicines_data():
    """Step 1 – Extract medicines from Anvisa API and save to MongoDB."""
    from medication_etl_src.usecase.extract_raw_data_and_save_it_as_is import GetRawDataAndSaveItAsIs

    uc = GetRawDataAndSaveItAsIs()
    uc.extract_and_save_active_medicines_data()
    uc.extract_and_save_inactive_medicines_data()
    uc.extract_and_save_regulatory_category()
    uc.extract_and_save_pharmaceutic_forms()


def _extract_presentations_data():
    """Step 2 – Extract presentations from Anvisa API and save to MongoDB."""
    from medication_etl_src.usecase.extract_raw_data_and_save_it_as_is import GetRawDataAndSaveItAsIs

    uc = GetRawDataAndSaveItAsIs()
    uc.extract_and_save_presentations()
    uc.extract_and_save_presentations_from_inactive_medicines()


def _extract_cmed_data():
    """Step 3 – Extract CMED price data and save to MongoDB."""
    from medication_etl_src.usecase.extract_raw_data_and_save_it_as_is import GetRawDataAndSaveItAsIs

    uc = GetRawDataAndSaveItAsIs()
    uc.extract_preco_maximo_consumidor_data()
    uc.extract_preco_maximo_governo_data()


def _run_migrations():
    """Ensure PostgreSQL schema is up-to-date before any load task."""
    from medication_etl_src.run_migrations import run_migrations

    run_migrations()


def _transform_and_load_medicines():
    """Step 4 – Transform medicines from MongoDB and load into PostgreSQL."""
    from medication_etl_src.usecase.extract_transform_and_load_from_staging_db_to_medicines_db import (
        ExtractTransformAndLoadFromStagingDBToMedicinesDB,
    )

    ExtractTransformAndLoadFromStagingDBToMedicinesDB().main()


def _transform_and_load_presentations():
    """Step 5 – Transform presentations from MongoDB and load into PostgreSQL."""
    from medication_etl_src.usecase.etl_apresentacoes import ExtractTransformAndLoadApresentacoes

    ExtractTransformAndLoadApresentacoes().main()


def _transform_and_load_cmed():
    """Step 6 – Transform CMED prices from MongoDB and load into PostgreSQL."""
    from medication_etl_src.usecase.etl_max_price import ETLMaxPrice

    ETLMaxPrice().main()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="medicine-data-etl-dag",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
    description="ETL pipeline: Anvisa + CMED → MongoDB (staging) → PostgreSQL",
    tags=["medicines", "etl"],
) as dag:

    # -- Extraction tasks (Anvisa API / CMED → MongoDB) --------------------

    extract_medicines = PythonOperator(
        task_id="extract_medicines_data",
        python_callable=_extract_medicines_data,
    )

    extract_presentations = PythonOperator(
        task_id="extract_presentations_data",
        python_callable=_extract_presentations_data,
    )

    extract_cmed = PythonOperator(
        task_id="extract_cmed_data",
        python_callable=_extract_cmed_data,
    )

    # -- Schema migration (PostgreSQL) -------------------------------------

    run_migrations = PythonOperator(
        task_id="run_migrations",
        python_callable=_run_migrations,
    )

    # -- Transform & Load tasks (MongoDB → PostgreSQL) ---------------------

    transform_load_medicines = PythonOperator(
        task_id="transform_and_load_medicines",
        python_callable=_transform_and_load_medicines,
    )

    transform_load_presentations = PythonOperator(
        task_id="transform_and_load_presentations",
        python_callable=_transform_and_load_presentations,
    )

    transform_load_cmed = PythonOperator(
        task_id="transform_and_load_cmed",
        python_callable=_transform_and_load_cmed,
    )

    # -- Task dependencies (matching the pipeline diagram) -----------------
    #
    #  extract_medicines  ──►  extract_presentations  ──►  extract_cmed
    #                                                           │
    #                          run_migrations  ─────────────────┤
    #                                                           ▼
    #                                               transform_load_medicines
    #                                                           │
    #                                                           ▼
    #                                             transform_load_presentations
    #                                                           │
    #                                                           ▼
    #                                                transform_load_cmed

    extract_medicines >> extract_presentations >> extract_cmed
    [extract_cmed, run_migrations] >> transform_load_medicines
    transform_load_medicines >> transform_load_presentations
    transform_load_presentations >> transform_load_cmed
