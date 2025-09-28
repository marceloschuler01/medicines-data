from medication_etl_src.api.api_anvisa import ApiAnvisa
from medication_etl_src.entity.anvisa_etities import MedicineAnvisa
import pandas as pd
from dataclasses import asdict


class GetMedicinesInfoAndStoreAsCsv:

    def __init__(self, api=ApiAnvisa()):
        self.api = api

    def get_medicines_info_and_store_as_csv(self):

        medicines: list[MedicineAnvisa] = self.api.get_medicines()
        medicines_df: pd.DataFrame = pd.DataFrame.from_records([asdict(s) for s in medicines])
        medicines_df.to_csv("/opt/airflow/dags/csvs/TEMP.csv", index=False)
