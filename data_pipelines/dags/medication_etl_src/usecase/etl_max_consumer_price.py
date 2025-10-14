import pandas as pd
import uuid
from dataclasses import asdict, dataclass

from medication_etl_src.api.adapter.cmed.cmed_prices_adapter import CMEDPricesAdapter
from medication_etl_src.database.api_database import ApiDatabase as sql

from medication_etl_src.database.db_connector import with_database_connection
from medication_etl_src.entity.cmed_entites import CmedPriceDefinition
from medication_etl_src.staging_db.staging_db import StagingDB




class ETLMaxConsumerPrice:

    def __init__(self, staging_db=StagingDB()):
        self.staging_db = staging_db

    @with_database_connection
    def main(self, conn=None):

        page = 1

        while True:

            data = self._read_from_staging_db(page=page)
            if not data:
                break

            print(f"Processing presentations max consumer price. Page:",
                  page, "with", len(data), "itens")
            self._extract_transform_and_load(data=data, conn=conn)

            page += 1

    def _read_from_staging_db(self, page: int) -> list[dict]:

        data = self.staging_db.select("preco_maximo_consumidor", page=page)

        return data

    @with_database_connection
    def _extract_transform_and_load(self, data: list[dict], conn=None):

        data: list[CmedPriceDefinition] = CMEDPricesAdapter().adapt(data)

        df_price_data = pd.DataFrame([asdict(x) for x in data])
        id_mapper = self._get_presentations_register_number_to_id_mapper(df_price_data=df_price_data, conn=conn)

        df_price_data["id_apresentacao_medicamento"] = df_price_data["numero_registro_anvisa"].map(id_mapper)

        self._update_presentations_with_cmed_info(df_price_data=df_price_data, conn=conn)

        pass


    def _update_presentations_with_cmed_info(self, df_price_data: pd.DataFrame, conn=None):

        df = df_price_data.copy()

        df = df[["id_apresentacao_medicamento", "ggrem", "ean_gtin", "ean_2", "regime_preco"]]

        sql.update_in_bulk(table_name="apresentacao_medicamento", data=df, filter_column="id_apresentacao_medicamento", conn=conn)

    @with_database_connection
    def _get_presentations_register_number_to_id_mapper(self, df_price_data: pd.DataFrame, conn=None) -> dict[str, str]:

        register_numbers = df_price_data["numero_registro_anvisa"].dropna().unique().tolist()
        filters = [sql.filter("numero_registro_anvisa", register_numbers, "IN")]

        presentations = sql.select_with_pandas("apresentacao_medicamento", filters=filters, columns=["id_apresentacao_medicamento", "numero_registro_anvisa"], conn=conn)

        return presentations.set_index("numero_registro_anvisa")["id_apresentacao_medicamento"].to_dict()


if __name__ == "__main__":
    ETLMaxConsumerPrice().main()
