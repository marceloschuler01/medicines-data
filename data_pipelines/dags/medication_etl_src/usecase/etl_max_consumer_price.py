import pandas as pd
import uuid
from dataclasses import asdict, dataclass

from medication_etl_src.api.adapter.cmed.cmed_prices_adapter import CMEDPricesAdapter
from medication_etl_src.database.api_database import ApiDatabase as sql

from medication_etl_src.database.db_connector import with_database_connection
from medication_etl_src.entity.cmed_entites import CmedPriceDefinition
from medication_etl_src.staging_db.staging_db import StagingDB
from medication_etl_src.utils.split_tax_definition_from_string import split_tax_definition_from_string


class ETLMaxConsumerPrice:

    def __init__(self, staging_db=StagingDB()):
        self.staging_db = staging_db

    @with_database_connection
    def main(self, conn=None):

        self.max_consumer_price_etl(conn=conn)
        self.max_government_price_etl(conn=conn)

    def max_government_price_etl(self, conn=None):

        page = 1

        while True:

            data = self._read_government_price_from_stagin_db(page=page)
            if not data:
                break

            print(f"Processing presentations max government price. Page:",
                  page, "with", len(data), "itens")
            self._extract_transform_and_load(data=data, only_pmvg=True, conn=conn)

            page += 1

    def max_consumer_price_etl(self, conn=None):

        page = 1

        while True:

            data = self._read_consumer_price_from_staging_db(page=page)
            if not data:
                break

            print(f"Processing presentations max consumer price. Page:",
                  page, "with", len(data), "itens")
            self._extract_transform_and_load(data=data, conn=conn)

            page += 1

    def _read_consumer_price_from_staging_db(self, page: int) -> list[dict]:

        data = self.staging_db.select("preco_maximo_consumidor", page=page)

        return data

    def _read_government_price_from_stagin_db(self, page: int) -> list[dict]:

        data = self.staging_db.select("preco_maximo_governo", page=page)

        return data

    @with_database_connection
    def _extract_transform_and_load(self, data: list[dict], only_pmvg=False, conn=None):

        data: list[CmedPriceDefinition] = CMEDPricesAdapter().adapt(data)

        df_price_data = pd.DataFrame([asdict(x) for x in data])
        id_mapper = self._get_presentations_register_number_to_id_mapper(df_price_data=df_price_data, conn=conn)

        df_price_data["id_apresentacao_medicamento"] = df_price_data["numero_registro_anvisa"].map(id_mapper)

        self._update_presentations_with_cmed_info(df_price_data=df_price_data, conn=conn)

        df_price_medicines = self._transform_price_data(df_price_data=df_price_data)

        if only_pmvg:
            # Ignore other types of price definitions
            df_price_medicines = df_price_medicines[df_price_medicines['tipo_aliquota'].str.upper().str.contains('PMVG', na=False)]

        df_price_medicines['id_tipo_preco_maximo'] = self._get_id_tipo_preco_maximo_and_add_missing(df_price_medicines["tipo_aliquota"].tolist(), conn=conn)
        df_price_medicines['id_aliquota_imposto'] = self._get_id_aliquota_imposto_and_add_missing(df_price_medicines["porcentagem_aliquota"].tolist(), conn=conn)

        df_price_medicines = df_price_medicines.dropna(subset=["id_apresentacao_medicamento"])

        self._load_price_data(df_price_medicines=df_price_medicines, conn=conn)

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

    def _transform_price_data(self, df_price_data: pd.DataFrame) -> pd.DataFrame:

        df = df_price_data.copy()

        df = (
            df
            .set_index(['id_apresentacao_medicamento'])
            ['aliquotas']
            .apply(pd.Series)
            .stack()
            .reset_index()
            .rename(columns={'level_1': 'tipo', 0: 'valor_maximo'})
        )

        df['valor_maximo'] = df['valor_maximo'].apply(lambda x: x.replace(',', '.').replace('*', '')).astype('float')

        df[['tipo_aliquota', 'porcentagem_aliquota']] = (
            df['tipo']
            .apply(split_tax_definition_from_string)  # retorna tupla
            .apply(pd.Series)  # transforma em colunas separadas
        )

        return df

    @with_database_connection
    def _get_id_tipo_preco_maximo_and_add_missing(self, tipos_preco_maximo: list[str], conn=None) -> list[str]:

        types_in_db = sql.select("tipo_preco_maximo", filters=sql.filter("nome", tipos_preco_maximo, "IN"), columns=["id_tipo_preco_maximo", "nome"], conn=conn)
        missing_types = set(tipos_preco_maximo) - set([x['nome'] for x in types_in_db])

        df_missing_types = pd.DataFrame({"nome": list(missing_types), "id_tipo_preco_maximo": [str(uuid.uuid4()) for _ in range(len(missing_types))]})

        if not df_missing_types.empty:
            sql.insert_with_copy(table_name="tipo_preco_maximo", data=df_missing_types.to_dict(orient="records"), conn=conn)

        all_types = types_in_db + df_missing_types.to_dict(orient="records")
        type_mapper = {x['nome']: x['id_tipo_preco_maximo'] for x in all_types}

        return [type_mapper[t] for t in tipos_preco_maximo]

    @with_database_connection
    def _get_id_aliquota_imposto_and_add_missing(self, aliquotas_imposto: list[float], conn=None) -> list[str]:

        types_in_db = sql.select("aliquota_imposto", filters=sql.filter("porcentagem_aliquota", aliquotas_imposto, "IN"), columns=["id_aliquota_imposto", "porcentagem_aliquota"], conn=conn)
        missing_percentages = set(aliquotas_imposto) - set([x['porcentagem_aliquota'] for x in types_in_db])

        df_missing_types = pd.DataFrame({"porcentagem_aliquota": list(missing_percentages), "id_aliquota_imposto": [str(uuid.uuid4()) for _ in range(len(missing_percentages))]})

        if not df_missing_types.empty:
            sql.insert_with_copy(table_name="aliquota_imposto", data=df_missing_types.to_dict(orient="records"), conn=conn)

        all_types = types_in_db + df_missing_types.to_dict(orient="records")
        type_mapper = {x['porcentagem_aliquota']: x['id_aliquota_imposto'] for x in all_types}

        return [type_mapper[t] for t in aliquotas_imposto]

    def _load_price_data(self, df_price_medicines: pd.DataFrame, conn=None):

        sql.insert_with_copy("preco_maximo_apresentacao_medicamento", data=df_price_medicines.to_dict(orient="records"), conn=conn)

if __name__ == "__main__":
    ETLMaxConsumerPrice().main()
