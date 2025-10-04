import pandas as pd
import uuid
from dataclasses import asdict, dataclass

from medication_etl_src.database.api_database import ApiDatabase as sql
from medication_etl_src.database.db_connector import with_database_connection
from medication_etl_src.staging_db.staging_db import StagingDB
from medication_etl_src.api.adapter.anvisa.anvisa_apresentations_adapter import AnvisaApresentationsAdapter


@dataclass
class Result:
    medicines: pd.DataFrame
    regulatory_categories: pd.DataFrame
    enterprises: pd.DataFrame


class ExtractTransformAndLoadApresentacoes:

    def __init__(self, staging_db=StagingDB()):
        self.staging_db = staging_db

    def main(self) -> None:

        apresentacoes = self._read_from_staging_db(page=1)

        self._extract_transform_and_load(apresentacoes=apresentacoes)

    @with_database_connection
    def _extract_transform_and_load(self, apresentacoes: list[dict], conn=None):

        parsed_data = AnvisaApresentationsAdapter().adapt(apresentacoes=apresentacoes)

        df_presentations = pd.DataFrame([asdict(item) for item in parsed_data.apresentacoes])
        del parsed_data.apresentacoes

        df_presentations["id"] = [self._generate_uuid() for _ in range(len(df_presentations))]

        self._extract_other_entities_data_from_presentations(presentations=df_presentations)

    def _read_from_staging_db(self, page: int) -> list[dict]:

        presentations = self.staging_db.select("presentations_from_active_medicines", page=page, page_size=1000)

        return presentations

    def _extract_other_entities_data_from_presentations(self, presentations: pd.DataFrame):

        df_active_ingredients = self._add_missing_active_ingredients_and_return_all_active_ingredients(presentations=presentations)

        df_medicnes = self._get_medicines_from_presentations(presentations=presentations)


    def _add_missing_active_ingredients_and_return_all_active_ingredients(self, presentations: pd.DataFrame) -> pd.DataFrame:

        active_ingredients = presentations["principios_ativos"].explode().dropna().unique().tolist()

        df_active_ingredients = pd.DataFrame(active_ingredients, columns=["nome"])

        active_ingredients_database = sql.select_with_pandas("principio_ativo", columns=["id_principio_ativo", "nome"])

        missing_active_ingredients = df_active_ingredients[~df_active_ingredients["nome"].isin(active_ingredients_database["nome"])]
        missing_active_ingredients["id_principio_ativo"] = [self._generate_uuid() for _ in range(len(missing_active_ingredients))]

        if not missing_active_ingredients.empty:
            sql.insert_with_copy(table_name="principio_ativo", data=missing_active_ingredients.to_dict(orient="records"))

        df_active_ingredients = pd.concat([active_ingredients_database, missing_active_ingredients], ignore_index=True)

        # Filter only the active ingredients present in the current batch of presentations
        df_active_ingredients = df_active_ingredients[df_active_ingredients["nome"].isin(active_ingredients)]

        return df_active_ingredients

    def _get_medicines_from_presentations(self, presentations: pd.DataFrame) -> pd.DataFrame:

        medicines = sql.select_with_pandas("medicamento", columns=["id_medicamento", "codigo_anvisa"])
        return medicines

    def _extract_regulatory_categories(self, regulatory_categories: list[dict]) -> pd.DataFrame:

        regulatory_categories = [regulatory_category for regulatory_category in regulatory_categories if pd.notnull(regulatory_category) and regulatory_category]

        df_regulatory_categories = pd.DataFrame(regulatory_categories)

        # TODO: Verify why some categories are coming with 'codigo' as NaN
        df_regulatory_categories = df_regulatory_categories.dropna(subset=["codigo"])

        df_regulatory_categories = df_regulatory_categories.drop_duplicates(subset=["codigo"])

        df_regulatory_categories = df_regulatory_categories[["codigo", "descricao"]]

        df_regulatory_categories = df_regulatory_categories.rename(columns={"codigo": "codigo_anvisa"})
        df_regulatory_categories["id_categoria_regulatoria"] = [self._generate_uuid() for _ in range(len(df_regulatory_categories))]

        return df_regulatory_categories

    def _extract_enterprises(self, enterprises: list[dict]) -> pd.DataFrame:

        enterprises = [enterprise for enterprise in enterprises if pd.notnull(enterprise) and enterprise]

        df_enterprises = pd.DataFrame(enterprises)
        df_enterprises = df_enterprises.dropna(subset=["numeroAutorizacao"])
        df_enterprises = df_enterprises.drop_duplicates(subset=["numeroAutorizacao"])

        df_enterprises = df_enterprises[["cnpj", "razaoSocial", "numeroAutorizacao"]]

        df_enterprises = df_enterprises.rename(columns={
            "numeroAutorizacao": "numero_autorizacao_anvisa",
            "cnpj": "cnpj",
            "razaoSocial": "razao_social",
        }
        )
        df_enterprises["id_empresa"] = [self._generate_uuid() for _ in range(len(df_enterprises))]

        return df_enterprises

    @staticmethod
    def _generate_uuid() -> str:
        return str(uuid.uuid4())


class LoadMedicinesToDB:

    @with_database_connection
    def main(self, df_medicines: pd.DataFrame, conn=None):

        sql.insert_with_copy(table_name="medicamento", data=df_medicines.to_dict(orient="records"), conn=conn)



if __name__ == "__main__":
    ExtractTransformAndLoadApresentacoes().main()