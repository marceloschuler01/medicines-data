import pandas as pd
import uuid
from dataclasses import asdict, dataclass

from medication_etl_src.database.api_database import ApiDatabase as sql
from medication_etl_src.database.db_connector import with_database_connection
from medication_etl_src.staging_db.staging_db import StagingDB
from medication_etl_src.api.adapter.anvisa.anvisa_apresentations_adapter import AnvisaApresentationsAdapter
from medication_etl_src.utils import extract_composition_from_presentation_string
from medication_etl_src.utils.extract_composition_from_presentation_string import ItemComposicao


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

        df_presentations["id_apresentacao_medicamento"] = [self._generate_uuid() for _ in range(len(df_presentations))]

        df_presentations["codigo_anvisa_medicamento"] = df_presentations["codigo_anvisa_medicamento"].astype(str)

        medicines_codigo_anvisa_to_id_mapper = self._get_medicines_from_presentations_id_mapper(presentations=df_presentations, conn=conn)

        df_presentations["id_medicamento"] = df_presentations["codigo_anvisa_medicamento"].map(medicines_codigo_anvisa_to_id_mapper)

        # TODO, DO NOT DROP
        df_presentations.drop(columns=["volume_total_em_ml", "fabricantes_nacionais", "fabricantesInternacionais"], inplace=True)
        df_presentations["volume_total_em_ml"] = 0.10

        LoadPresentationsToDB().main(df_presentations=df_presentations, conn=conn)

        df_presentations = self._extract_other_entities_data_from_presentations(presentations=df_presentations, conn=conn)

    def _read_from_staging_db(self, page: int) -> list[dict]:

        presentations = self.staging_db.select("presentations_from_active_medicines", page=page, page_size=1000)

        return presentations

    @with_database_connection
    def _extract_other_entities_data_from_presentations(self, presentations: pd.DataFrame, conn=None):

        df_active_ingredients = self._add_missing_active_ingredients_and_return_all_active_ingredients(presentations=presentations, conn=conn)
        self._extract_compositions_and_load_to_database(presentations=presentations, active_medicines=df_active_ingredients, conn=conn)

        return presentations

    @with_database_connection
    def _add_missing_active_ingredients_and_return_all_active_ingredients(self, presentations: pd.DataFrame, conn=None) -> pd.DataFrame:

        active_ingredients = presentations["principios_ativos"].explode().dropna().unique().tolist()

        df_active_ingredients = pd.DataFrame(active_ingredients, columns=["nome"])

        active_ingredients_database = sql.select_with_pandas("principio_ativo", columns=["id_principio_ativo", "nome"], conn=conn)

        missing_active_ingredients = df_active_ingredients[~df_active_ingredients["nome"].isin(active_ingredients_database["nome"])]
        missing_active_ingredients["id_principio_ativo"] = [self._generate_uuid() for _ in range(len(missing_active_ingredients))]

        if not missing_active_ingredients.empty:
            sql.insert_with_copy(table_name="principio_ativo", data=missing_active_ingredients.to_dict(orient="records"), conn=conn)

        df_active_ingredients = pd.concat([active_ingredients_database, missing_active_ingredients], ignore_index=True)

        # Filter only the active ingredients present in the current batch of presentations
        df_active_ingredients = df_active_ingredients[df_active_ingredients["nome"].isin(active_ingredients)]

        return df_active_ingredients

    @with_database_connection
    def _extract_compositions_and_load_to_database(self, presentations: pd.DataFrame, active_medicines: pd.DataFrame, conn=None) -> None:

        compositions = self._extract_composition_from_presentations(presentations=presentations, active_medicines=active_medicines)

        #sql.delete(table_name="composicao_apresentacao_medicamento", filters=sql.filter("id_apresentacao_medicamento", presentations["id_apresentacao_medicamento"].tolist(), "IN"))

        # TODO these are compositions that could not be parsed correctly, correct it
        compositions = compositions.dropna(subset=["dosagem"])

        sql.insert_with_copy(table_name="composicao_apresentacao_medicamento", data=compositions.to_dict(orient="records"), conn=conn)

    def _extract_composition_from_presentations(self, presentations: pd.DataFrame, active_medicines: pd.DataFrame) -> pd.DataFrame:

        presentations = presentations[["id_apresentacao_medicamento", "apresentacao", "principios_ativos"]].copy()

        compositions: list[ItemComposicao] = []

        for row in presentations.itertuples(index=False):
            extracted_compositions = extract_composition_from_presentation_string(row.apresentacao, row.principios_ativos, row.id_apresentacao_medicamento)
            compositions.extend(extracted_compositions)

        compositions: list[dict] = [asdict(item) for item in compositions]

        compositions: pd.DataFrame = pd.DataFrame(compositions)

        id_active_ingredient_mapper = active_medicines.set_index("nome")["id_principio_ativo"].to_dict()
        compositions["id_principio_ativo"] = compositions["principio_ativo"].map(id_active_ingredient_mapper)

        return compositions[["id_apresentacao_medicamento", "id_principio_ativo", "dosagem", "unidade_de_medida"]]

    @with_database_connection
    def _get_medicines_from_presentations_id_mapper(self, presentations: pd.DataFrame, conn=None) -> dict[str, str]:

        codigos_medicamento = presentations["codigo_anvisa_medicamento"].dropna().unique().tolist()

        medicines = sql.select_with_pandas("medicamento", filters=sql.filter("codigo_anvisa", codigos_medicamento, "IN"), columns=["id_medicamento", "codigo_anvisa"], conn=conn)

        return medicines.set_index("codigo_anvisa")["id_medicamento"].to_dict()

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


class LoadPresentationsToDB:

    @with_database_connection
    def main(self, df_presentations: pd.DataFrame, conn=None):

        sql.insert_with_copy(table_name="apresentacao_medicamento", data=df_presentations.to_dict(orient="records"), conn=conn)


if __name__ == "__main__":
    ExtractTransformAndLoadApresentacoes().main()