import pandas as pd
import uuid
from dataclasses import asdict, dataclass

from medication_etl_src.database.api_database import ApiDatabase as sql
from medication_etl_src.database.db_connector import with_database_connection
from medication_etl_src.staging_db.staging_db import StagingDB
from medication_etl_src.api.adapter.anvisa.anvisa_medicines_adapter import AnvisaMedicinesAdapter
from medication_etl_src.entity.anvisa_entities import MedicineAnvisa



class ExtractTransformAndLoadFromStagingDBToMedicinesDB:

    def __init__(self, staging_db=StagingDB()):
        self.staging_db = staging_db

    @with_database_connection
    def main(self, conn=None):

        self._delete_all_old_medicines_data(conn=conn)
        medicines = self._read_from_staging_db_and_parse_data()

        df_medicines = pd.DataFrame([asdict(item) for item in medicines])
        del medicines

        df_medicines['id'] = [self._generate_uuid() for _ in range(len(df_medicines))]

        regulatory_categories = self._extract_regulatory_categories(df_medicines['categoria_regulatoria'])
        map_regulatory_categories_id = regulatory_categories.set_index('codigo_anvisa')['id_categoria_regulatoria'].to_dict()
        df_medicines['id_categoria_regulatoria'] = df_medicines['categoria_regulatoria'].apply(lambda x: map_regulatory_categories_id[x['codigo']] if isinstance(x, dict) and x['codigo'] else None)

        enterprises = self._extract_enterprises(df_medicines['empresa'])
        map_enterprises_id = enterprises.set_index('numero_autorizacao_anvisa')['id_empresa'].to_dict()
        df_medicines['id_empresa'] = df_medicines['empresa'].apply(lambda x: map_enterprises_id[x['numeroAutorizacao']] if isinstance(x, dict) and x['numeroAutorizacao'] else None)

        df_medicines = df_medicines.drop(columns=["categoria_regulatoria", "empresa"])
        df_medicines["id_medicamento"] = [self._generate_uuid() for _ in range(len(df_medicines))]

        LoadRegularoryCategoriesToDB().main(categories=regulatory_categories, conn=conn)
        LoadEnterprisesToDB().main(enterprises=enterprises, conn=conn)
        LoadMedicinesToDB().main(df_medicines=df_medicines, conn=conn)

        return df_medicines

    @with_database_connection
    def _delete_all_old_medicines_data(self, conn=None) -> None:

        print("Deleting all old medicines data...")
        sql.delete(table_name="forma_farmaceutica_apresentacao_medicamento", conn=conn)
        sql.delete(table_name="forma_farmaceutica", conn=conn)
        sql.delete(table_name="classe_terapeutica_medicamento", conn=conn)
        sql.delete(table_name="classe_terapeutica", conn=conn)
        sql.delete(table_name="medicamento", conn=conn)
        sql.delete(table_name="categoria_regulatoria", conn=conn)
        sql.delete(table_name="empresa", conn=conn)
        print("Delete completed.")

    def _read_from_staging_db_and_parse_data(self) -> list[MedicineAnvisa]:

        active_medicines = self.staging_db.select("active_medicines")
        inactive_medicines = self.staging_db.select("inactive_medicines")

        medicamentos = AnvisaMedicinesAdapter().adapt(active_medicines, registro_ativo=True)
        medicamentos_inativos = AnvisaMedicinesAdapter().adapt(inactive_medicines, registro_ativo=False)

        medicamentos = medicamentos + medicamentos_inativos

        return medicamentos

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

class LoadRegularoryCategoriesToDB:

    @with_database_connection
    def main(self, categories: pd.DataFrame, conn=None):

        sql.insert_with_copy(table_name="categoria_regulatoria", data=categories.to_dict(orient="records"), conn=conn)

class LoadEnterprisesToDB:

    @with_database_connection
    def main(self, enterprises: pd.DataFrame, conn=None):

        sql.insert_with_copy(table_name="empresa", data=enterprises.to_dict(orient="records"), conn=conn)



if __name__ == "__main__":
    ExtractTransformAndLoadFromStagingDBToMedicinesDB().main()
