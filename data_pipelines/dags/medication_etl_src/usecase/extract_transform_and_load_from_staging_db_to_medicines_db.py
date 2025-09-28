import pandas as pd
import uuid
from dataclasses import asdict, dataclass


from medication_etl_src.staging_db.staging_db import StagingDB
from medication_etl_src.api.adapter.anvisa.anvisa_medicines_adapter import AnvisaMedicinesAdapter
from medication_etl_src.entity.anvisa_etities import MedicineAnvisa


@dataclass
class Result:
    medicines: pd.DataFrame
    regulatory_categories: pd.DataFrame
    enterprises: pd.DataFrame


class ExtractTransformAndLoadFromStagingDBToMedicinesDB:

    def __init__(self, staging_db=StagingDB()):
        self.staging_db = staging_db

    def main(self):

        medicines = self._read_from_staging_db_and_parse_data()

        df_medicines = pd.DataFrame([asdict(item) for item in medicines])
        del medicines

        df_medicines['id'] = [self._generate_uuid() for _ in range(len(df_medicines))]

        regulatory_categories = self._extract_regulatory_categories(df_medicines['categoria_regulatoria'])
        map_regulatory_categories_id = regulatory_categories.set_index('codigo_anvisa')['id'].to_dict()
        df_medicines['id_categoria_regulatoria'] = df_medicines['categoria_regulatoria'].apply(lambda x: map_regulatory_categories_id[x['codigo']] if isinstance(x, dict) and x['codigo'] else None)

        enterprises = self._extract_enterprises(df_medicines['empresa'])
        map_enterprises_id = enterprises.set_index('numero_autorizacao_anvisa')['id'].to_dict()
        df_medicines['id_empresa'] = df_medicines['empresa'].apply(lambda x: map_enterprises_id[x['numeroAutorizacao']] if isinstance(x, dict) and x['numeroAutorizacao'] else None)

        df_medicines = df_medicines.drop(columns=["categoria_regulatoria", "empresa"])

        return df_medicines

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
        df_regulatory_categories = df_regulatory_categories.drop_duplicates(subset=["codigo"])

        df_regulatory_categories = df_regulatory_categories[["codigo", "descricao"]]

        df_regulatory_categories = df_regulatory_categories.rename(columns={"codigo": "codigo_anvisa"})
        df_regulatory_categories["id"] = [self._generate_uuid() for _ in range(len(df_regulatory_categories))]

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
            "razaoSocial": "razaoSocial",
        }
        )
        df_enterprises["id"] = [self._generate_uuid() for _ in range(len(df_enterprises))]

        return df_enterprises

    @staticmethod
    def _generate_uuid() -> str:
        return str(uuid.uuid4())
