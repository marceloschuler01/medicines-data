import pandas as pd
from medication_etl_src.entity.medicine import Medicine

class AnvisaMedicinesAdapter:

    COLUMN_MAPPER = {
        'codigo': 'codigo_anvisa',
        'nome': 'nome_produto',
        'numeroRegistro': 'numero_registro',
        'numero': 'numero_processo',
        'principioAtivo': 'principio_ativo',
        'categoriaRegulatoria.descricao': 'categoria_regulatoria',
        'cnpj': 'cnpj_laboratorio',
        'razaoSocial': 'razao_social_laboratorio',
    }

    def adapt(self, medicines: list[dict]) -> list[Medicine]:

        medicines_df = pd.DataFrame(medicines)

        medicines_df = pd.concat([
            medicines_df.drop(columns=['produto', 'empresa', 'processo']),
            pd.json_normalize(medicines_df['produto']),
            pd.json_normalize(medicines_df['empresa']),
            pd.json_normalize(medicines_df['processo']),
        ], axis=1)

        medicines_df = medicines_df[[k for k in self.COLUMN_MAPPER]]
        medicines_df = medicines_df.rename(columns=self.COLUMN_MAPPER)

        medicines = medicines_df.apply(lambda row: Medicine(*row), axis=1).tolist()

        return medicines
