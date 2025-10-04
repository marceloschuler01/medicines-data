import pandas as pd
from medication_etl_src.entity.anvisa_entities import MedicineAnvisa

class AnvisaMedicinesAdapter:

    COLUMN_MAPPER = {
        'codigo': 'codigo_anvisa',
        'codigoNotificacao':'codigo_notificacao_anvisa',
        'nome': 'nome_comercial',
        'numeroRegistro': 'numero_registro_anvisa',
        'numero': 'numero_processo_anvisa',
        'tipoAutorizacao': 'tipo_autorizacao_anvisa',
        'medicamentoReferencia': 'medicamento_referencia',
        'empresa': 'empresa',
        #'situacaoApresentacao': 'registro_ativo',
        'registro_ativo': 'registro_ativo',
        'dataRegistro': 'data_registro_anvisa',
        'sinonimos': 'sinonimos',
        'dataVencimentoRegistro': 'data_vencimento_registro_anvisa',
        'categoriaRegulatoria': 'categoria_regulatoria',
    }

    def adapt(self, medicines: list[dict], registro_ativo: bool) -> list[MedicineAnvisa]:

        _medicines_parsed = [medicine['produto'] for medicine in medicines]
        for i in range(len(_medicines_parsed)):
            _medicines_parsed[i].update(medicines[i]['processo'])
            _medicines_parsed[i]['empresa'] = medicines[i]['empresa']

        medicines_df = pd.DataFrame(_medicines_parsed)

        medicines_df['registro_ativo'] = registro_ativo

        medicines_df = medicines_df[[k for k in self.COLUMN_MAPPER]]
        medicines_df = medicines_df.rename(columns=self.COLUMN_MAPPER)

        medicines = medicines_df.apply(lambda row: MedicineAnvisa(**row), axis=1).tolist()

        return medicines
