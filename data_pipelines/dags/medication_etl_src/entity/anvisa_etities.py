from dataclasses import dataclass

@dataclass
class MedicineAnvisa:

    codigo_anvisa: str
    codigo_notificacao_anvisa: str
    nome_produto: str
    numero_registro_anvisa: str
    numero_processo_anvisa: str
    tipo_autorizacao_anvisa: str
    medicamento_referencia: str
    empresa: dict
    registro_ativo: str
    data_registro_anvisa: str
    sinonimos: str
    data_vencimento_registro_anvisa: str
    categoria_regulatoria: str

