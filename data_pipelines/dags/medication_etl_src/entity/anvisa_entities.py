from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

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


@dataclass
class ApresentacaoAnvisa:

    codigo_anvisa: str
    codigo_anvisa_medicamento: str
    apresentacao: str
    principios_ativos: list[str]
    formas_farmaceuticas: list[str]
    volume_total_em_ml: Decimal
    via_administracao: str
    embalagens_primarias: list[dict]
    embalagens_secundarias: list[dict]
    fabricantes_nacionais: list[dict]
    fabricantesInternacionais: list[dict]
    tipo_autorizacao_anvisa: Literal['REGISTRADO', 'NOTIFICADO']
    registro_ativo: bool
    tarja: str


@dataclass
class AcondicionamentoAnvisa:
    codigo_anvisa: str
    codigo_notificacao_anvisa_medicamento: str
    apresentacao: str
    volume: str
    principios_ativos: list[str]


@dataclass
class ProdutoApresentacaoAnvisa:

    codigo_anvisa: str
    codigo_notificacao_anvisa: str
    classes_terapeuticas: list[str]
    #numero_registro_anvisa: str
