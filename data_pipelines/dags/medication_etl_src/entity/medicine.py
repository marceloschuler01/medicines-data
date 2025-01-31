from dataclasses import dataclass

@dataclass
class Medicine:

    codigo_anvisa: str = None
    nome_produto: str = None
    numero_registro: str = None
    numero_processo: str = None
    principio_ativo: str = None
    categoria_regulatoria: str = None
    medicamento_referencia: str = None
    cnpj_laboratorio: str = None
    razao_social_laboratorio: str = None
    registro_ativo: bool = None
    data_registro: str = None
    data_vencimento_registro: str = None
    tipo_autorizacao: str = None
