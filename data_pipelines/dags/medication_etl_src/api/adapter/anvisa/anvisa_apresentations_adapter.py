import pandas as pd
from dataclasses import dataclass
from medication_etl_src.entity.anvisa_entities import ProdutoApresentacaoAnvisa, ApresentacaoAnvisa, AcondicionamentoAnvisa


@dataclass
class ResultParseApresentacoesAnvisa:
    produtos: list[ProdutoApresentacaoAnvisa]
    apresentacoes: list[ApresentacaoAnvisa]
    acondicionamentos: list[AcondicionamentoAnvisa]


class AnvisaApresentationsAdapter:

    PRODUTO_MAPPER = {
        'codigoProduto': 'codigo_anvisa',
        'codigoNotificacao':'codigo_notificacao_anvisa',
        'classesTerapeuticas': 'classes_terapeuticas',
        #'numeroRegistro': 'numero_registro_anvisa',
        'apresentacoes': 'apresentacoes',
        'acondicionamentos': 'acondicionamentos',
    }

    APRESENTACOES_MAPPER = {
        'codigo': 'codigo_anvisa',
        'codigo_anvisa_medicamento': 'codigo_anvisa_medicamento',
        'apresentacao': 'apresentacao',
        'principiosAtivos': 'principios_ativos',
        'formasFarmaceuticas': 'formas_farmaceuticas',
        'qtdUnidadeMedida': 'quantidade',
        'viasAdministracao': 'via_administracao',
        'embalagemPrimariaTodas': 'embalagens_primarias',
        'embalagemSecundariaTodas': 'embalagens_secundarias',
        'fabricantesNacionais': 'fabricantes_nacionais',
        'fabricantesInternacionais': 'fabricantesInternacionais',
        'tipoAutorizacao': 'tipo_autorizacao_anvisa',
        'ativa': 'registro_ativo',
        'tarja': 'tarja',
    }

    
    ACONDICIONAMENTOS_MAPPER = {
        "codigo_notificacao_anvisa_medicamento" : "codigo_notificacao_anvisa_medicamento",
        'codigo': 'codigo_anvisa',
        'apresentacao': 'apresentacao',
        'volume': 'volume',
        'principiosAtivos': 'principios_ativos',
    }

    def extract_medicine_info(self, apresentacoes: list[dict]) -> list[ProdutoApresentacaoAnvisa]:

        apresentacoes = pd.DataFrame(apresentacoes)

        apresentacoes = apresentacoes[[k for k in self.PRODUTO_MAPPER]]
        apresentacoes = apresentacoes.rename(columns=self.PRODUTO_MAPPER)

        produtos = apresentacoes.copy()

        produtos = produtos.apply(lambda row: ProdutoApresentacaoAnvisa(**row), axis=1).tolist()

        return produtos

    def adapt(self, apresentacoes: list[dict]):

        apresentacoes = pd.DataFrame(apresentacoes)

        apresentacoes = apresentacoes[[k for k in self.PRODUTO_MAPPER]]
        apresentacoes = apresentacoes.rename(columns=self.PRODUTO_MAPPER)

        produtos = apresentacoes.copy()
        
        apresentacoes = produtos[["codigo_anvisa", "apresentacoes"]]
        apresentacoes = apresentacoes.dropna(subset="apresentacoes")
        apresentacoes = apresentacoes[apresentacoes["apresentacoes"].apply(lambda x: isinstance(x, list) and len(x) > 0)]
        if not apresentacoes.empty:
            apresentacoes = apresentacoes.explode("apresentacoes").reset_index(drop=True)
            codigos_anvisa = apresentacoes["codigo_anvisa"].copy()
            apresentacoes = pd.json_normalize(apresentacoes["apresentacoes"])
            apresentacoes["codigo_anvisa_medicamento"] = codigos_anvisa
            apresentacoes = apresentacoes[[k for k in self.APRESENTACOES_MAPPER]]
            apresentacoes = apresentacoes.rename(columns=self.APRESENTACOES_MAPPER)
            apresentacoes["via_administracao"] = apresentacoes["via_administracao"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
            apresentacoes = apresentacoes.apply(lambda row: ApresentacaoAnvisa(**row), axis=1).tolist()
        else:
            apresentacoes = []

        acondicionamentos = produtos[["codigo_notificacao_anvisa", "acondicionamentos"]]
        acondicionamentos = acondicionamentos[acondicionamentos["acondicionamentos"].apply(lambda x: isinstance(x, list) and len(x) > 0)]
        if not acondicionamentos.empty:
            acondicionamentos = acondicionamentos.explode("acondicionamentos").reset_index(drop=True)
            codigos_anvisa = acondicionamentos["codigo_notificacao_anvisa"].copy()
            acondicionamentos = pd.json_normalize(acondicionamentos["acondicionamentos"])
            acondicionamentos["codigo_notificacao_anvisa_medicamento"] = codigos_anvisa
            acondicionamentos = acondicionamentos[[k for k in self.ACONDICIONAMENTOS_MAPPER]]
            acondicionamentos = acondicionamentos.rename(columns=self.ACONDICIONAMENTOS_MAPPER)
            acondicionamentos = acondicionamentos.apply(lambda row: AcondicionamentoAnvisa(**row), axis=1).tolist()
        else:
            acondicionamentos = []

        produtos = produtos.drop(columns=["apresentacoes", "acondicionamentos"])
        produtos = produtos.apply(lambda row: ProdutoApresentacaoAnvisa(**row), axis=1).tolist()

        result = ResultParseApresentacoesAnvisa(
            produtos=produtos,
            apresentacoes=apresentacoes,
            acondicionamentos=acondicionamentos,
        )

        return result
