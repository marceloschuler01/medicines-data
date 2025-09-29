import pandas as pd
from dataclasses import dataclass
from medication_etl_src.entity.anvisa_etities import ProdutoApresentacaoAnvisa, ApresentacaoAnvisa, AcondicionamentoAnvisa


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
        'numeroRegistro': 'numero_registro_anvisa',
        'apresentacoes': 'apresentacoes',
        'acondicionamentos': 'acondicionamentos',
    }

    APRESENTACOES_MAPPER = {
        'codigo': 'codigo_anvisa',
        'apresentacao': 'apresentacao',
        'principiosAtivos': 'principios_ativos',
        'formasFarmaceuticas': 'formas_farmaceuticas',
        'qtdUnidadeMedida': 'volume_total_em_ml',
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
        'codigo': 'codigo_anvisa',
        'apresentacao': 'apresentacao',
        'volume': 'volume',
        'principiosAtivos': 'principios_ativos',
    }

    def adapt(self, apresentacoes: list[dict]):

        apresentacoes = pd.DataFrame(apresentacoes)

        apresentacoes = apresentacoes[[k for k in self.PRODUTO_MAPPER]]
        apresentacoes = apresentacoes.rename(columns=self.PRODUTO_MAPPER)

        produtos = apresentacoes.copy()
        
        apresentacoes = produtos[["codigo_anvisa", "apresentacoes"]]
        apresentacoes = apresentacoes.dropna(subset="apresentacoes")
        apresentacoes = apresentacoes.explode("apresentacoes").reset_index(drop=True)
        codigos_anvisa = apresentacoes["codigo_anvisa"].copy()
        apresentacoes = pd.json_normalize(apresentacoes["apresentacoes"])
        apresentacoes["codigo_anvisa_medicamento"] = codigos_anvisa
        apresentacoes = apresentacoes[[k for k in self.APRESENTACOES_MAPPER]]
        apresentacoes = apresentacoes.rename(columns=self.APRESENTACOES_MAPPER)
        apresentacoes = apresentacoes.apply(lambda row: ApresentacaoAnvisa(**row), axis=1).tolist()

        acondicionamentos = produtos[["codigo_anvisa", "acondicionamentos"]]
        acondicionamentos = acondicionamentos.explode("acondicionamentos").reset_index(drop=True)
        codigos_anvisa = acondicionamentos["codigo_anvisa"].copy()
        acondicionamentos = pd.json_normalize(acondicionamentos["acondicionamentos"])
        acondicionamentos["codigo_anvisa_medicamento"] = codigos_anvisa
        acondicionamentos = acondicionamentos[[k for k in self.ACONDICIONAMENTOS_MAPPER]]
        acondicionamentos = acondicionamentos.rename(columns=self.ACONDICIONAMENTOS_MAPPER)
        acondicionamentos = acondicionamentos.apply(lambda row: AcondicionamentoAnvisa(**row), axis=1).tolist()

        produtos = produtos.drop(columns=["apresentacoes", "acondicionamentos"])
        produtos = produtos.apply(lambda row: ProdutoApresentacaoAnvisa(**row), axis=1).tolist()

        result = ResultParseApresentacoesAnvisa(
            produtos=produtos,
            apresentacoes=apresentacoes,
            acondicionamentos=acondicionamentos,
        )

        return result
