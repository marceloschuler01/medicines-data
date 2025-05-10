import pandas as pd
from medication_etl_src.entity.medicine import Medicine
import json


class BadResultException(Exception):
    pass


class MockApiAnvisa:
    BASE_URL = "https://consultas.anvisa.gov.br/api"
    MAX_RETRIES = 2
    ENDPOINT_MEDICAMENTOS = "/consulta/medicamento/produtos"
    MOCKS_PATH = "C://Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/medication_etl_src/tests/mocks/"

    def __init__(self):
        pass

    def get_active_medicines(self) -> list[Medicine]:

        with open(self.MOCKS_PATH+"mock_medicines_anvisa.json", 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data

    def get_inactive_medicines(self):

        with open(self.MOCKS_PATH+"mock_medicines_anvisa.json", 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for produto in data:
            produto['produto']['situacaoApresentacao'] = 'Inativo'

        return data

    def get_presentations(self, medicines: list[dict]) -> list:

        codigos_medicamentos = []
        codigo_notificacoes = []

        for medicine in medicines:
            if medicine['tipoAutorizacao']== 'REGISTRADO':
                codigos_medicamentos.append(medicine['codigo'])
            else:
                codigo_notificacoes.append(medicine['codigoNotificacao'])

        response = []
        with open(self.MOCKS_PATH+"mock_presentations_anvisa.json", 'r', encoding='utf-8') as f:
            data = json.load(f)

        for _t in data:
            if (
                _t['tipoAutorizacao'] == 'REGISTRADO' and _t['codigoProduto'] in codigos_medicamentos
                ) or (
                _t['tipoAutorizacao'] == 'NOTIFICADO' and _t['codigoNotificacao'] == codigo_notificacoes
                ):
                response.append(_t)

        return response

    def get_regulation_category(self):

        pass

    def get_pharmaceutic_forms(self):

        pass
