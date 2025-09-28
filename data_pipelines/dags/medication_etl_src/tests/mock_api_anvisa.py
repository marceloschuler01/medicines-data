import pandas as pd
from medication_etl_src.entity.medicine import MedicineAnvisa
import json


class BadResultException(Exception):
    pass


class MockApiAnvisa:
    BASE_URL = "https://consultas.anvisa.gov.br/api"
    MAX_RETRIES = 2
    ENDPOINT_MEDICAMENTOS = "/consulta/medicamento/produtos"
    MOCKS_PATH = "C://Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/medication_etl_src/tests/mocks/"

    MEDICINES_WITH_ERROR = [145690, 22998,]
    NOTIFICATIONS_WITH_ERROR = [49756, 15841]

    def __init__(self, return_medicines_with_error=False):
        self.return_medicines_with_error = return_medicines_with_error

    def get_active_medicines(self) -> list[MedicineAnvisa]:

        if self.return_medicines_with_error:
            path = self.MOCKS_PATH+"mock_medicines_anvisa_with_errors.json"
        else:
            path = self.MOCKS_PATH+"mock_medicines_anvisa.json"

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        return data

    def get_inactive_medicines(self):

        if self.return_medicines_with_error:
            path = self.MOCKS_PATH+"mock_medicines_anvisa_with_errors.json"
        else:
            path = self.MOCKS_PATH+"mock_medicines_anvisa.json"

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for produto in data:
            produto['produto']['situacaoApresentacao'] = 'Inativo'

        return data

    def get_presentations(self, medicines: list[dict]) -> tuple[list, list]:

        errors = []

        codigos_medicamentos = []
        codigo_notificacoes = []

        for medicine in medicines:
            if medicine['tipoAutorizacao']== 'REGISTRADO':
                codigos_medicamentos.append(medicine['codigo'])
            else:
                codigo_notificacoes.append(medicine['codigoNotificacao'])

            if medicine['codigo'] in self.MEDICINES_WITH_ERROR:
                errors.append(medicine)

            if medicine['codigoNotificacao'] in self.NOTIFICATIONS_WITH_ERROR:
                errors.append(medicine)

        response = []
        with open(self.MOCKS_PATH+"mock_presentations_anvisa.json", 'r', encoding='utf-8') as f:
            data = json.load(f)

        for _t in data:
            if (
                _t['tipoAutorizacao'] == 'REGISTRADO' and _t['codigoProduto'] in codigos_medicamentos
                ) or (
                _t['tipoAutorizacao'] == 'NOTIFICADO' and _t['codigoNotificacao'] in codigo_notificacoes
                ):
                response.append(_t)

        return response, errors

    def get_regulation_category(self):

        with open(self.MOCKS_PATH+"mock_pharmaceutic_forms.json", 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data

    def get_pharmaceutic_forms(self):

        with open(self.MOCKS_PATH+"mock_regulatory_categories.json", 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data
