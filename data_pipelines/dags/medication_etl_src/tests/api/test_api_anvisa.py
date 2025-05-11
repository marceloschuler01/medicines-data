import unittest
from medication_etl_src.api.api_anvisa import ApiAnvisa
from medication_etl_src.utils.stealth_requests_wrapper import StealthSessionWrapper
import time
import numpy as np


class TestApiAnvisa(unittest.TestCase):

    def test_make_requests(self):

        api = ApiAnvisa()

        with StealthSessionWrapper() as session:

            params = {"filter[situacaoRegistro]": "V", "count": 10, "page": 1}

            #session.get("https://consultas.anvisa.gov.br")
            time.sleep(self._get_random_number(1, 3, 2))

            result = api._make_request(session=session, endpoint="/consulta/medicamento/produtos", headers=None, params=params, allow_retries=False)

            self.assertIsInstance(result, dict)
            self.assertTrue('content' in result)
            content = result['content']
            self.assertIsInstance(content, list)
            self.assertEqual(len(content), 10)

    def test_make_requests_with_pagination(self):

        api = ApiAnvisa()

        with StealthSessionWrapper() as session:

            params = {"filter[nomeProduto]": "tylenol", "filter[situacaoRegistro]": "V", "count": 1000, "page": 1}

            #session.get("https://consultas.anvisa.gov.br")
            time.sleep(self._get_random_number(1, 3, 2))
            
            result = api._make_request_with_pagination_with_new_session(endpoint="/consulta/medicamento/produtos", count_by_page=2, headers=None, params=params)

            self.assertIsInstance(result, list)

            #session.get("https://consultas.anvisa.gov.br")
            time.sleep(self._get_random_number(1, 3, 2))

            params = {"filter[nomeProduto]": "tylenol", "filter[situacaoRegistro]": "V"}

            expected_result = api._make_request(session=session, endpoint="/consulta/medicamento/produtos", headers=None, params=params, allow_retries=False)['content']

            if len(expected_result) < 3:
                raise Exception("Not a good test")

            self.assertEqual(len(result), len(expected_result))
            self.assertListEqual(expected_result, result)

    def test_get_presentations(self):

        medicines = [{
            "ordem": 1,
            "produto": {
                "codigo": 3652639,
                "nome": "TYLENOL",
                "numeroRegistro": "157211214",
                "dataVencimento": "2028-02-16T00:00:00.000-0300",
                "mesAnoVencimento": "022028",
                "dataVencimentoRegistro": "2028-02-01T00:00:00.000-0300",
                "principioAtivo": "PARACETAMOL",
                "situacaoApresentacao": "Ativo",
                "dataRegistro": "2023-10-16T00:00:00.000-0300",
                "tipoAutorizacao": "REGISTRADO",
                "codigoNotificacao": 0,
            },
            "empresa": {
                "cnpj": "59748988000114",
                "razaoSocial": "KENVUE LTDA.",
                "numeroAutorizacao": "1057211",
                "cnpjFormatado": "59.748.988/0001-14"
            },
            "processo": {
                "numero": "25351464826202349",
                "situacao": 29,
                "numeroProcessoFormatado": "25351.464826/2023-49"
            }
        },
        {
            "ordem": 3,
            "produto": {
                "codigo": 3651010,
                "nome": "TYLENOL DC",
                "numeroRegistro": "157211205",
                "dataVencimento": "2027-09-16T00:00:00.000-0300",
                "mesAnoVencimento": "092027",
                "dataVencimentoRegistro": "2027-09-01T00:00:00.000-0300",
                "principioAtivo": "PARACETAMOL, CAFEÍNA",
                "situacaoApresentacao": "Ativo",
                "tipoAutorizacao": "REGISTRADO",
                "codigoNotificacao": 0,
                "dataCancelamento": None,
                "mesAnoVencimentoFormatado": "09/2027",
                "numeroRegistroFormatado": "157211205",
                "acancelar": False
            },
            "empresa": {
                "cnpj": "59748988000114",
                "razaoSocial": "KENVUE LTDA.",
                "numeroAutorizacao": "1057211",
                "cnpjFormatado": "59.748.988/0001-14"
            },
            "processo": {
                "numero": "25351464218202334",
                "situacao": 29,
                "numeroProcessoFormatado": "25351.464218/2023-34"
            }
        },
        {
            "ordem": 2,
            "produto": {
                "codigo": 8544,
                "nome": "TYLENOL BEBÊ",
                "dataVencimento": "2034-01-08T00:00:00.000-0300",
                "mesAnoVencimento": "012034",
                "dataVencimentoRegistro": "2034-01-01T00:00:00.000-0300",
                "situacaoApresentacao": "Ativo",
                "dataRegistro": "2024-01-08T00:00:00.000-0300",
                "tipoAutorizacao": "NOTIFICADO",
                "descricaoMedicamentoNotificado": "PARACETAMOL 100 MG/ML (SUSPENSÃO) C",
                "categoriaMedicamentoNotificado": None,
                "codigoNotificacao": 53773,
                "mesAnoVencimentoFormatado": "01/2034",
                "numeroRegistroFormatado": "         ",
                "acancelar": False
            },
            "empresa": {
                "cnpj": "59748988000114",
                "razaoSocial": "KENVUE LTDA.",
                "numeroAutorizacao": "1057211",
                "cnpjFormatado": "59.748.988/0001-14"
            },
            "processo": {
                "numero": None,
                "situacao": 1,
                "numeroProcessoFormatado": None
            }
        }]

        produtos = [medicine['produto'] for medicine in medicines]

        api = ApiAnvisa()

        time.sleep(self._get_random_number(1, 3, 2))
        import copy
        presentations, errors = api.get_presentations(medicines=copy.deepcopy(produtos))

        self.assertEqual(len(presentations), 3)
        self.assertIsInstance(errors, list)
        self.assertEqual(len(errors), 0)

        self.assertEqual(presentations[0]['codigoProduto'], produtos[0]['codigo'])
        self.assertEqual(presentations[1]['codigoProduto'], produtos[1]['codigo'])
        self.assertEqual(presentations[2]['codigoNotificacao'], produtos[2]['codigoNotificacao'])

        self.assertIsInstance(presentations[0].get('apresentacoes'), list)
        self.assertIsInstance(presentations[0].get('acondicionamentos'), list)
        self.assertTrue(len(presentations[0]['apresentacoes']) > 0)
        self.assertEqual(len(presentations[0]['acondicionamentos']), 0)

        self.assertIsInstance(presentations[1].get('apresentacoes'), list)
        self.assertIsInstance(presentations[1].get('acondicionamentos'), list)
        self.assertTrue(len(presentations[1]['apresentacoes']) > 0)
        self.assertEqual(len(presentations[1]['acondicionamentos']), 0)

        self.assertIsInstance(presentations[2].get('apresentacoes'), list)
        self.assertIsInstance(presentations[2].get('acondicionamentos'), list)
        # Notificated medicines does not have presentations, but have acondicionamentos
        self.assertEqual(len(presentations[2]['apresentacoes']), 0)
        self.assertTrue(len(presentations[2]['acondicionamentos']) > 0)


    def test_get_presentations_with_errors(self):

        with_error_medicine = 145690

        medicines = [{
            "ordem": 1,
            "produto": {
                "codigo": 3652639,
                "nome": "TYLENOL",
                "numeroRegistro": "157211214",
                "situacaoApresentacao": "Ativo",
                "tipoAutorizacao": "REGISTRADO",
                "codigoNotificacao": 0,
            },
        },{
            "ordem": 2,
            "produto": {
                "codigo": with_error_medicine,
                "nome": "MENOP",
                "numeroRegistro": "118610226",
                "situacaoApresentacao": "Inativo",
                "tipoAutorizacao": "REGISTRADO",
                "codigoNotificacao": 0,
            },
        },
        {
            "ordem": 3,
            "produto": {
                "codigo": 3651010,
                "nome": "TYLENOL DC",
                "numeroRegistro": "157211205",
                "situacaoApresentacao": "Ativo",
                "tipoAutorizacao": "REGISTRADO",
                "codigoNotificacao": 0,
            },
        },
        {
            "ordem": 4,
            "produto": {
                "codigo": 8544,
                "nome": "TYLENOL BEBÊ",
                "situacaoApresentacao": "Ativo",
                "tipoAutorizacao": "NOTIFICADO",
                "codigoNotificacao": 53773,
            },
        }]

        produtos = [medicine['produto'] for medicine in medicines]

        api = ApiAnvisa()

        time.sleep(self._get_random_number(1, 3, 2))
        import copy
        presentations, errors = api.get_presentations(medicines=copy.deepcopy(produtos))

        self.assertEqual(len(presentations), 3)
        self.assertIsInstance(errors, list)
        self.assertEqual(len(errors), 1)

        self.assertEqual(presentations[0]['codigoProduto'], produtos[0]['codigo'])
        self.assertEqual(presentations[1]['codigoProduto'], produtos[2]['codigo'])
        self.assertEqual(presentations[2]['codigoNotificacao'], produtos[3]['codigoNotificacao'])

    def _get_random_number(self, min_number=None, max_number=None, mode=None):

        numbers = np.random.triangular(
            #left = min_number - 0.5,
            left = min_number,
            mode = mode,
            #right = max_number + 0.5,
            right = max_number,
            size=1
        )
        return numbers[0]
