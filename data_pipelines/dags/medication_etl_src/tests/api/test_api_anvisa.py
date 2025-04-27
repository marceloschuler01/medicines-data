import unittest
from api.api_anvisa import ApiAnvisa
from utils.stealth_requests_wrapper import StealthSessionWrapper
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
            self.assertEquals(len(content), 10)

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

            self.assertEquals(len(result), len(expected_result))
            self.assertListEqual(expected_result, result)

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
