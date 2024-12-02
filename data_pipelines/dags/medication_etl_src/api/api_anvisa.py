import pandas as pd
import requests
from medication_etl_src.entity.medicine import Medicine
from medication_etl_src.api.adapter.anvisa.anvisa_medicines_adapter import AnvisaMedicinesAdapter


class ApiAnvisa:
    BASE_URL = "https://consultas.anvisa.gov.br/api"
    MAX_RETRIES = 3

    def __init__(self):
        self._times_retried: int

    def get_medicines(self) -> list[Medicine]:

        medicines: list[dict] = self._make_request_with_pagination(
            endpoint="/consulta/medicamento/produtos",
            params={"filter[situacaoRegistro]": "V", 'checkNotificado': 'false', 'checkRegistrado': 'true'},
            count_by_page=1000,
        )

        medicines: list[Medicine] = AnvisaMedicinesAdapter().adapt(medicines=medicines)

        return medicines 

    def _make_request_with_pagination(self, endpoint: str, count_by_page: int, headers: str | None=None, params: dict | None=None) -> list[dict]:
        
        self._times_retried = 0
        if params is None:
            params = {}
        
        params["count"] = count_by_page
        
        n_page = 0
        total_pages = 1

        result = []

        while n_page < total_pages:
            n_page += 1
            params["page"] = n_page
            res = self._make_request(endpoint=endpoint, headers=headers, params=params)
            total_pages = res['totalPages']
            result += res['content']

        return result
    
    def _make_request(self, endpoint, headers: str | None=None, params: dict | None=None) -> dict:

        url = self.BASE_URL + endpoint

        if params:
            url += "?"
            for param in params:
                url += param
                url += "="
                url += str(params[param])
                url += "&"
            url = url[:-1]

        if headers is None:
            headers = {"Authorization": "Guest"}
    
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            if self._times_retried < self.MAX_RETRIES:
                self._times_retried += 1
                return self._make_request(endpoint=endpoint, headers=headers, params=params)
            try:
                res_json = res.json()
            except Exception:
                res_json = ""
            raise Exception(f"Erro na integração com a API da Anvisa: \nstatus:{res.status_code}\nresponse:{res_json}")

        res_json = res.json()

        return res_json
