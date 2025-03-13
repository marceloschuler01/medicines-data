import pandas as pd
import requests
from medication_etl_src.entity.medicine import Medicine
from medication_etl_src.api.adapter.anvisa.anvisa_medicines_adapter import AnvisaMedicinesAdapter
from stealth_requests import StealthSession
import random
import time
import numpy as np


class ApiAnvisa:
    BASE_URL = "https://consultas.anvisa.gov.br/api"
    MAX_RETRIES = 10

    def __init__(self):
        self._times_retried: int

    def get_medicines(self) -> list[Medicine]:

        active_medicines: list[dict] = self._make_request_with_pagination(
            endpoint="/consulta/medicamento/produtos",
            params={"filter[situacaoRegistro]": "V"},
            count_by_page=1500,
        )
        for a in active_medicines:
            a['registro_ativo'] = True

        not_active_medicines: list[dict] = self._make_request_with_pagination(
            endpoint="/consulta/medicamento/produtos",
            params={"filter[situacaoRegistro]": "C"},
            count_by_page=1500,
        )
        for a in not_active_medicines:
            a['registro_ativo'] = False

        all_medicines: list[dict] = not_active_medicines + active_medicines
        medicines: list[Medicine] = AnvisaMedicinesAdapter().adapt(medicines=all_medicines)

        return medicines

    def get_presentations(self, medicine_codes: list[str]) -> list:

        result = []

        total_medicines = len(medicine_codes)
        i = 0

        with StealthSession() as session:

            
            # First get the home page to make requests more stealthy
            session.get("https://consultas.anvisa.gov.br")
            time.sleep(self._get_random_number(1, 3, 2))

            # First get the home page to make requests more stealthy
            session.get("https://consultas.anvisa.gov.br/api/empresa/funcionamento?column=&count=10&filter%5BtipoProduto%5D=1&order=asc&page=1")
            time.sleep(self._get_random_number(1, 3, 2))

            for medicine in medicine_codes:

                self._times_retried = 0
                i += 1

                print("Getting presentation ", i, "of ", total_medicines)

                if i % random.randint(7, 15) == 0:
                    sleep_time = self._get_random_number(2, 5, 2)
                    print("Sleeping ", sleep_time, "seconds")
                    time.sleep(sleep_time)
                    # get the home page to make requests more stealthy
                    session.get("https://consultas.anvisa.gov.br/api/empresa/funcionamento?column=&count=10&filter%5BtipoProduto%5D=1&order=asc&page=1")
                    time.sleep(random.random())

                sleep_time = self._get_random_number(2, 10, 2)
                print("Sleeping ", sleep_time, "seconds")
                time.sleep(sleep_time)

                # Todo remove this try catch
                try:
                    presentations: list[dict] = self._make_request(
                        endpoint=f"/consulta/medicamento/produtos/codigo/{medicine}",
                        session=session
                    )
                except Exception:
                    print("QUEBROU QUEBROU QUEBROU")
                    break

                result.append(presentations)

        return result

    def _make_request_with_pagination(self, endpoint: str, count_by_page: int, headers: str | None=None, params: dict | None=None) -> list[dict]:
        
        with StealthSession() as session:

            # First get the home page to make requests more stealthy
            session.get("https://consultas.anvisa.gov.br")
            time.sleep(random.random())

            self._times_retried = 0
            if params is None:
                params = {}
            
            params["count"] = count_by_page
            
            n_page = 0
            total_pages = 1

            result = []

            while n_page < total_pages:
                print("page: ", n_page, " of ", total_pages)
                n_page += 1
                params["page"] = n_page
                sleep_time = self._get_random_number(2, 10, 2)
                print("Sleeping ", sleep_time, "seconds")
                time.sleep(sleep_time)
                res = self._make_request(session=session, endpoint=endpoint, headers=headers, params=params)
                total_pages = res['totalPages']
                result += res['content']

        return result
    
    def _make_request(self, session, endpoint, headers: str | None=None, params: dict | None=None) -> dict:

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
    
        print("Starting request to ", url)
        try:
            start_time = time.time()
            res = session.get(
                url,
                headers=headers
            )
            end_time = time.time()
        except Exception as e:
            if self._times_retried < self.MAX_RETRIES:
                time.sleep(10+random.randint(0, 10))
                print("Erro na requisição zzzzzz")
                return self._make_request(session=session, endpoint=endpoint, headers=headers, params=params)
            else:
                raise e
        print("Finished request to ", url, " with status ", res.status_code)
        print("Request took ", end_time - start_time, " seconds")

        if res.status_code != 200:
            if self._times_retried < self.MAX_RETRIES:
                self._times_retried += 1
                if res.status_code == 429:
                    time.sleep(60+random.randint(0, 10))
                    print("zzzzzzzz")
                return self._make_request(session=session, endpoint=endpoint, headers=headers, params=params)
            try:
                res_json = res.json()
            except Exception:
                res_json = ""
            raise Exception(f"Erro na integração com a API da Anvisa: \nstatus:{res.status_code}\nresponse:{res_json}")

        res_json = res.json()

        return res_json

    def _get_random_number(self, min_number=None, max_number=None, mode=None):

        numbers = np.random.triangular(
            left = min_number - 0.5,
            mode = mode,
            right = max_number + 0.5,
            size=1
        )
        return numbers[0]
