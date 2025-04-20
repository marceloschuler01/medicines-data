import pandas as pd
import requests
from medication_etl_src.entity.medicine import Medicine
from medication_etl_src.api.adapter.anvisa.anvisa_medicines_adapter import AnvisaMedicinesAdapter
from stealth_requests import StealthSession
import random
import time
import numpy as np
import traceback


class ApiAnvisa:
    BASE_URL = "https://consultas.anvisa.gov.br/api"
    MAX_RETRIES = 2

    def __init__(self):
        self._times_retried: int=0

    def get_active_medicines(self) -> list[Medicine]:

        active_medicines: list[dict] = self._make_request_with_pagination(
            endpoint="/consulta/medicamento/produtos",
            params={"filter[situacaoRegistro]": "V"},
            count_by_page=1200,
        )
        #for a in active_medicines:
        #    a['registro_ativo'] = True

        #not_active_medicines: list[dict] = self._make_request_with_pagination(
        #    endpoint="/consulta/medicamento/produtos",
        #    params={"filter[situacaoRegistro]": "C"},
        #    count_by_page=1500,
        #)
        #for a in not_active_medicines:
        #    a['registro_ativo'] = False

        #all_medicines: list[dict] = not_active_medicines + active_medicines
        #medicines: list[Medicine] = AnvisaMedicinesAdapter().adapt(medicines=all_medicines)

        return active_medicines

    def get_inactive_medicines(self):

        not_active_medicines: list[dict] = self._make_request_with_pagination(
            endpoint="/consulta/medicamento/produtos",
            params={"filter[situacaoRegistro]": "C"},
            count_by_page=1200,
        )

        return not_active_medicines

    def get_presentations(self, medicines: list[dict]) -> list:

        result = []

        total_medicines = len(medicines)
        i = 0

        with StealthSession() as session:

            try:
                # First get the home page to make requests more stealthy
                session.get("https://consultas.anvisa.gov.br")
                time.sleep(self._get_random_number(1, 3, 2))

                # First get the home page to make requests more stealthy
                session.get("https://consultas.anvisa.gov.br/api/empresa/funcionamento?column=&count=10&filter%5BtipoProduto%5D=1&order=asc&page=1")
                time.sleep(self._get_random_number(1, 3, 2))
            except Exception:
                time.sleep(self._get_random_number(5, 10, 7))
                pass

            while medicines:

                medicine = medicines.pop(0)

                i += 1

                print("Getting presentation ", i, "of ", total_medicines)

                if i % random.randint(7, 15) == 0:
                    sleep_time = self._get_random_number(0.5, 1, 0.7)
                    print("Sleeping ", sleep_time, "seconds")
                    time.sleep(sleep_time)
                    # get the home page to make requests more stealthy
                    try:
                        session.get("https://consultas.anvisa.gov.br/api/empresa/funcionamento?column=&count=10&filter%5BtipoProduto%5D=1&order=asc&page=1")
                    except Exception:
                        time.sleep(self._get_random_number(5, 10, 7))
                    time.sleep(random.random())

                #sleep_time = self._get_random_number(0.2, 0.7, 0.4)
                sleep_time = self._get_random_number(0.1, 0.4, 0.2)
                print("Sleeping ", sleep_time, "seconds")
                time.sleep(sleep_time)

                # Todo remove this try catch
                try:
                    presentations: list[dict] = self._make_request(
                        endpoint=f"/consulta/medicamento/produtos/codigo/{medicine['codigo']}",
                        params={"codigoNotificacao":medicine['codigoNotificacao']} if medicine['codigoNotificacao'] else None,
                        session=session,
                        allow_retries=False,
                    )
                    result.append(presentations)
                    self._times_retried = 0
                except Exception:
                    print("Erro:")
                    print(traceback.format_exc())
                    self._times_retried += 1
                    if self._times_retried < self.MAX_RETRIES:
                        self._times_retried += 1
                        return result + self.get_presentations(medicines=medicines)
                    else:
                        return result

        return result

    def get_regulation_category(self):

        with StealthSession() as session:

            
            # First get the home page to make requests more stealthy
            session.get("https://consultas.anvisa.gov.br")
            time.sleep(self._get_random_number(1, 3, 2))

            categories = self._make_request(
                endpoint="/tipoCategoriaRegulatoria",
                session=session,
            )

        return categories


    def get_pharmaceutic_forms(self):

        with StealthSession() as session:

            # First get the home page to make requests more stealthy
            session.get("https://consultas.anvisa.gov.br")
            time.sleep(self._get_random_number(1, 3, 2))

            pharmaceutic_forms = self._make_request(
                endpoint="/formafarmaceutica/formasFisicas",
                session=session,
            )

        return pharmaceutic_forms

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
    
    def _make_request(self, session, endpoint, headers: str | None=None, params: dict | None=None, allow_retries=True) -> dict:

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
            if self._times_retried < self.MAX_RETRIES and allow_retries:
                self._times_retried += 1
                time.sleep(10+random.randint(0, 10))
                print("Erro na requisição zzzzzz")
                print(traceback.format_exc())
                return self._make_request(session=session, endpoint=endpoint, headers=headers, params=params)
            else:
                raise e
        print("Finished request to ", url, " with status ", res.status_code)
        print("Request took ", end_time - start_time, " seconds")

        if res.status_code != 200:
            if self._times_retried < self.MAX_RETRIES and allow_retries:
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
            #left = min_number - 0.5,
            left = min_number,
            mode = mode,
            #right = max_number + 0.5,
            right = max_number,
            size=1
        )
        return numbers[0]
