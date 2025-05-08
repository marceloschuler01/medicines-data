import pandas as pd
import requests
from medication_etl_src.entity.medicine import Medicine
from medication_etl_src.api.adapter.anvisa.anvisa_medicines_adapter import AnvisaMedicinesAdapter
from stealth_requests import StealthSession
import random
import time
import numpy as np
import traceback
from medication_etl_src.utils.stealth_requests_wrapper import StealthSessionWrapper
from medication_etl_src.utils.retry_decorator import retry_decorator



class BadResultException(Exception):
    pass


class ApiAnvisa:
    BASE_URL = "https://consultas.anvisa.gov.br/api"
    MAX_RETRIES = 2
    ENDPOINT_MEDICAMENTOS = "/consulta/medicamento/produtos"

    def __init__(self):
        self._times_to_retry: int=self.MAX_RETRIES

    @retry_decorator(retry_num=3, retry_sleep_sec=20)
    def get_active_medicines(self) -> list[Medicine]:

        self._times_to_retry = 0

        active_medicines: list[dict] = self._make_request_with_pagination_with_new_session(
            endpoint=self.ENDPOINT_MEDICAMENTOS,
            params={"filter[situacaoRegistro]": "V"},
            count_by_page=1200,
        )

        return active_medicines

    @retry_decorator(retry_num=3, retry_sleep_sec=20)
    def get_inactive_medicines(self):

        self._times_to_retry = 0

        not_active_medicines: list[dict] = self._make_request_with_pagination_with_new_session(
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

            '''try:
                # First get the home page to make requests more stealthy
                session.get("https://consultas.anvisa.gov.br")
                time.sleep(self._get_random_number(1, 3, 2))

                # First get the home page to make requests more stealthy
                session.get("https://consultas.anvisa.gov.br/api/empresa/funcionamento?column=&count=10&filter%5BtipoProduto%5D=1&order=asc&page=1")
                time.sleep(self._get_random_number(1, 3, 2))
            except Exception:
                time.sleep(self._get_random_number(5, 10, 7))
                pass'''

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
                    self._times_to_retry = self.MAX_RETRIES
                except Exception:
                    print("Erro:")
                    print(traceback.format_exc())
                    if self._times_to_retry > 0:
                        self._times_to_retry -= 1
                        
                        time.sleep(self._get_random_number(1, 3, 2))
                        return result + self.get_presentations(medicines=medicines)
                    else:
                        return result

        return result

    @retry_decorator(retry_num=3, retry_sleep_sec=20)
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

    @retry_decorator(retry_num=3, retry_sleep_sec=20)
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

    def _make_request_with_pagination_with_new_session(self, endpoint: str, count_by_page: int, headers: str | None=None, params: dict | None=None, n_page=0, total_pages=1) -> list[dict]:
        
        with StealthSessionWrapper() as session:

            result = self._make_request_with_pagination(endpoint=endpoint, count_by_page=count_by_page, headers=headers, params=params, n_page=n_page, total_pages=total_pages, session=session)

        return result

    def _make_request_with_pagination(self, session: StealthSessionWrapper, endpoint: str, count_by_page: int, headers: str | None=None, params: dict | None=None, n_page=0, total_pages=1) -> list[dict]:
        
        time.sleep(random.random())

        if params is None:
            params = {}
        
        params["count"] = count_by_page

        result = []

        try:
            print("page: ", n_page, " of ", total_pages)
            n_page += 1
            params["page"] = n_page
            sleep_time = self._get_random_number(0.3, 1.2, 0.6)
            print("Sleeping ", sleep_time, "seconds")
            time.sleep(sleep_time)
            res = self._make_request(session=session, endpoint=endpoint, headers=headers, params=params)
            total_pages = res['totalPages']
            result += res['content']
        except Exception as e:
            if self._times_to_retry > 0:
                n_page -= 1
                self._times_to_retry -= 1
                return result + self._make_request_with_pagination_with_new_session(endpoint=endpoint, count_by_page=count_by_page, headers=headers, params=params, n_page=n_page, total_pages=total_pages)
            else:
                raise e

        if n_page < total_pages:
            return result + self._make_request_with_pagination(session=session, endpoint=endpoint, count_by_page=count_by_page, headers=headers, params=params, n_page=n_page, total_pages=total_pages)
    
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
            headers = self._default_headers()

        print("Starting request to ", url)

        try:
            start_time = time.time()
            res = session.get(
                url,
                headers=headers
            )
            end_time = time.time()

        except Exception as e:
            raise e

        print("Finished request to ", url, " with status ", res.status_code)
        print("Request took ", end_time - start_time, " seconds")

        if res.status_code != 200:
            try:
                res_json = res.json()
            except Exception:
                res_json = ""
            raise BadResultException(f"Erro na integração com a API da Anvisa: \nstatus:{res.status_code}\nresponse:{res_json}")

        res_json = res.json()

        return res_json
    
    def _default_headers(self):
        return {"Authorization": "Guest"}

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


