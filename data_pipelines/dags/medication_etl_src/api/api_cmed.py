import requests
from io import BytesIO
import pandas as pd

class ApiCMED:

    def get_preco_maximo_consumidor(self) -> pd.DataFrame:
        url = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/arquivos/xls_conformidade_site_20251007_180845178.xlsx/@@download/file"
        response = requests.get(url)

        if not response.ok:
            raise Exception("Bad Request. Status Code: {}".format(response.status_code))

        io = BytesIO(response.content)
        df = pd.read_excel(io, skiprows=41)

        return df

    def get_preco_maximo_governo(self) -> pd.DataFrame:

        url = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/arquivos/xls_conformidade_gov_20251007_180845178.xlsx/@@download/file"
        response = requests.get(url)

        if not response.ok:
            raise Exception("Bad Request. Status Code: {}".format(response.status_code))

        io = BytesIO(response.content)
        df = pd.read_excel(io, skiprows=52)

        return df
