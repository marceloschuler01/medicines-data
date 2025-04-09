from medication_etl_src.api.api_anvisa import ApiAnvisa
import sys
import datetime
import json
import pandas as pd
import random

class GetRawDataAndSaveItAsIs():

    #PATH_TO_SAVE_DATA=sys.path[1]+"\\dags\\medication_etl_src\\temp_files\\"
    PATH_TO_SAVE_DATA="C://Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/temp_files/"

    def __init__(self, api=ApiAnvisa):
        self.api = api()

    def get_raw_data_and_save_it_as_is(self):

        #self.extract_and_save_active_medicines_data()
        #self.extract_and_save_inactive_medicines_data()
        #self.extract_and_save_regulatory_category()
        #self.extract_and_save_pharmaceutic_forms()
        #self.extract_and_save_presentations()
        self.extract_and_save_presentations_from_inactive_medicines()

    def extract_and_save_active_medicines_data(self):

        medicines_data = self.api.get_active_medicines()
        self.save_json(data=medicines_data, filename="active_medicines.json")

    def extract_and_save_inactive_medicines_data(self):

        medicines_data = self.api.get_inactive_medicines()
        self.save_json(data=medicines_data, filename="inactive_medicines.json")

    def extract_and_save_presentations(self):

        # From active medicines
        with open('C://Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/temp_files/2025-03-30active_medicines.json', 'r', encoding="utf8") as f:
            medicines = json.load(f)
            medicines = [{'codigo': m['produto']['codigo'], 'codigoNotificacao': m['produto']['codigoNotificacao']} for m in medicines]
            medicines = pd.DataFrame(medicines)

        try:
            with open('C:/Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/temp_files/2025-03-30presentations_from_active_medicines.json', 'r', encoding="utf8") as f:
                alredy_saved_data = json.load(f)
                already_readed_codes = set([m['codigoProduto'] for m in alredy_saved_data])
        except FileNotFoundError:
            alredy_saved_data = []
            already_readed_codes = []

        #medicine_codes: list = list(medicine_codes - already_readed_codes)
        medicines: pd.DataFrame = medicines[~medicines['codigo'].isin(already_readed_codes)]
        medicines: list[dict] = medicines.to_dict(orient="records")
        random.shuffle(medicines)

        medicines_per_time: int = 100

        print(len(medicines), " medicines to be readed")
        to_be_saved_after = len(medicines) - medicines_per_time

        medicines = medicines[:medicines_per_time] if len(medicines) > medicines_per_time else medicines

        presentations = self.api.get_presentations(medicines=medicines)
        if presentations:
            self.save_json(data=alredy_saved_data+presentations, filename="presentations_from_active_medicines.json")

        # del variables before call recursive function
        del medicines
        del presentations
        del alredy_saved_data
        del already_readed_codes
    
        if to_be_saved_after > 0:
            return self.extract_and_save_presentations()
        return "Finalizado"

    def extract_and_save_presentations_from_inactive_medicines(self):

        # From active medicines
        with open('C://Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/temp_files/2025-03-30inactive_medicines.json', 'r', encoding="utf8") as f:
            medicines = json.load(f)
            medicines = [{'codigo': m['produto']['codigo'], 'codigoNotificacao': m['produto']['codigoNotificacao']} for m in medicines]
            medicines = pd.DataFrame(medicines)

        try:
            with open('C:/Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/temp_files/2025-03-30presentations_from_inactive_medicines.json', 'r', encoding="utf8") as f:
                alredy_saved_data = json.load(f)
                already_readed_codes = set([m['codigoProduto'] for m in alredy_saved_data])
        except FileNotFoundError:
            alredy_saved_data = []
            already_readed_codes = []

        #medicine_codes: list = list(medicine_codes - already_readed_codes)
        medicines: pd.DataFrame = medicines[~medicines['codigo'].isin(already_readed_codes)]
        medicines: list[dict] = medicines.to_dict(orient="records")
        random.shuffle(medicines)

        medicines_per_time: int = 300

        print(len(medicines), " medicines to be readed")
        to_be_saved_after = len(medicines) - medicines_per_time

        medicines = medicines[:medicines_per_time] if len(medicines) > medicines_per_time else medicines

        presentations = self.api.get_presentations(medicines=medicines)
        if presentations:
            self.save_json(data=alredy_saved_data+presentations, filename="presentations_from_inactive_medicines.json")

        # del variables before call recursive function
        del medicines
        del presentations
        del alredy_saved_data
        del already_readed_codes
    
        if to_be_saved_after > 0:
            return self.extract_and_save_presentations_from_inactive_medicines()
        return "Finalizado"

    def extract_and_save_regulatory_category(self):

        categories = self.api.get_regulation_category()
        self.save_json(data=categories, filename="regulatory_categories.json")

    def extract_and_save_pharmaceutic_forms(self):

        categories = self.api.get_pharmaceutic_forms()
        self.save_json(data=categories, filename="pharmaceutic_forms.json")

    def save_json(self, data: list[dict] | dict, filename: str):

        file_path = self.PATH_TO_SAVE_DATA+self.get_current_date_as_str()+filename
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def get_current_date_as_str(self) -> str:

        #return datetime.date.today().strftime("%Y-%m-%d")
        return "2025-03-30"
