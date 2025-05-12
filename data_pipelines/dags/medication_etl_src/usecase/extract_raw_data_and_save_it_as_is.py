from medication_etl_src.api.api_anvisa import ApiAnvisa
import json
import pandas as pd

PATH_TO_SAVE_DATA="C://Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/temp_files/"

class GetRawDataAndSaveItAsIs():

    def __init__(self, api=ApiAnvisa, path_to_save_data=None):
        self.api = api()
        self.PATH_TO_SAVE_DATA = path_to_save_data or PATH_TO_SAVE_DATA
        self.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS: int = 200

    def get_raw_data_and_save_it_as_is(self):

        self.extract_and_save_active_medicines_data()
        self.extract_and_save_inactive_medicines_data()
        self.extract_and_save_regulatory_category()
        self.extract_and_save_pharmaceutic_forms()
        self.extract_and_save_presentations()
        self.extract_and_save_presentations_from_inactive_medicines()

    def extract_and_save_active_medicines_data(self):

        medicines_data = self.api.get_active_medicines()
        self.save_json(data=medicines_data, filename="active_medicines.json")

    def extract_and_save_inactive_medicines_data(self):

        medicines_data = self.api.get_inactive_medicines()
        self.save_json(data=medicines_data, filename="inactive_medicines.json")

    def extract_and_save_presentations(self):

        self.extract_and_save_presentations_from_medicines(medicines_table='active_medicines')

    def extract_and_save_presentations_from_inactive_medicines(self):

        self.extract_and_save_presentations_from_medicines(medicines_table='inactive_medicines')
    
    def extract_and_save_presentations_from_medicines(self, medicines_table: str):

        with open(self.PATH_TO_SAVE_DATA+self.get_current_date_as_str()+medicines_table+'.json', 'r', encoding="utf8") as f:
            medicines = json.load(f)
            medicines = [{'codigo': m['produto']['codigo'], 'codigoNotificacao': m['produto']['codigoNotificacao'], 'tipoAutorizacao': m['produto']['tipoAutorizacao']} for m in medicines]
            medicines = pd.DataFrame(medicines)

        registered_medicines = medicines[medicines['tipoAutorizacao'] != "NOTIFICADO"]
        notificated_medicines = medicines[medicines['tipoAutorizacao'] == "NOTIFICADO"]

        try:
            with open(self.PATH_TO_SAVE_DATA+self.get_current_date_as_str()+'presentations_from_'+medicines_table+'.json', 'r', encoding="utf8") as f:
                alredy_saved_medicines = json.load(f)
        except FileNotFoundError:
            alredy_saved_medicines = []


        try:
            with open(self.PATH_TO_SAVE_DATA+self.get_current_date_as_str()+medicines_table+'_presentation_error.json', 'r', encoding="utf8") as f:
                alredy_saved_errors= json.load(f)
        except FileNotFoundError:
            alredy_saved_errors = []

        for item in alredy_saved_errors:
            if 'codigoProduto' not in item:
                item['codigoProduto'] = item['codigo']
        alredy_saved_data = alredy_saved_medicines + alredy_saved_errors

        already_saved_registered_medicines = [m for m in alredy_saved_data if m['tipoAutorizacao'] != "NOTIFICADO"]
        already_saved_notificated_medicines = [m for m in alredy_saved_data if m['tipoAutorizacao'] == "NOTIFICADO"]
        already_readed_registered_codes = set([m['codigoProduto'] for m in already_saved_registered_medicines])
        already_readed_notification_codes = set([m['codigoNotificacao'] for m in already_saved_notificated_medicines])

        for item in alredy_saved_errors:
            if 'codigoProduto' in item:
                del item['codigoProduto']

        registered_medicines: pd.DataFrame = registered_medicines[~registered_medicines['codigo'].isin(already_readed_registered_codes)]
        registered_medicines: list[dict] = registered_medicines.to_dict(orient="records")

        notificated_medicines: pd.DataFrame = notificated_medicines[~notificated_medicines['codigoNotificacao'].isin(already_readed_notification_codes)]
        notificated_medicines: list[dict] = notificated_medicines.to_dict(orient="records")

        medicines = registered_medicines + notificated_medicines

        medicines_per_time: int = self.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS

        print(len(medicines), " medicines to be readed")
        to_be_saved_after = len(medicines) - medicines_per_time

        medicines = medicines[:medicines_per_time] if len(medicines) > medicines_per_time else medicines

        presentations, errors = self.api.get_presentations(medicines=medicines)
        if presentations:
            self.save_json(data=alredy_saved_medicines+presentations, filename='presentations_from_'+medicines_table+'.json')
        self.save_json(data=alredy_saved_errors+errors, filename=medicines_table+'_presentation_error.json')

        # del variables before call recursive function
        del medicines
        del presentations
        del alredy_saved_data
        del alredy_saved_errors
        del alredy_saved_medicines
        del already_readed_registered_codes
        del already_readed_notification_codes
        del registered_medicines
        del notificated_medicines
        del errors

        if to_be_saved_after > 0:
            return self.extract_and_save_presentations_from_medicines(medicines_table=medicines_table)
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
        return "2025-05-10"
