import unittest
from medication_etl_src.usecase.extract_raw_data_and_save_it_as_is import GetRawDataAndSaveItAsIs
from medication_etl_src.tests.mock_api_anvisa import MockApiAnvisa
import os
import shutil
import json
import pandas as pd

class TestExtractRawDataAndSaveItAsIs(unittest.TestCase):

    path_to_save_temp_tests_files="medication_etl_src/tests/_temp_files/"

    @classmethod
    def setUpClass(cls):
        try:
            os.mkdir(cls.path_to_save_temp_tests_files[:-1])
        except FileExistsError:
            pass

    @classmethod
    def tearDownClass(cls):
        dir_path = cls.path_to_save_temp_tests_files
        try:
            for filename in os.listdir(dir_path):
                file_path = os.path.join(dir_path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path) 
            print(f"All files and subdirectories in '{dir_path}' have been deleted.")
        except FileNotFoundError:
            print(f"Error: Directory '{dir_path}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def test_extract_raw_data_and_save_it_as_is(self):

        uc = GetRawDataAndSaveItAsIs(api=MockApiAnvisa, path_to_save_data=self.path_to_save_temp_tests_files)
        uc.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS = 2
        uc.get_raw_data_and_save_it_as_is()

        mock_anvisa = MockApiAnvisa()

        # active medicines
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'active_medicines.json', 'r', encoding="utf8") as f:
            medicines = json.load(f)

        self.assertListEqual(medicines, mock_anvisa.get_active_medicines())

        # active presentations
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'presentations_from_active_medicines.json', 'r', encoding="utf8") as f:
            presentations = json.load(f)
        
        expected_presentations, errors = mock_anvisa.get_presentations(medicines=[med['produto'] for med in medicines])
        self.assertListEqual(sorted(presentations, key=lambda x: x['codigoProduto']), sorted(expected_presentations, key=lambda x: x['codigoProduto']))

        # inactive medicines
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'inactive_medicines.json', 'r', encoding="utf8") as f:
            medicines = json.load(f)

        self.assertListEqual(medicines, mock_anvisa.get_inactive_medicines())
        
        # inactive presentations
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'presentations_from_inactive_medicines.json', 'r', encoding="utf8") as f:
            presentations = json.load(f)

        expected_presentations, errors = mock_anvisa.get_presentations(medicines=[med['produto'] for med in medicines])
        self.assertListEqual(sorted(presentations, key=lambda x: x['codigoProduto']), sorted(expected_presentations, key=lambda x: x['codigoProduto']))
        
        # pharmaceutic forms
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'pharmaceutic_forms.json', 'r', encoding="utf8") as f:
            pharmaceutic_forms = json.load(f)

        expected_result = mock_anvisa.get_pharmaceutic_forms()
        self.assertListEqual(sorted(pharmaceutic_forms, key=lambda x: x['id']), sorted(expected_result, key=lambda x: x['id']))
        
        # regulatory categories
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'regulatory_categories.json', 'r', encoding="utf8") as f:
            categories = json.load(f)

        expected_result = mock_anvisa.get_regulation_category()
        self.assertListEqual(sorted(categories, key=lambda x: x['id']), sorted(expected_result, key=lambda x: x['id']))

    def test_extract_and_save_presentations_with_errors(self):

        uc = GetRawDataAndSaveItAsIs(api=MockApiAnvisa, path_to_save_data=self.path_to_save_temp_tests_files)
        uc.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS = 2
        uc.api.return_medicines_with_error = True
        uc.get_raw_data_and_save_it_as_is()

        mock_anvisa = MockApiAnvisa(return_medicines_with_error=True)

        # active medicines
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'active_medicines.json', 'r', encoding="utf8") as f:
            medicines = json.load(f)

        self.assertListEqual(medicines, mock_anvisa.get_active_medicines())

        # active presentations
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'presentations_from_active_medicines.json', 'r', encoding="utf8") as f:
            presentations = json.load(f)

        expected_presentations, errors = mock_anvisa.get_presentations(medicines=[med['produto'] for med in medicines])
        self.assertListEqual(sorted(presentations, key=lambda x: x['codigoProduto']), sorted(expected_presentations, key=lambda x: x['codigoProduto']))

        # errors from active presentations
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'inactive_medicines_presentation_error.json', 'r', encoding="utf8") as f:
            errors = json.load(f)

        expected_presentations, expected_errors = mock_anvisa.get_presentations(medicines=[med['produto'] for med in medicines])
        self.assertListEqual(sorted(presentations, key=lambda x: x['codigoProduto']), sorted(expected_presentations, key=lambda x: x['codigoProduto']))
        expected_err = []
        for err in expected_errors:
            expected_err.append({
                "codigo": err['codigo'],
                'codigoNotificacao': err['codigoNotificacao'],
                'tipoAutorizacao': err['tipoAutorizacao'],
            })
        self.assertListEqual(sorted(errors, key=lambda x: x['codigo']), sorted(expected_err, key=lambda x: x['codigo']))

        # inactive medicines
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'inactive_medicines.json', 'r', encoding="utf8") as f:
            medicines = json.load(f)

        self.assertListEqual(medicines, mock_anvisa.get_inactive_medicines())
        
        # inactive presentations
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'presentations_from_inactive_medicines.json', 'r', encoding="utf8") as f:
            presentations = json.load(f)

        expected_presentations, errors = mock_anvisa.get_presentations(medicines=[med['produto'] for med in medicines])
        self.assertListEqual(sorted(presentations, key=lambda x: x['codigoProduto']), sorted(expected_presentations, key=lambda x: x['codigoProduto']))

        # errors from active presentations
        with open(uc.PATH_TO_SAVE_DATA+uc.get_current_date_as_str()+'inactive_medicines_presentation_error.json', 'r', encoding="utf8") as f:
            errors = json.load(f)

        expected_presentations, expected_errors = mock_anvisa.get_presentations(medicines=[med['produto'] for med in medicines])
        self.assertListEqual(sorted(presentations, key=lambda x: x['codigoProduto']), sorted(expected_presentations, key=lambda x: x['codigoProduto']))
        expected_err = []
        for err in expected_errors:
            expected_err.append({
                "codigo": err['codigo'],
                'codigoNotificacao': err['codigoNotificacao'],
                'tipoAutorizacao': err['tipoAutorizacao'],
            })
        self.assertListEqual(sorted(errors, key=lambda x: x['codigo']), sorted(expected_err, key=lambda x: x['codigo']))
