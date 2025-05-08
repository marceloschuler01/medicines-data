import unittest
from medication_etl_src.usecase.extract_raw_data_and_save_it_as_is import GetRawDataAndSaveItAsIs
from medication_etl_src.tests.mock_api_anvisa import MockApiAnvisa

class TestExtractRawDataAndSaveItAsIs(unittest.TestCase):
    
    def test_extract_raw_data_and_save_it_as_is(self):

        uc = GetRawDataAndSaveItAsIs(api=MockApiAnvisa, path_to_save_data="C://Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/temp_files/tests/")
        uc.get_raw_data_and_save_it_as_is()
