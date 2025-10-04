from unittest import TestCase
from medication_etl_src.utils.extract_composition_info_from_presentation_string import extract_composition_info_from_presentation_string, ItemComposicao


class TestExtractCompositionInfoFromPresentationString(TestCase):

    def test_extract_composition_info_from_presentation_string(self):

        active_ingrediets = ["CLORIDRATO DE DORZOLAMIDA", "MALEATO DE TIMOLOL"]
        presentation = '(20+ 5) MG/ML SOL OFT CT FR GOT PLAS PEBD OPC X 5 ML'

        expected_result = [
            ItemComposicao(
                principio_ativo="CLORIDRATO DE DORZOLAMIDA",
                quantidade=20,
                unidade="MG/ML"
            ),
            ItemComposicao(
                principio_ativo="MALEATO DE TIMOLOL",
                quantidade=5,
                unidade="MG/ML",
            )]

        result = extract_composition_info_from_presentation_string(presentation, active_ingrediets)

        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_2(self):

        active_ingrediets = ["CLORIDRATO DE DONEPEZILA"]
        presentation = '10 MG COM REV CX BL AL PLAS TRANS X 200'

        expected_result = [
            ItemComposicao(
                principio_ativo="CLORIDRATO DE DONEPEZILA",
                quantidade=10.0,
                unidade="MG"
            )]

        result = extract_composition_info_from_presentation_string(presentation, active_ingrediets)

        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_3(self):

        active_ingrediets = ["CLORIDRATO DE CIPROFLOXACINO" , "CIPROFLOXACINO"]
        presentation = '1000 MG COM REV MULT LIB PROL CT BL AL/AL X 3'

        expected_result = [
            ItemComposicao(
                principio_ativo="CLORIDRATO DE CIPROFLOXACINO",
                quantidade=1000,
                unidade="MG"
            )]

        result = extract_composition_info_from_presentation_string(presentation, active_ingrediets)

        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_4(self):

        active_ingrediets = ['TRIANCINOLONA ACETONIDA', 'SULFATO DE NEOMICINA', 'GRAMICIDINA', 'NISTATINA']
        presentation = '1 MG + 2,5 MG + 0,25 MG + 100.000 UI/G CREM DERM CT BG AL X 10 G'

        expected_result = [
            ItemComposicao(
                principio_ativo="TRIANCINOLONA ACETONIDA",
                quantidade=1,
                unidade="MG"
            ),ItemComposicao(
                principio_ativo="SULFATO DE NEOMICINA",
                quantidade=2.5,
                unidade="MG"
            ),ItemComposicao(
                principio_ativo="GRAMICIDINA",
                quantidade=0.25,
                unidade="MG"
            ),ItemComposicao(
                principio_ativo="NISTATINA",
                quantidade=100_000,
                unidade="UI/G"
            ),]

        result = extract_composition_info_from_presentation_string(presentation, active_ingrediets)

        self.assertCountEqual(expected_result, result)
