from unittest import TestCase
from medication_etl_src.utils.extract_composition_from_presentation_string import extract_composition_from_presentation_string, ItemComposicao


class TestExtractCompositionInfoFromPresentationString(TestCase):

    def test_extract_composition_info_from_presentation_string(self):

        active_ingrediets = ["CLORIDRATO DE DORZOLAMIDA", "MALEATO DE TIMOLOL"]
        presentation = '(20+ 5) MG/ML SOL OFT CT FR GOT PLAS PEBD OPC X 5 ML'

        expected_result = [
            ItemComposicao(
                principio_ativo="CLORIDRATO DE DORZOLAMIDA",
                dosagem=20,
                unidade_de_medida="MG/ML",
                id_apresentacao_medicamento="teste",
            ),
            ItemComposicao(
                principio_ativo="MALEATO DE TIMOLOL",
                dosagem=5,
                unidade_de_medida="MG/ML",
            id_apresentacao_medicamento="teste",
            )]

        result = extract_composition_from_presentation_string(presentation, active_ingrediets, "teste")

        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_2(self):

        active_ingrediets = ["CLORIDRATO DE DONEPEZILA"]
        presentation = '10 MG COM REV CX BL AL PLAS TRANS X 200'

        expected_result = [
            ItemComposicao(
                principio_ativo="CLORIDRATO DE DONEPEZILA",
                dosagem=10.0,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="Outro",
            )]

        result = extract_composition_from_presentation_string(presentation, active_ingrediets, "Outro")

        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_3(self):

        active_ingrediets = ["CLORIDRATO DE CIPROFLOXACINO" , "CIPROFLOXACINO"]
        presentation = '1000 MG COM REV MULT LIB PROL CT BL AL/AL X 3'

        expected_result = [
            ItemComposicao(
                principio_ativo="CLORIDRATO DE CIPROFLOXACINO",
                dosagem=1000,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="1234",
            )]

        result = extract_composition_from_presentation_string(presentation, active_ingrediets, "1234")

        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_4(self):

        active_ingrediets = ['TRIANCINOLONA ACETONIDA', 'SULFATO DE NEOMICINA', 'GRAMICIDINA', 'NISTATINA']
        presentation = '1 MG + 2,5 MG + 0,25 MG + 100.000 UI/G CREM DERM CT BG AL X 10 G'

        expected_result = [
            ItemComposicao(
                principio_ativo="TRIANCINOLONA ACETONIDA",
                dosagem=1,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="2222",
            ),ItemComposicao(
                principio_ativo="SULFATO DE NEOMICINA",
                dosagem=2.5,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="2222",
            ),ItemComposicao(
                principio_ativo="GRAMICIDINA",
                dosagem=0.25,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="2222",
            ),ItemComposicao(
                principio_ativo="NISTATINA",
                dosagem=100_000,
                unidade_de_medida="UI/G",
                id_apresentacao_medicamento="2222",
            ),]

        result = extract_composition_from_presentation_string(presentation, active_ingrediets, "2222")

        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_5(self):

        presentation = "2,5 MG + 150 UTR POM DERM CT BG AL X 10 G"
        active_ingredients = ["HIALURONIDASE", "VALERATO DE BETAMETASONA"]

        expected_result = [
            ItemComposicao(
                principio_ativo="HIALURONIDASE",
                dosagem=2.5,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="3333",
            ),
            ItemComposicao(
                principio_ativo="VALERATO DE BETAMETASONA",
                dosagem=150.0,
                unidade_de_medida="UTR",
                id_apresentacao_medicamento="3333",
            ),
        ]

        result = extract_composition_from_presentation_string(presentation, active_ingredients, "3333")
        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_6(self):

        presentation = "20 MG COM REV LIB RETARD CT BL AL AL X 7"
        active_ingredients = ["ESOMEPRAZOL MAGNÉSICO", "ESOMEPRAZOL MAGNÉSICO"]

        expected_result = [
            ItemComposicao(
                principio_ativo="ESOMEPRAZOL MAGNÉSICO",
                dosagem=20.0,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="4",
            ),
        ]

        result = extract_composition_from_presentation_string(presentation, active_ingredients, "4")
        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_6(self):

        presentation = "0,6 U/G + 0,01 G/G POM DERM CT 01 BG AL X 5 G + ESP PLAS"
        active_ingredients = ["COLAGENASE", "CLORANFENICOL"]

        expected_result = [
            ItemComposicao(
                principio_ativo="COLAGENASE",
                dosagem=0.6,
                unidade_de_medida="U/G",
                id_apresentacao_medicamento="5",
            ),
            ItemComposicao(
                principio_ativo="CLORANFENICOL",
                dosagem=0.01,
                unidade_de_medida="G/G",
                id_apresentacao_medicamento="5",
            ),
        ]

        result = extract_composition_from_presentation_string(presentation, active_ingredients, "5")
        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_7(self):

        presentation = "450 + 50 MG COM REV CT BL AL PLAS PVC TRANS X 30"
        active_ingredients = ["DIOSMINA", "FLAVONÓIDES EXPRESSOS EM HESPERIDINA"]

        expected_result = [
            ItemComposicao(
                principio_ativo="DIOSMINA",
                dosagem=450.0,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="6",
            ),
            ItemComposicao(
                principio_ativo="FLAVONÓIDES EXPRESSOS EM HESPERIDINA",
                dosagem=50.0,
                unidade_de_medida="MG",
                id_apresentacao_medicamento="6",
            ),
        ]

        result = extract_composition_from_presentation_string(presentation, active_ingredients, "6")
        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_8(self):

        presentation = "0,16 MG/ML + 0,16 MCL/ML + 0,16 MCL/ML XPE INF CT FR VD AMB X 120 ML + COP"
        active_ingredients = ["MENTOL", "EUCALIPTOL", "terpina monoidratada"]

        expected_result = [
            ItemComposicao(
                principio_ativo="MENTOL",
                dosagem=0.16,
                unidade_de_medida="MG/ML",
                id_apresentacao_medicamento="6",
            ),
            ItemComposicao(
                principio_ativo="EUCALIPTOL",
                dosagem=0.16,
                unidade_de_medida="MCL/ML",
                id_apresentacao_medicamento="6",
            ),
            ItemComposicao(
                principio_ativo="terpina monoidratada",
                dosagem=0.16,
                unidade_de_medida="MCL/ML",
                id_apresentacao_medicamento="6",
            ),
        ]

        result = extract_composition_from_presentation_string(presentation, active_ingredients, "6")
        self.assertCountEqual(expected_result, result)

    def test_extract_composition_info_from_presentation_9(self):

        presentation = "301,5 MG COM SUBL CT FR PLAS OPC X 50"
        active_ingredients = ['ARNICA MONTANA L.', 'CALENDULA OFFICINALIS L.', 'HAMAMELIS VIRGINIANA L.', 'ECHINACEA ANGUSTIFOLIA DC.', 'ECHINACEA PURPUREA (L.) MOENCH', 'MATRICARIA CHAMOMILLA L.', 'BELLIS PERENNIS', 'SYMPHYTUM OFFICINALE', 'HYPERICUM PERFORATUM', 'ACHILLEA MILLEFOLIUM L.', 'ACONITUM NAPELLUS', 'ATROPA BELLADONNA', 'Mercurius solubilis', 'hepar sulfuris']

        expected_result = [
            ItemComposicao(
                principio_ativo="MENTOL",
                dosagem=0.16,
                unidade_de_medida="MG/ML",
                id_apresentacao_medicamento="6",
            ),
            ItemComposicao(
                principio_ativo="EUCALIPTOL",
                dosagem=0.16,
                unidade_de_medida="MCL/ML",
                id_apresentacao_medicamento="6",
            ),
            ItemComposicao(
                principio_ativo="terpina monoidratada",
                dosagem=0.16,
                unidade_de_medida="MCL/ML",
                id_apresentacao_medicamento="6",
            ),
        ]

        result = extract_composition_from_presentation_string(presentation, active_ingredients, "6")
        self.assertCountEqual(expected_result, result)

