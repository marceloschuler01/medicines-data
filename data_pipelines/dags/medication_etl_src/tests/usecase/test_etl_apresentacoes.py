import unittest

import pandas as pd

from medication_etl_src.usecase.etl_apresentacoes import ExtractTransformAndLoadApresentacoes


class TestEtlApresentacoes(unittest.TestCase):
    def test_extract_therapeutic_classes_from_products_filters_and_deduplicates(self):
        products = pd.DataFrame(
            {
                "classes_terapeuticas": [
                    ["ANALGESICO", " ANALGESICO ", None],
                    [],
                    ["ANTI-INFLAMATORIO", ""],
                ]
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_therapeutic_classes_from_products(products)

        self.assertListEqual(
            sorted(result["classe_terapeutica"].tolist()),
            ["ANALGESICO", "ANTI-INFLAMATORIO"],
        )

    def test_extract_therapeutic_class_relationships_maps_classes_to_medicines(self):
        products = pd.DataFrame(
            {
                "id_medicamento": ["med-1", "med-2", None],
                "classes_terapeuticas": [
                    ["ANALGESICO", " ANTI-INFLAMATORIO ", "ANALGESICO"],
                    ["ANALGESICO", None, ""],
                    ["ANALGESICO"],
                ],
            }
        )
        therapeutic_classes = pd.DataFrame(
            {
                "id_classe_terapeutica": ["class-1", "class-2"],
                "classe_terapeutica": ["ANALGESICO", "ANTI-INFLAMATORIO"],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_therapeutic_class_relationships(
            products,
            therapeutic_classes,
        )

        self.assertCountEqual(
            result.to_dict(orient="records"),
            [
                {"id_classe_terapeutica": "class-1", "id_medicamento": "med-1"},
                {"id_classe_terapeutica": "class-2", "id_medicamento": "med-1"},
                {"id_classe_terapeutica": "class-1", "id_medicamento": "med-2"},
            ],
        )

    def test_extract_pharmaceutic_forms_from_presentations_filters_and_deduplicates(self):
        presentations = pd.DataFrame(
            {
                "formas_farmaceuticas": [
                    ["CAPSULA", " CAPSULA ", None],
                    [],
                    ["COMPRIMIDO", ""],
                ]
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_pharmaceutic_forms_from_presentations(presentations)

        self.assertListEqual(
            sorted(result["forma_farmaceutica"].tolist()),
            ["CAPSULA", "COMPRIMIDO"],
        )

    def test_extract_pharmaceutic_form_relationships_maps_forms_to_presentations(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2", None],
                "formas_farmaceuticas": [
                    ["CAPSULA", " COMPRIMIDO ", "CAPSULA"],
                    ["CAPSULA", None, ""],
                    ["CAPSULA"],
                ],
            }
        )
        pharmaceutic_forms = pd.DataFrame(
            {
                "id_forma_farmaceutica": ["form-1", "form-2"],
                "forma_farmaceutica": ["CAPSULA", "COMPRIMIDO"],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_pharmaceutic_form_relationships(
            presentations,
            pharmaceutic_forms,
        )

        self.assertCountEqual(
            result.to_dict(orient="records"),
            [
                {"id_apresentacao_medicamento": "pres-1", "id_forma_farmaceutica": "form-1"},
                {"id_apresentacao_medicamento": "pres-1", "id_forma_farmaceutica": "form-2"},
                {"id_apresentacao_medicamento": "pres-2", "id_forma_farmaceutica": "form-1"},
            ],
        )

    def test_extract_packaging_from_presentations_extracts_primary_and_secondary(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2", "pres-3"],
                "embalagens_primarias": [
                    [{"tipo": "FRASCO DE PLASTICO", "observacao": "transparente"}],
                    [{"tipo": "BLISTER", "observacao": None}],
                    None,
                ],
                "embalagens_secundarias": [
                    [{"tipo": "CARTUCHO DE CARTOLINA", "observacao": "caixa"}],
                    [],
                    [{"tipo": "CAIXA DE PAPELAO", "observacao": "sem observacao"}],
                ],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_packaging_from_presentations(presentations)

        self.assertEqual(len(result), 4)
        self.assertTrue(all(result["primaria"].isin([True, False])))
        self.assertTrue(any(result["tipo"] == "FRASCO DE PLASTICO"))
        self.assertTrue(any(result["tipo"] == "BLISTER"))
        self.assertTrue(any(result["tipo"] == "CARTUCHO DE CARTOLINA"))
        self.assertTrue(any(result["tipo"] == "CAIXA DE PAPELAO"))

    def test_extract_packaging_from_presentations_filters_empty_types(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1"],
                "embalagens_primarias": [
                    [{"tipo": "VALID TYPE", "observacao": "obs"}],
                ],
                "embalagens_secundarias": [
                    [{"tipo": "", "observacao": None}],
                ],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_packaging_from_presentations(presentations)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["tipo"], "VALID TYPE")

    def test_extract_packaging_from_presentations_returns_empty_df_when_no_data(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2"],
                "embalagens_primarias": [None, []],
                "embalagens_secundarias": [[], None],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_packaging_from_presentations(presentations)

        self.assertEqual(len(result), 0)

    def test_extract_fabricantes_nacionais_from_presentations_extracts_data(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2"],
                "fabricantes_nacionais": [
                    [
                        {"fabricante": "FABRICA 1", "cnpj": "12345678000100", "uf": "SP", "cidade": "SAO PAULO", "etapaFabricacao": "Producao"},
                        {"fabricante": "FABRICA 2", "cnpj": "98765432000100", "uf": "RJ", "cidade": "RIO DE JANEIRO", "etapaFabricacao": None},
                    ],
                    [
                        {"fabricante": "FABRICA 1", "cnpj": "12345678000100", "uf": "SP", "cidade": "SAO PAULO", "etapaFabricacao": "Producao"},
                    ],
                ],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_fabricantes_nacionais_from_presentations(presentations)

        self.assertEqual(len(result), 2)
        self.assertTrue(any(result["nome"] == "FABRICA 1"))
        self.assertTrue(any(result["nome"] == "FABRICA 2"))
        self.assertTrue(any(result["cnpj"] == "12345678000100"))
        self.assertTrue(any(result["cnpj"] == "98765432000100"))

    def test_extract_fabricantes_nacionais_from_presentations_filters_empty_names(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1"],
                "fabricantes_nacionais": [
                    [{"fabricante": "", "cnpj": "12345678000100"}],
                ],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_fabricantes_nacionais_from_presentations(presentations)

        self.assertEqual(len(result), 0)

    def test_extract_fabricantes_nacionais_from_presentations_returns_empty_df_when_no_data(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2"],
                "fabricantes_nacionais": [None, []],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_fabricantes_nacionais_from_presentations(presentations)

        self.assertEqual(len(result), 0)

    def test_extract_fabricantes_internacionais_from_presentations_extracts_data(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2"],
                "fabricantesInternacionais": [
                    [
                        {"fabricante": "INTAS PHARMA", "pais": "INDIA", "endereco": "123 Main St", "codigoUnico": "ABC123", "etapaFabricacao": "Embalagem"},
                    ],
                    [
                        {"fabricante": "INTAS PHARMA", "pais": "INDIA", "endereco": None, "codigoUnico": None, "etapaFabricacao": None},
                    ],
                ],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_fabricantes_internacionais_from_presentations(presentations)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["nome_fabricante"], "INTAS PHARMA")
        self.assertEqual(result.iloc[0]["pais"], "INDIA")

    def test_extract_fabricantes_internacionais_from_presentations_filters_empty_names(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1"],
                "fabricantesInternacionais": [
                    [{"fabricante": "", "pais": "BRASIL"}],
                ],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_fabricantes_internacionais_from_presentations(presentations)

        self.assertEqual(len(result), 0)

    def test_extract_fabricantes_internacionais_from_presentations_returns_empty_df_when_no_data(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2"],
                "fabricantesInternacionais": [None, []],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_fabricantes_internacionais_from_presentations(presentations)

        self.assertEqual(len(result), 0)

    def test_extract_fabricante_nacional_relationships_maps_fabricantes_to_presentations(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2", "pres-3"],
                "fabricantes_nacionais": [
                    [{"fabricante": "FABRICA 1", "cnpj": "12345678000100"}],
                    [{"fabricante": "FABRICA 2", "cnpj": "98765432000100"}],
                    None,
                ],
            }
        )
        fabricantes = pd.DataFrame(
            {
                "id_fabricante_nacional": ["fab-1", "fab-2"],
                "nome": ["FABRICA 1", "FABRICA 2"],
                "cnpj": ["12345678000100", "98765432000100"],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_fabricante_nacional_relationships(presentations, fabricantes)

        self.assertEqual(len(result), 2)
        self.assertTrue(any(result["id_apresentacao_medicamento"] == "pres-1"))
        self.assertTrue(any(result["id_apresentacao_medicamento"] == "pres-2"))

    def test_extract_fabricante_internacional_relationships_maps_fabricantes_to_presentations(self):
        presentations = pd.DataFrame(
            {
                "id_apresentacao_medicamento": ["pres-1", "pres-2", "pres-3"],
                "fabricantesInternacionais": [
                    [{"fabricante": "INTAS", "pais": "INDIA"}],
                    [{"fabricante": "OTHER", "pais": "USA"}],
                    None,
                ],
            }
        )
        fabricantes = pd.DataFrame(
            {
                "id_fabricante_internacional": ["fab-int-1", "fab-int-2"],
                "nome_fabricante": ["INTAS", "OTHER"],
                "pais": ["INDIA", "USA"],
            }
        )

        result = ExtractTransformAndLoadApresentacoes._extract_fabricante_internacional_relationships(presentations, fabricantes)

        self.assertEqual(len(result), 2)
        self.assertTrue(any(result["id_apresentacao_medicamento"] == "pres-1"))
        self.assertTrue(any(result["id_apresentacao_medicamento"] == "pres-2"))


if __name__ == "__main__":
    unittest.main()
