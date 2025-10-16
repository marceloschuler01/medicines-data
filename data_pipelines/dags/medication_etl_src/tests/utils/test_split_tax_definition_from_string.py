from unittest import TestCase
from medication_etl_src.utils.split_tax_definition_from_string import split_tax_definition_from_string

TIPOS = ['PF Sem Impostos', 'PF 0%', 'PF 12 %', 'PF 12 % ALC', 'PF 17 %', 'PF 17 % ALC', 'PF 17,5 %', 'PF 17,5 % ALC', 'PF 18 %', 'PF 18 % ALC', 'PF 19 %', 'PF 19 % ALC', 'PF 19,5 %', 'PF 19,5 % ALC', 'PF 20 %', 'PF 20 % ALC', 'PF 20,5 %', 'PF 20,5 % ALC', 'PF 21 %', 'PF 21 % ALC', 'PF 22 %', 'PF 22 % ALC', 'PF 22,5 %', 'PF 22,5 % ALC', 'PF 23 %', 'PF 23 % ALC', 'PMC Sem Impostos', 'PMC 0 %', 'PMC 12 %', 'PMC 12 % ALC', 'PMC 17 %', 'PMC 17 % ALC', 'PMC 17,5 %', 'PMC 17,5 % ALC', 'PMC 18 %', 'PMC 18 % ALC', 'PMC 19 %', 'PMC 19 % ALC', 'PMC 19,5 %', 'PMC 19,5 % ALC', 'PMC 20 %', 'PMC 20 % ALC', 'PMC 20,5 %', 'PMC 20,5 % ALC', 'PMC 21 %', 'PMC 21 % ALC', 'PMC 22 %', 'PMC 22 % ALC', 'PMC 22,5 %', 'PMC 22,5 % ALC', 'PMC 23 %', 'PMC 23 % ALC']

class TestSplitTaxDefinitionFromString(TestCase):

    def test_split_tax_definition_from_string(self):

            self.assertEqual(split_tax_definition_from_string('PF Sem Impostos')[0], 'PF Sem Impostos')
            self.assertEqual(split_tax_definition_from_string('PF Sem Impostos')[1], 0 )

            self.assertEqual(split_tax_definition_from_string('PF 0%')[0], 'PF')
            self.assertEqual(split_tax_definition_from_string('PF 0%')[1], 0)

            self.assertEqual(split_tax_definition_from_string('PF 12 %')[0], 'PF')
            self.assertEqual(split_tax_definition_from_string('PF 12 %')[1], 12)

            self.assertEqual(split_tax_definition_from_string('PF 12 % ALC')[0], 'PF ALC')
            self.assertEqual(split_tax_definition_from_string('PF 12 % ALC')[1], 12)

            self.assertEqual(split_tax_definition_from_string('PMC 17,5 % ALC')[0], 'PMC ALC')
            self.assertEqual(split_tax_definition_from_string('PMC 17,5 % ALC')[1], 17.5)

            self.assertEqual(split_tax_definition_from_string('PMC 23 %')[0], 'PMC')
            self.assertEqual(split_tax_definition_from_string('PMC 23 %')[1], 23)
