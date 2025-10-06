import unittest
from medication_etl_src.utils.split_active_principles_strings import split_active_principles_strings


class TestSplitActivePrinciplesStrings(unittest.TestCase):
    def test_basic(self):
        input_str = "Paracetamol, Ibuprofen, Aspirin"
        expected = ["Paracetamol", "Ibuprofen", "Aspirin"]
        self.assertEqual(split_active_principles_strings(input_str), expected)

    def test_empty(self):
        self.assertEqual(split_active_principles_strings(""), [])

    def test_with_none(self):
        self.assertEqual(split_active_principles_strings(None), [])

    def test_custom_sep(self):
        input_str = "Paracetamol;Ibuprofen;Aspirin"
        expected = ["Paracetamol", "Ibuprofen", "Aspirin"]
        self.assertEqual(split_active_principles_strings(input_str, sep=";"), expected)


if __name__ == "__main__":
    unittest.main()
