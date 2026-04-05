import unittest

import mongomock

from medication_etl_src.staging_db.staging_db import StagingDB
from medication_etl_src.usecase.extract_raw_data_and_save_it_as_is import GetRawDataAndSaveItAsIs
from medication_etl_src.tests.mock_api_anvisa import MockApiAnvisa


class TestExtractRawDataAndSaveItAsIs(unittest.TestCase):
    """Validate that GetRawDataAndSaveItAsIs correctly extracts data from the
    API mocks and persists it in the staging MongoDB (mongomock in tests)."""

    def setUp(self):
        """Create a fresh in-memory MongoDB for every test."""
        self.mongo_client = mongomock.MongoClient()
        self.db = self.mongo_client["test_staging_db"]
        self.staging_db = StagingDB(db=self.db)

    def tearDown(self):
        """Drop the test database after each test."""
        self.mongo_client.drop_database("test_staging_db")
        self.mongo_client.close()

    # ------------------------------------------------------------------
    # Happy path – no presentation errors
    # ------------------------------------------------------------------

    def test_extract_raw_data_and_save_it_as_is(self):
        uc = GetRawDataAndSaveItAsIs(api=MockApiAnvisa, staging_db=self.staging_db)
        uc.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS = 2
        uc.get_raw_data_and_save_it_as_is()

        mock_anvisa = MockApiAnvisa()

        # -- active medicines --
        medicines = self.staging_db.select("active_medicines")
        self.assertListEqual(medicines, mock_anvisa.get_active_medicines())

        # -- active presentations --
        presentations = self.staging_db.select("presentations_from_active_medicines")
        expected_presentations, _ = mock_anvisa.get_presentations(
            medicines=[med["produto"] for med in medicines],
        )
        self.assertListEqual(
            sorted(presentations, key=lambda x: x["codigoProduto"]),
            sorted(expected_presentations, key=lambda x: x["codigoProduto"]),
        )

        # -- inactive medicines --
        medicines = self.staging_db.select("inactive_medicines")
        self.assertListEqual(medicines, mock_anvisa.get_inactive_medicines())

        # -- inactive presentations --
        presentations = self.staging_db.select("presentations_from_inactive_medicines")
        expected_presentations, _ = mock_anvisa.get_presentations(
            medicines=[med["produto"] for med in medicines],
        )
        self.assertListEqual(
            sorted(presentations, key=lambda x: x["codigoProduto"]),
            sorted(expected_presentations, key=lambda x: x["codigoProduto"]),
        )

        # -- pharmaceutic forms --
        pharmaceutic_forms = self.staging_db.select("pharmaceutic_forms")
        expected_result = mock_anvisa.get_pharmaceutic_forms()
        self.assertListEqual(
            sorted(pharmaceutic_forms, key=lambda x: x["id"]),
            sorted(expected_result, key=lambda x: x["id"]),
        )

        # -- regulatory categories --
        categories = self.staging_db.select("regulatory_categories")
        expected_result = mock_anvisa.get_regulation_category()
        self.assertListEqual(
            sorted(categories, key=lambda x: x["id"]),
            sorted(expected_result, key=lambda x: x["id"]),
        )

    # ------------------------------------------------------------------
    # Presentations with errors
    # ------------------------------------------------------------------

    def test_extract_presentations_aborts_on_non_500_error(self):
        """Non-500 errors (e.g. timeouts) must abort the pipeline, not be silently swallowed."""
        uc = GetRawDataAndSaveItAsIs(api=MockApiAnvisa, staging_db=self.staging_db)
        uc.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS = 2
        uc.api.raise_non_500_error = True

        # Pre-populate medicines so presentations step has data to fetch
        mock_anvisa = MockApiAnvisa(return_medicines_with_error=True)
        self.staging_db.insert("active_medicines", mock_anvisa.get_active_medicines())

        with self.assertRaises(Exception):
            uc.extract_and_save_presentations()

    # ------------------------------------------------------------------
    # Presentations with 500 errors (gracefully handled)
    # ------------------------------------------------------------------

    def test_extract_and_save_presentations_with_errors(self):
        uc = GetRawDataAndSaveItAsIs(api=MockApiAnvisa, staging_db=self.staging_db)
        uc.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS = 2
        uc.api.return_medicines_with_error = True
        uc.get_raw_data_and_save_it_as_is()

        mock_anvisa = MockApiAnvisa(return_medicines_with_error=True)

        # -- active medicines --
        medicines = self.staging_db.select("active_medicines")
        self.assertListEqual(medicines, mock_anvisa.get_active_medicines())

        # -- active presentations --
        presentations = self.staging_db.select("presentations_from_active_medicines")
        expected_presentations, _ = mock_anvisa.get_presentations(
            medicines=[med["produto"] for med in medicines],
        )
        self.assertListEqual(
            sorted(presentations, key=lambda x: x["codigoProduto"]),
            sorted(expected_presentations, key=lambda x: x["codigoProduto"]),
        )

        # -- errors from active presentations --
        errors = self.staging_db.select("active_medicines_presentation_error")
        expected_presentations, expected_errors = mock_anvisa.get_presentations(
            medicines=[med["produto"] for med in medicines],
        )
        expected_err = [
            {
                "codigo": err["codigo"],
                "codigoNotificacao": err["codigoNotificacao"],
                "tipoAutorizacao": err["tipoAutorizacao"],
            }
            for err in expected_errors
        ]
        self.assertListEqual(
            sorted(errors, key=lambda x: x["codigo"]),
            sorted(expected_err, key=lambda x: x["codigo"]),
        )

        # -- inactive medicines --
        medicines = self.staging_db.select("inactive_medicines")
        self.assertListEqual(medicines, mock_anvisa.get_inactive_medicines())

        # -- inactive presentations --
        presentations = self.staging_db.select("presentations_from_inactive_medicines")
        expected_presentations, _ = mock_anvisa.get_presentations(
            medicines=[med["produto"] for med in medicines],
        )
        self.assertListEqual(
            sorted(presentations, key=lambda x: x["codigoProduto"]),
            sorted(expected_presentations, key=lambda x: x["codigoProduto"]),
        )

        # -- errors from inactive presentations --
        errors = self.staging_db.select("inactive_medicines_presentation_error")
        expected_presentations, expected_errors = mock_anvisa.get_presentations(
            medicines=[med["produto"] for med in medicines],
        )
        expected_err = [
            {
                "codigo": err["codigo"],
                "codigoNotificacao": err["codigoNotificacao"],
                "tipoAutorizacao": err["tipoAutorizacao"],
            }
            for err in expected_errors
        ]
        self.assertListEqual(
            sorted(errors, key=lambda x: x["codigo"]),
            sorted(expected_err, key=lambda x: x["codigo"]),
        )

