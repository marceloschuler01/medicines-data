import pandas as pd

from medication_etl_src.api.api_anvisa import ApiAnvisa
from medication_etl_src.api.api_cmed import ApiCMED
from medication_etl_src.staging_db.staging_db import StagingDB


class GetRawDataAndSaveItAsIs:
    """Extract raw data from external APIs and persist it in the staging MongoDB.

    Each data category is stored in its own collection.  Presentations are
    fetched in configurable-sized batches and incrementally appended so that
    a crash mid-way does not require re-fetching everything from scratch.
    """

    # Collections used as staging tables
    COLLECTION_ACTIVE_MEDICINES = "active_medicines"
    COLLECTION_INACTIVE_MEDICINES = "inactive_medicines"
    COLLECTION_PRESENTATIONS_ACTIVE = "presentations_from_active_medicines"
    COLLECTION_PRESENTATIONS_INACTIVE = "presentations_from_inactive_medicines"
    COLLECTION_ERRORS_ACTIVE = "active_medicines_presentation_error"
    COLLECTION_ERRORS_INACTIVE = "inactive_medicines_presentation_error"
    COLLECTION_REGULATORY_CATEGORIES = "regulatory_categories"
    COLLECTION_PHARMACEUTIC_FORMS = "pharmaceutic_forms"
    COLLECTION_PRECO_MAX_CONSUMIDOR = "preco_maximo_consumidor"
    COLLECTION_PRECO_MAX_GOVERNO = "preco_maximo_governo"

    # Presentation index fields for query performance
    _PRESENTATION_INDEX_FIELDS = ["codigoProduto", "codigoNotificacao", "tipoAutorizacao"]

    def __init__(self, api=ApiAnvisa, api_cmed=ApiCMED, staging_db: StagingDB = None):
        self.api = api()
        self.api_cmed = api_cmed()
        self.staging_db = staging_db or StagingDB()
        self.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS: int = 800

    # ------------------------------------------------------------------
    # Presentations staging cleanup
    # ------------------------------------------------------------------

    def drop_presentations_collections(self):
        self.staging_db.drop_collection(self.COLLECTION_PRESENTATIONS_ACTIVE)
        self.staging_db.drop_collection(self.COLLECTION_PRESENTATIONS_INACTIVE)
        self.staging_db.drop_collection(self.COLLECTION_ERRORS_ACTIVE)
        self.staging_db.drop_collection(self.COLLECTION_ERRORS_INACTIVE)

    # ------------------------------------------------------------------
    # Main orchestrator
    # ------------------------------------------------------------------

    def get_raw_data_and_save_it_as_is(self):
        self.extract_and_save_active_medicines_data()
        self.extract_and_save_inactive_medicines_data()
        self.extract_and_save_regulatory_category()
        self.extract_and_save_pharmaceutic_forms()
        self.extract_and_save_presentations()
        self.extract_and_save_presentations_from_inactive_medicines()
        self.extract_preco_maximo_consumidor_data()

    # ------------------------------------------------------------------
    # Medicines
    # ------------------------------------------------------------------

    def extract_and_save_active_medicines_data(self):
        medicines_data = self.api.get_active_medicines()
        self.staging_db.drop_collection(self.COLLECTION_ACTIVE_MEDICINES)
        self.staging_db.insert(self.COLLECTION_ACTIVE_MEDICINES, medicines_data)

    def extract_and_save_inactive_medicines_data(self):
        medicines_data = self.api.get_inactive_medicines()
        self.staging_db.drop_collection(self.COLLECTION_INACTIVE_MEDICINES)
        self.staging_db.insert(self.COLLECTION_INACTIVE_MEDICINES, medicines_data)

    # ------------------------------------------------------------------
    # Presentations (batched / incremental)
    # ------------------------------------------------------------------

    def extract_and_save_presentations(self):
        self.extract_and_save_presentations_from_medicines(
            medicines_collection=self.COLLECTION_ACTIVE_MEDICINES,
            presentations_collection=self.COLLECTION_PRESENTATIONS_ACTIVE,
            errors_collection=self.COLLECTION_ERRORS_ACTIVE,
        )

    def extract_and_save_presentations_from_inactive_medicines(self):
        self.extract_and_save_presentations_from_medicines(
            medicines_collection=self.COLLECTION_INACTIVE_MEDICINES,
            presentations_collection=self.COLLECTION_PRESENTATIONS_INACTIVE,
            errors_collection=self.COLLECTION_ERRORS_INACTIVE,
        )

    def extract_and_save_presentations_from_medicines(
        self,
        medicines_collection: str,
        presentations_collection: str,
        errors_collection: str,
    ):
        """Incrementally fetch and store presentations for all medicines.

        Uses MongoDB ``distinct`` queries to determine which medicine codes
        still need fetching — avoiding loading full documents into memory.
        Only the newly fetched batch is appended — no full-collection rewrite.
        """

        # 1. Read the medicines list from the staging DB
        raw_medicines = self.staging_db.select(medicines_collection)
        medicines = pd.DataFrame([
            {
                "codigo": m["produto"]["codigo"],
                "codigoNotificacao": m["produto"]["codigoNotificacao"],
                "tipoAutorizacao": m["produto"]["tipoAutorizacao"],
            }
            for m in raw_medicines
        ])

        registered = medicines[medicines["tipoAutorizacao"] != "NOTIFICADO"]
        notified = medicines[medicines["tipoAutorizacao"] == "NOTIFICADO"]

        # 2. Use distinct queries to get only the keys already saved (low memory)
        not_notified_filter = {"tipoAutorizacao": {"$ne": "NOTIFICADO"}}
        notified_filter = {"tipoAutorizacao": "NOTIFICADO"}

        saved_registered_codes = set(
            self.staging_db.distinct(presentations_collection, "codigoProduto", not_notified_filter)
        ) | set(
            self.staging_db.distinct(errors_collection, "codigo", not_notified_filter)
        )

        saved_notification_codes = set(
            self.staging_db.distinct(presentations_collection, "codigoNotificacao", notified_filter)
        ) | set(
            self.staging_db.distinct(errors_collection, "codigoNotificacao", notified_filter)
        )

        # 3. Filter out already-fetched medicines
        remaining_registered = registered[~registered["codigo"].isin(saved_registered_codes)].to_dict(orient="records")
        remaining_notified = notified[~notified["codigoNotificacao"].isin(saved_notification_codes)].to_dict(orient="records")
        remaining_medicines = remaining_registered + remaining_notified

        print(len(remaining_medicines), " medicines to be read")

        if not remaining_medicines:
            self.staging_db.ensure_indexes(presentations_collection, self._PRESENTATION_INDEX_FIELDS)
            return "Finalizado"

        # 4. Take only the next batch
        batch_size = self.PRESENTATIONS_PER_TIME_IN_GET_PRESENTATIONS
        to_be_saved_after = len(remaining_medicines) - batch_size
        batch = remaining_medicines[:batch_size]

        # 5. Fetch presentations from the API
        presentations, errors = self.api.get_presentations(medicines=batch)

        # 6. Append only the new data (no full rewrite)
        if presentations:
            self.staging_db.insert(presentations_collection, presentations)

        if errors:
            error_docs = [
                {
                    "codigo": err["codigo"],
                    "codigoNotificacao": err["codigoNotificacao"],
                    "tipoAutorizacao": err["tipoAutorizacao"],
                }
                for err in errors
            ]
            self.staging_db.insert(errors_collection, error_docs)

        # 7. Free memory and recurse if there are remaining medicines
        del remaining_medicines, presentations, errors, batch

        if to_be_saved_after > 0:
            return self.extract_and_save_presentations_from_medicines(
                medicines_collection=medicines_collection,
                presentations_collection=presentations_collection,
                errors_collection=errors_collection,
            )

        # Create indexes on presentations for downstream ETL performance
        self.staging_db.ensure_indexes(presentations_collection, self._PRESENTATION_INDEX_FIELDS)

        return "Finalizado"

    # ------------------------------------------------------------------
    # Regulatory categories & pharmaceutical forms
    # ------------------------------------------------------------------

    def extract_and_save_regulatory_category(self):
        categories = self.api.get_regulation_category()
        self.staging_db.drop_collection(self.COLLECTION_REGULATORY_CATEGORIES)
        self.staging_db.insert(self.COLLECTION_REGULATORY_CATEGORIES, categories)

    def extract_and_save_pharmaceutic_forms(self):
        forms = self.api.get_pharmaceutic_forms()
        self.staging_db.drop_collection(self.COLLECTION_PHARMACEUTIC_FORMS)
        self.staging_db.insert(self.COLLECTION_PHARMACEUTIC_FORMS, forms)

    # ------------------------------------------------------------------
    # CMED prices
    # ------------------------------------------------------------------

    def extract_preco_maximo_consumidor_data(self):
        data = self.api_cmed.get_preco_maximo_consumidor()
        self.staging_db.drop_collection(self.COLLECTION_PRECO_MAX_CONSUMIDOR)
        self.staging_db.insert(self.COLLECTION_PRECO_MAX_CONSUMIDOR, data.to_dict(orient="records"))

    def extract_preco_maximo_governo_data(self):
        data = self.api_cmed.get_preco_maximo_governo()
        self.staging_db.drop_collection(self.COLLECTION_PRECO_MAX_GOVERNO)
        self.staging_db.insert(self.COLLECTION_PRECO_MAX_GOVERNO, data.to_dict(orient="records"))


if __name__ == "__main__":
    uc = GetRawDataAndSaveItAsIs()
    uc.get_raw_data_and_save_it_as_is()
