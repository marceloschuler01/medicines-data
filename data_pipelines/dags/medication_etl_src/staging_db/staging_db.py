from datetime import datetime, timezone
from typing import Optional

from pymongo import ASCENDING
from pymongo.database import Database

from medication_etl_src.staging_db.mongo_connector import get_mongo_database

# Default batch size for bulk write operations.
# Keeps memory usage bounded when inserting hundreds of thousands of documents.
_BULK_WRITE_BATCH_SIZE = 5_000


class StagingDB:
    """Staging database backed by MongoDB.

    Stores raw extracted data as documents in per-collection tables.
    Every document receives an ``extracted_at`` timestamp on insertion so that
    downstream consumers can filter by extraction date if needed.

    Parameters
    ----------
    db : pymongo.database.Database, optional
        An existing database instance (useful for testing with *mongomock*).
        When *None*, a connection is created via :func:`get_mongo_database`.
    """

    def __init__(self, db: Optional[Database] = None):
        self._db: Database = db or get_mongo_database()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def insert(self, collection_name: str, data: list[dict]) -> int:
        """Bulk-insert *data* into *collection_name*.

        Each document is enriched with an ``extracted_at`` UTC timestamp.
        Uses ``ordered=False`` so that a single bad document does not abort
        the entire batch — important when dealing with hundreds of thousands
        of presentation rows.

        Returns the number of inserted documents.
        """
        if not data:
            return 0

        now = datetime.now(timezone.utc)
        collection = self._db[collection_name]

        total_inserted = 0
        for start in range(0, len(data), _BULK_WRITE_BATCH_SIZE):
            batch = data[start : start + _BULK_WRITE_BATCH_SIZE]
            docs = [{**doc, "extracted_at": now} for doc in batch]
            result = collection.insert_many(docs, ordered=False)
            total_inserted += len(result.inserted_ids)

        return total_inserted

    def drop_collection(self, collection_name: str) -> None:
        """Drop an entire collection (e.g. before a full re-extraction)."""
        self._db.drop_collection(collection_name)

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def select(
        self,
        collection_name: str,
        page: Optional[int] = None,
        page_size: int = 5_000,
    ) -> list[dict]:
        """Read documents from *collection_name*.

        When *page* is ``None`` all documents are returned; otherwise
        standard offset-based pagination is applied.

        The MongoDB ``_id`` and ``extracted_at`` fields are excluded from the
        result so that the returned dicts match the original raw data shape,
        keeping downstream consumers (ETL adapters) unchanged.
        """
        collection = self._db[collection_name]
        projection = {"_id": 0, "extracted_at": 0}

        cursor = collection.find({}, projection)

        if page is not None:
            offset = (page - 1) * page_size
            cursor = cursor.skip(offset).limit(page_size)

        return list(cursor)

    def count(self, collection_name: str) -> int:
        """Return the number of documents in *collection_name*."""
        return self._db[collection_name].count_documents({})

    # ------------------------------------------------------------------
    # Index helpers
    # ------------------------------------------------------------------

    def distinct(self, collection_name: str, field: str, query: dict | None = None) -> list:
        """Return distinct values for *field* in *collection_name*.

        This is far more memory-efficient than :meth:`select` when only a
        single key is needed for deduplication (e.g. checking which medicine
        codes already have presentations stored).

        Parameters
        ----------
        collection_name: str
            The collection to query.
        field : str
            The document field whose distinct values are requested.
        query : dict, optional
            An optional filter to narrow the set of documents considered.
        """
        collection = self._db[collection_name]
        return collection.distinct(field, query or {})

    def ensure_indexes(self, collection_name: str, index_fields: list[str]) -> None:
        """Create ascending indexes on *index_fields* for *collection_name*.

        Indexes are created in the background so that insert-heavy workloads
        are not blocked.  Call this once after the initial bulk insert for
        collections like ``presentations_from_active_medicines`` that hold
        hundreds of thousands of documents and are later queried during ETL.
        """
        collection = self._db[collection_name]
        for field in index_fields:
            collection.create_index([(field, ASCENDING)], background=True)

