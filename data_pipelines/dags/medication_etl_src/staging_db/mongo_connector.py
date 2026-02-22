import os
from pymongo import MongoClient
from pymongo.database import Database


def get_mongo_database() -> Database:
    """Create and return a MongoDB database connection from environment variables.

    Environment variables:
        MONGO_URI: MongoDB connection string (default: mongodb://localhost:27017)
        MONGO_DB_NAME: Database name (default: staging_db)
    """
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DB_NAME", "staging_db")

    client = MongoClient(mongo_uri)
    return client[db_name]

