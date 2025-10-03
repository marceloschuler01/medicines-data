import os
import pandas as pd
from io import StringIO

from medication_etl_src.database.db_connector import PostgresConnection, with_database_connection

db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),  # Change if using a remote server
    'port': os.getenv('DB_PORT')  # Default PostgreSQL port
}


class ApiDatabase:
    def __init__(self, db_connector: PostgresConnection=None):
        self.db_connector = db_connector or PostgresConnection()

    @with_database_connection
    def insert_with_copy(self, table_name: str, data: list[dict], conn: PostgresConnection=None):

        sep = ","
        null_value = "NULL"
        quote = '\"'

        df = pd.DataFrame(data)

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep=sep, na_rep=null_value, quotechar=quote)

        columns = ', '.join(df.columns)
        buffer.seek(0)

        query = f""" COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER '{sep}', NULL '{null_value}', QUOTE '{quote}'); """
        conn.execute_query(query)

    @with_database_connection
    def select(self, table_name: str, conn: PostgresConnection=None) -> list[tuple]:

        query = f"SELECT * FROM {table_name};"
        result = conn.execute_query(query, fetch=True)
        return result
