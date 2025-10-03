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
    def get_columns(self, table_name: str, conn: PostgresConnection=None) -> list[str]:

        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """
        result = conn.execute_query(query, fetch=True)
        return [row[0] for row in result]

    @with_database_connection
    def insert_with_copy(self, table_name: str, data: list[dict], conn: PostgresConnection=None):

        sep = ","
        null_value = "NULL"
        quote = '\"'

        df = pd.DataFrame(data)

        table_columns = self.get_columns(table_name=table_name, conn=conn)
        df = df.filter(items=table_columns, axis="columns")
        columns = ', '.join(df.columns)

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep=sep, na_rep=null_value, quotechar=quote)

        buffer.seek(0)

        query = f""" COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER '{sep}', NULL '{null_value}', QUOTE '{quote}'); """
        conn.copy_expert(query=query, file=buffer)

    @with_database_connection
    def select(self, table_name: str, conn: PostgresConnection=None) -> list[tuple]:

        query = f"SELECT * FROM {table_name};"
        result = conn.execute_query(query, fetch=True)
        return result
