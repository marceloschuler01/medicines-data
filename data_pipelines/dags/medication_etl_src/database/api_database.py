import os
import pandas as pd
from io import StringIO
from typing import Any

from medication_etl_src.database.db_connector import PostgresConnection, with_database_connection

db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),  # Change if using a remote server
    'port': os.getenv('DB_PORT')  # Default PostgreSQL port
}

class Filter:

    def __init__(self, column: str, value: str, operator: str = '='):
        self.column = column
        self.value = value
        self.operator = operator

class ApiDatabase:

    @staticmethod
    def filter(column: str, value: Any, operator: str = '=') -> Filter:
        return Filter(column=column, value=value, operator=operator)

    @staticmethod
    @with_database_connection
    def get_columns(table_name: str, conn: PostgresConnection=None) -> list[str]:

        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """
        result = conn.execute_query(query, fetch=True)
        return [row[0] for row in result]

    @staticmethod
    @with_database_connection
    def insert_with_copy(table_name: str, data: list[dict], conn: PostgresConnection=None):

        sep = ","
        null_value = "NULL"
        quote = '\"'

        df = pd.DataFrame(data)

        table_columns = ApiDatabase.get_columns(table_name=table_name, conn=conn)
        df = df.filter(items=table_columns, axis="columns")
        columns = ', '.join(df.columns)

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep=sep, na_rep=null_value, quotechar=quote)

        buffer.seek(0)

        query = f""" COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER '{sep}', NULL '{null_value}', QUOTE '{quote}'); """
        conn.copy_expert(query=query, file=buffer)

    @staticmethod
    @with_database_connection
    def select(table_name: str, columns: list[str]=None, filters: list[Filter] | Filter | None = None, conn: PostgresConnection=None) -> list[tuple]:

        params = {}
        sql_filters = ApiDatabase._parse_filters(filters=filters, params=params)

        query = f"SELECT {','.join(columns) if columns else '*'} FROM {table_name} {sql_filters};"
        result = conn.execute_query(query, params=params, fetch=True)

        return result

    @staticmethod
    @with_database_connection
    def select_with_pandas(table_name: str, columns: list[str]=None, filters: list[Filter] | Filter | None = None, conn: PostgresConnection=None) -> pd.DataFrame:

        params = {}
        sql_filters = ApiDatabase._parse_filters(filters=filters, params=params)

        query = f"SELECT {','.join(columns) if columns else '*'} FROM {table_name} {sql_filters};"
        result = pd.read_sql_query(query, params=params, con=conn.conn)

        return result

    @staticmethod
    @with_database_connection
    def execute(query: str, params: dict | None = None, conn: PostgresConnection=None) -> None:
        conn.execute_query(query, params=params, fetch=False)

    @staticmethod
    def _parse_filters(filters: list[Filter] | Filter, params: dict) -> str:

        if isinstance(filters, Filter):
            filters = [filters]

        if not filters:
            return ""

        filter_clauses = []

        for i, filter in enumerate(filters):

            value = filter.value

            if filter.value is None and filter.operator == '=':
                clause = f"{filter.column} IS NULL"
                filter_clauses.append(clause)
                continue

            if filter.value is None and filter.operator == '!=':
                clause = f"{filter.column} IS NOT NULL"
                filter_clauses.append(clause)
                continue

            if filter.operator.upper() in ['IN', 'NOT IN']:
                if not isinstance(value, (list, tuple, set)):
                    raise ValueError(f"Value for 'IN' operator must be a list, tuple or set. Got {type(value)}")
                if len(value) == 0:
                    # não há nada para filtrar → força expressão falsa
                    filter_clauses.append("1=0")
                    continue
                value = tuple(value)

            params[f"filter_value_{i}"] = value
            clause = f"{filter.column}   {filter.operator}  %({f'filter_value_{i}'})s"
            filter_clauses.append(clause)

        return " WHERE " + " AND ".join(filter_clauses)
