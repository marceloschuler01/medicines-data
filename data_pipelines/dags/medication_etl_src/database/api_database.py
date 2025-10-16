import os
import pandas as pd
from io import StringIO
from typing import Any
import uuid

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

        if not table_columns:
            raise ValueError(f"Table '{table_name}' does not exist or has no columns.")

        df = df.filter(items=table_columns, axis="columns")
        columns = ', '.join(df.columns)

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep=sep, na_rep=null_value, quotechar=quote)

        buffer.seek(0)

        query = f""" COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER '{sep}', NULL '{null_value}', QUOTE '{quote}'); """
        conn.copy_expert(query=query, file=buffer)

    @staticmethod
    @with_database_connection
    def select(table_name: str, columns: list[str]=None, filters: list[Filter] | Filter | None = None, conn: PostgresConnection=None) -> list[dict]:

        params = {}
        sql_filters = ApiDatabase._parse_filters(filters=filters, params=params)

        columns = columns or ApiDatabase.get_columns(table_name=table_name, conn=conn)

        query = f"SELECT {','.join(columns)} FROM {table_name} {sql_filters};"
        result = conn.execute_query(query, params=params, fetch=True)

        # Convert list of tuples to list of dicts
        return [dict(zip(columns, row)) for row in result]

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
    def delete(table_name: str, filters: list[Filter] | Filter | None = None, conn: PostgresConnection=None) -> None:

        params = {}
        sql_filters = ApiDatabase._parse_filters(filters=filters, params=params)

        query = f"DELETE FROM {table_name} {sql_filters};"
        conn.execute_query(query, params=params, fetch=False)

    @staticmethod
    @with_database_connection
    def execute(query: str, params: dict | None = None, conn: PostgresConnection=None) -> None:
        conn.execute_query(query, params=params, fetch=False)

    @staticmethod
    @with_database_connection
    def update_in_bulk(
        table_name: str,
        data: pd.DataFrame,
        filter_column: str,
        *,
        skip_unchanged: bool = True,
        conn: PostgresConnection = None,
    ) -> int:
        """
        Update rows in `table_name` using values from a pandas DataFrame.
        - `data` must contain the `filter_column` and 1+ columns to update.
        - All non-filter columns in `data` will be updated.
        - Returns an approximate count of rows that will be updated (matches).
        """
        if data is None or data.empty:
            return 0

        # 1) Validate columns against the table
        table_cols = ApiDatabase.get_columns(table_name=table_name, conn=conn)
        if not table_cols:
            raise ValueError(f"Table '{table_name}' not found or has no columns.")

        if filter_column not in data.columns:
            raise ValueError(f"Filter column '{filter_column}' not found in DataFrame.")

        if filter_column not in table_cols:
            raise ValueError(f"Filter column '{filter_column}' not found in table '{table_name}'.")

        # columns to update = all dataframe columns minus the filter column
        update_cols = [c for c in data.columns if c != filter_column]
        if not update_cols:
            raise ValueError("Provide at least one column to update besides the filter column.")

        # Also ensure all update columns exist in the table
        missing = [c for c in update_cols if c not in table_cols]
        if missing:
            raise ValueError(f"Columns not found in table '{table_name}': {missing}")

        # 2) Create a temp table with *the same structure* (types) as the target table
        #    This avoids any type-casting headaches.
        temp_name = f"tmp_upd_{uuid.uuid4().hex[:10]}"
        conn.execute_query(
            f'CREATE TEMP TABLE {temp_name} AS SELECT * FROM {table_name} WHERE false;',
            fetch=False
        )

        # 3) COPY DataFrame rows into the temp table (only the needed columns)
        #    Keep it simple: CSV via StringIO, like your insert_with_copy.
        load_cols = [filter_column] + update_cols
        df = data[load_cols].copy()

        sep = ","
        null_value = "NULL"
        quote = '\"'

        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep=sep, na_rep=null_value, quotechar=quote)
        buf.seek(0)

        conn.copy_expert(
            query=(
                f"COPY {temp_name} ({', '.join(load_cols)}) "
                f"FROM STDIN WITH (FORMAT CSV, DELIMITER '{sep}', NULL '{null_value}', QUOTE '{quote}')"
            ),
            file=buf
        )

        # 4) (Optional) count how many rows will be touched (approx; before update)
        #    We use the same join and, if requested, the same 'changed' predicate.
        base_join = f"FROM {table_name} t JOIN {temp_name} s ON t.{filter_column} = s.{filter_column}"
        if skip_unchanged:
            diff_pred = " OR ".join([f"t.{c} IS DISTINCT FROM s.{c}" for c in update_cols])
            count_sql = f"SELECT COUNT(*) {base_join} WHERE {diff_pred};"
        else:
            count_sql = f"SELECT COUNT(*) {base_join};"

        matches = conn.execute_query(count_sql, fetch=True)[0][0]

        # 5) Build and perform the single UPDATE … FROM
        set_clause = ", ".join([f"{c} = s.{c}" for c in update_cols])
        if skip_unchanged:
            diff_pred = " OR ".join([f"t.{c} IS DISTINCT FROM s.{c}" for c in update_cols])
            where_extra = f" AND ({diff_pred})"
        else:
            where_extra = ""

        update_sql = f"""
            UPDATE {table_name} AS t
            SET {set_clause}
            FROM {temp_name} AS s
            WHERE t.{filter_column} = s.{filter_column}
            {where_extra};
        """
        conn.execute_query(update_sql, fetch=False)

        # Temp table drops automatically at session end; explicitly drop to be tidy:
        conn.execute_query(f"DROP TABLE IF EXISTS {temp_name};", fetch=False)

        return int(matches)


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
