import os
import psycopg2
from psycopg2 import OperationalError


def with_database_connection(func):
    def wrapper(*args, **kwargs):

        if 'conn' in kwargs and kwargs['conn'] is not None:
            result = func(*args, **kwargs)
            return result

        kwargs['conn'] = PostgresConnection()
        result = func(*args, **kwargs)
        kwargs['conn'].commit()
        if not kwargs['conn'].closed:
            kwargs['conn'].close()
        return result

    return wrapper

class PostgresConnection:
    def __init__(self):
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT", 5432)
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            # Desliga o autocommit explicitamente
            self.conn.autocommit = False
            print("✅ Conexão estabelecida (autocommit desativado).")
        except OperationalError as e:
            print(f"❌ Erro na conexão: {e}")
            self.conn = None

    @property
    def closed(self):
        return self.conn is None or self.conn.closed != 0

    def execute_query(self, query, params=None, fetch=False):
        if self.conn is None:
            print("⚠️ Não há conexão ativa.")
            return None

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                return True
        except Exception as e:
            print(f"❌ Erro ao executar query: {e}")
            return None

    def commit(self):
        if self.conn:
            self.conn.commit()
            print("💾 Transação confirmada (commit).")

    def rollback(self):
        if self.conn:
            self.conn.rollback()
            print("↩️ Transação revertida (rollback).")

    def close(self):
        if self.conn:
            self.conn.close()
            print("🔒 Conexão encerrada.")
