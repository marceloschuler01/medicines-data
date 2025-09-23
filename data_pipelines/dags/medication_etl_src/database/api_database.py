import os

db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),  # Change if using a remote server
    'port': os.getenv('DB_PORT')  # Default PostgreSQL port
}