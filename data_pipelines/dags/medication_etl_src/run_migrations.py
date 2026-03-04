import os
import sys
from medication_etl_src.database.api_database import ApiDatabase as sql
from medication_etl_src.database.db_connector import with_database_connection

# Path to the migrations folder (relative to *this* file, not the CWD)
migrations_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'migrations')

@with_database_connection
def run_migrations(conn=None, migration_name=None):
    """
    Run all migrations or a specific one if migration_name is provided.
    """
    try:
        # Get all .sql files in sorted order
        migration_files = sorted(
            [f for f in os.listdir(migrations_folder) if f.endswith('.sql')]
        )

        # If a specific migration is provided, filter to only that one
        if migration_name:
            if migration_name not in migration_files:
                print(f"Migration '{migration_name}' not found in {migrations_folder}")
                return
            migration_files = [migration_name]

        for filename in migration_files:
            file_path = os.path.join(migrations_folder, filename)
            with open(file_path, 'r') as file:
                sql_query = file.read()

            print(f"Running migration: {filename}")
            sql.execute(sql_query, conn=conn)

        print("Selected migrations have been applied successfully!")

    except Exception as e:
        print(f"Error running migrations: {e}")
        raise e


if __name__ == '__main__':
    # Allow passing the migration name as a command-line argument
    migration_to_run = sys.argv[1] if len(sys.argv) > 1 else None
    run_migrations(migration_name=migration_to_run)
