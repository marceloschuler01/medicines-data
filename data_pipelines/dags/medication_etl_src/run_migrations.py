import os
import psycopg2

# Database connection settings
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),  # Change if using a remote server
    'port': os.getenv('DB_PORT')  # Default PostgreSQL port
}

# Path to the migrations folder
migrations_folder = './migrations'

# Function to execute SQL migration files
def run_migrations():
    # Establish a connection to the database
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Loop through all the SQL migration files in the folder
        for filename in sorted(os.listdir(migrations_folder)):
            if filename.endswith('.sql'):
                file_path = os.path.join(migrations_folder, filename)

                # Read the contents of the migration file
                with open(file_path, 'r') as file:
                    sql_query = file.read()

                # Execute the SQL query
                print(f"Running migration: {filename}")
                cursor.execute(sql_query)
                conn.commit()  # Commit changes to the database

        # Close the cursor and connection
        cursor.close()
        conn.close()
        print("All migrations have been applied successfully!")

    except Exception as e:
        print(f"Error running migrations: {e}")

# Run the migrations
if __name__ == '__main__':
    run_migrations()
