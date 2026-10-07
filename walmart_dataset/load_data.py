import psycopg2
import os
from dotenv import load_dotenv

# Database connection string (read from DATABASE_URL in .env)
load_dotenv()
conn_string = os.environ["DATABASE_URL"]

# CSV files mapping to tables
csv_files = {
    "customers.csv": "raw.customers",
    "stores.csv": "raw.stores",
    "products.csv": "raw.products",
    "employees.csv": "raw.employees",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}

base_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(base_dir, "data")
schema_file = os.path.join(base_dir, "ddl", "walmart_schema.sql")

conn = None
try:
    # Connect to the database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Create the raw schema and its tables if they don't exist yet
    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cursor.execute("SELECT to_regclass('raw.customers')")
    if cursor.fetchone()[0] is None:
        print("Creating tables in schema raw...")
        cursor.execute("SET search_path TO raw")
        with open(schema_file, 'r', encoding='utf-8') as f:
            cursor.execute(f.read())
        cursor.execute("SET search_path TO public")
    conn.commit()

    # Load each CSV file into its corresponding table
    for csv_file, table_name in csv_files.items():
        csv_path = os.path.join(data_dir, csv_file)

        if os.path.exists(csv_path):
            print(f"Loading {csv_file} into {table_name}...")

            # Clear old rows so the script can be re-run safely
            cursor.execute(f"TRUNCATE {table_name}")
            with open(csv_path, 'r', encoding='utf-8') as f:
                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)

            conn.commit()
            print(f"✓ Successfully loaded {csv_file}")
        else:
            print(f"✗ File not found: {csv_path}")

    cursor.close()
    conn.close()
    print("\n✓ All data loaded successfully!")

except Exception as e:
    print(f"Error: {e}")
    if conn:
        conn.rollback()
        conn.close()
    raise
