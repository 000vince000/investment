import duckdb
import csv
import os

def create_table_and_insert_csv(db_name, table_name, csv_file):
    # Connect to the database (or create it if it doesn't exist)
    conn = duckdb.connect(db_name)

    # Read the CSV file to get column names and data types
    with open(csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        headers = next(csv_reader)  # Get the first row (headers)

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f"{header} VARCHAR" for header in headers])}
        , PRIMARY KEY (job_id)
    )
    """
    conn.execute(create_table_query)

    existing_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Existing rows before import: {existing_rows}")

    # Import CSV data into the table
    import_query = f"""
        INSERT INTO {table_name}
        SELECT * FROM read_csv_auto('{csv_file}', header=true, sample_size=-1)
        ON CONFLICT (job_id) DO NOTHING
        RETURNING *
    """
    conn.execute(import_query)

    # Count total rows after import
    final_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    print(f"Total rows after import: {final_rows}")

    conn.commit()
    conn.close()

def main():
    db_name = 'linkedin_jobs.db'
    table_name = 'jobs'
    csv_file = 'linkedin-job-scraper-database.csv'

    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found.")
        return

    print(f"Starting import process for '{csv_file}' into '{db_name}'...")
    create_table_and_insert_csv(db_name, table_name, csv_file)
    print("Import process completed.")

if __name__ == "__main__":
    main()
