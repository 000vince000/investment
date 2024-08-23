import duckdb
import pandas as pd

def execute_query(db_name, query, params=None):
    """
    Execute a SQL query against a DuckDB database and return the results as a pandas DataFrame.
    
    :param db_name: Name of the DuckDB database file
    :param query: SQL query to execute
    :param params: Optional parameters for the query (for parameterized queries)
    :return: pandas DataFrame with query results
    """
    conn = duckdb.connect(db_name)
    try:
        if params:
            result = conn.execute(query, params)
        else:
            result = conn.execute(query)
        
        # Convert result to pandas DataFrame
        df = result.df()
        return df
    finally:
        conn.close()

# Example usage
db_name = 'linkedin_jobs.db'

# Simple query example
query1 = "SELECT * FROM linkedin_jobs LIMIT 5"
result1 = execute_query(db_name, query1)
print("First 5 jobs:")
print(result1)

# Parameterized query example
query2 = "SELECT * FROM linkedin_jobs WHERE company = ? AND job_type = ?"
params2 = ('Meta', 'eng')
result2 = execute_query(db_name, query2, params2)
print("\nMeta Software Engineer jobs:")
print(result2)

# Complex query example
query3 = """
SELECT company, COUNT(*) as job_count
FROM linkedin_jobs
GROUP BY company
ORDER BY job_count DESC
LIMIT 10
"""
result3 = execute_query(db_name, query3)
print("\nTop 10 companies by job count:")
print(result3)

