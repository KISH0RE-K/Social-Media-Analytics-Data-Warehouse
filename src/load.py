import duckdb

def load_to_duckdb():
    con = duckdb.connect('data/warehouse/social_analytics.db')
    
    con.execute("""
    CREATE OR REPLACE TABLE fact_events 
    AS SELECT * FROM 'data/warehouse/fact_events.parquet'
    """)
    
    con.execute("""
    CREATE OR REPLACE TABLE dim_users 
    AS SELECT * FROM 'data/warehouse/dim_users.parquet'
    """)
    
    print("Data loaded into DuckDB Warehouse.")
    print(con.execute("SHOW TABLES").fetchall())
    
    con.close()

if __name__ == "__main__":
    load_to_duckdb()