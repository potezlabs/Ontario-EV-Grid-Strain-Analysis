"""
Load IESO hourly demand (parquet) and FSA boundary (CSV with WKT) data into Snowflake.

Usage:
    python load_ieso_and_boundaries.py

Prerequisites:
    - pip install snowflake-connector-python pandas pyarrow
    - Snowflake connection named 'DEMO' (or set SNOWFLAKE_CONNECTION_NAME env var)
    - Tables IESO_FSA_HOURLY_DEMAND and FSA_BOUNDARIES must exist (run sql/01_setup.sql first)
"""

import pandas as pd
import os
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')

conn = connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "DEMO"))
cursor = conn.cursor()

cursor.execute("USE DATABASE ELECTRIFICATION_READINESS")
cursor.execute("USE SCHEMA PUBLIC")


# --- Load IESO hourly demand from parquet ---
print("Loading IESO hourly demand data...")
ieso_path = os.path.join(DATA_DIR, "ieso_hourly_by_fsa.parquet")
ieso_df = pd.read_parquet(ieso_path)

# Ensure column names match table
ieso_df.columns = [c.upper() for c in ieso_df.columns]
ieso_df = ieso_df[['FSA', 'HOUR', 'AVG_DAILY_KWH', 'TOTAL_ANNUAL_KWH', 'PREMISE_COUNT']]

cursor.execute("DELETE FROM IESO_FSA_HOURLY_DEMAND")
conn.commit()

success, nchunks, nrows, _ = write_pandas(conn, ieso_df, 'IESO_FSA_HOURLY_DEMAND')
print(f"  Loaded {nrows} rows into IESO_FSA_HOURLY_DEMAND (success={success})")


# --- Load FSA boundaries from CSV (WKT format) ---
print("Loading FSA boundary data...")
boundaries_path = os.path.join(DATA_DIR, "fsa_boundaries.csv")
boundaries_df = pd.read_csv(boundaries_path)

cursor.execute("DELETE FROM FSA_BOUNDARIES")
conn.commit()

# Insert boundaries using ST_GEOGRAPHYFROMWKT to convert WKT to GEOGRAPHY
loaded = 0
errors = 0
for _, row in boundaries_df.iterrows():
    fsa = row['FSA']
    wkt = row['WKT']
    try:
        cursor.execute(
            "INSERT INTO FSA_BOUNDARIES (FSA, GEOMETRY) "
            "SELECT %s, ST_GEOGRAPHYFROMWKT(%s)",
            (fsa, wkt)
        )
        loaded += 1
    except Exception as e:
        errors += 1
        print(f"  Warning: Failed to load {fsa}: {str(e)[:100]}")

conn.commit()
print(f"  Loaded {loaded} FSA boundaries ({errors} errors)")


# --- Verification ---
cursor.execute("SELECT COUNT(*) FROM IESO_FSA_HOURLY_DEMAND")
print(f"\nVerification: IESO_FSA_HOURLY_DEMAND = {cursor.fetchone()[0]} rows")

cursor.execute("SELECT COUNT(*) FROM FSA_BOUNDARIES WHERE GEOMETRY IS NOT NULL")
print(f"Verification: FSA_BOUNDARIES = {cursor.fetchone()[0]} rows with geometry")

cursor.close()
conn.close()
print("Done.")
