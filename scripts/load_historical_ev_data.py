import pandas as pd
import os
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
EV_DIR = os.path.join(DATA_DIR, 'ev_historical')

conn = connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "DEMO"))
cursor = conn.cursor()

cursor.execute("USE DATABASE ELECTRIFICATION_READINESS")
cursor.execute("USE SCHEMA PUBLIC")
cursor.execute("DELETE FROM EV_REGISTRATIONS")
conn.commit()

quarters = [
    ('ev_2022_q1.csv', '2022-Q1'), ('ev_2022_q2.csv', '2022-Q2'),
    ('ev_2022_q3.csv', '2022-Q3'), ('ev_2022_q4.csv', '2022-Q4'),
    ('ev_2023_q1.csv', '2023-Q1'), ('ev_2023_q2.csv', '2023-Q2'),
    ('ev_2023_q3.csv', '2023-Q3'), ('ev_2023_q4.csv', '2023-Q4'),
    ('ev_2024_q1.csv', '2024-Q1'), ('ev_2024_q2.csv', '2024-Q2'),
    ('ev_2024_q3.csv', '2024-Q3'), ('ev_2024_q4.csv', '2024-Q4'),
    ('ev_2025_q1.csv', '2025-Q1'), ('ev_2025_q2.csv', '2025-Q2'),
    ('ev_2025_q3.csv', '2025-Q3'), ('ev_2025_q4.csv', '2025-Q4'),
]

all_data = []
for filename, quarter in quarters:
    filepath = os.path.join(EV_DIR, filename)
    df = pd.read_csv(filepath)
    df.columns = [c.upper().replace(' ', '_') for c in df.columns]
    if 'TOTAL_EV' not in df.columns and 'TOTALEV' in df.columns:
        df = df.rename(columns={'TOTALEV': 'TOTAL_EV'})
    
    ontario_df = df[df['FSA'].str.match(r'^[KLMNP][0-9][A-Z]$', na=False)].copy()
    ontario_df['QUARTER'] = quarter
    ontario_df['BEV_COUNT'] = ontario_df['BEV'].fillna(0).astype(int)
    ontario_df['PHEV_COUNT'] = ontario_df['PHEV'].fillna(0).astype(int)
    ontario_df['TOTAL_EV'] = ontario_df['TOTAL_EV'].fillna(0).astype(int)
    
    all_data.append(ontario_df[['FSA', 'QUARTER', 'BEV_COUNT', 'PHEV_COUNT', 'TOTAL_EV']])
    print(f"Processed {len(ontario_df)} FSAs for {quarter}")

combined_df = pd.concat(all_data, ignore_index=True)
print(f"\nTotal records to load: {len(combined_df)}")

success, nchunks, nrows, _ = write_pandas(conn, combined_df, 'EV_REGISTRATIONS')
print(f"Loaded {nrows} rows in {nchunks} chunks, success={success}")

cursor.execute("SELECT COUNT(*) as cnt, COUNT(DISTINCT QUARTER) as quarters FROM EV_REGISTRATIONS")
result = cursor.fetchone()
print(f"Verification: {result[0]} rows across {result[1]} quarters")

cursor.close()
conn.close()
