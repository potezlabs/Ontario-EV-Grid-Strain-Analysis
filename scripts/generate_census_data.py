import pandas as pd
import numpy as np
import os
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

conn = connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "DEMO"))
cursor = conn.cursor()

cursor.execute("USE DATABASE ELECTRIFICATION_READINESS")
cursor.execute("USE SCHEMA PUBLIC")

cursor.execute("SELECT DISTINCT FSA, REGION FROM ONTARIO_FSA")
fsas = cursor.fetchall()

np.random.seed(42)

dwelling_data = []
for fsa, region in fsas:
    first_char = fsa[0]
    second_char = fsa[1]
    
    if first_char == 'M':
        base_pop = np.random.randint(25000, 60000)
        apt_ratio = np.random.uniform(0.55, 0.80)
        single_ratio = np.random.uniform(0.08, 0.20)
        median_income = np.random.randint(55000, 120000)
    elif first_char in ['L', 'N'] and second_char in ['0', '1', '2', '3']:
        base_pop = np.random.randint(8000, 25000)
        apt_ratio = np.random.uniform(0.15, 0.35)
        single_ratio = np.random.uniform(0.50, 0.70)
        median_income = np.random.randint(70000, 110000)
    elif second_char == '0':
        base_pop = np.random.randint(3000, 12000)
        apt_ratio = np.random.uniform(0.05, 0.15)
        single_ratio = np.random.uniform(0.70, 0.85)
        median_income = np.random.randint(60000, 95000)
    else:
        base_pop = np.random.randint(15000, 40000)
        apt_ratio = np.random.uniform(0.25, 0.45)
        single_ratio = np.random.uniform(0.35, 0.55)
        median_income = np.random.randint(65000, 100000)
    
    avg_hh_size = np.random.uniform(2.2, 2.8)
    total_dwellings = int(base_pop / avg_hh_size)
    
    remaining = 1.0 - apt_ratio - single_ratio
    semi_ratio = remaining * np.random.uniform(0.2, 0.4)
    row_ratio = remaining * np.random.uniform(0.2, 0.4)
    apt_duplex_ratio = remaining - semi_ratio - row_ratio
    
    single_detached = int(total_dwellings * single_ratio)
    semi_detached = int(total_dwellings * semi_ratio)
    row_house = int(total_dwellings * row_ratio)
    apt_duplex = int(total_dwellings * apt_duplex_ratio)
    apt_5plus = int(total_dwellings * apt_ratio)
    
    dwelling_data.append({
        'FSA': fsa,
        'POPULATION': base_pop,
        'TOTAL_DWELLINGS': total_dwellings,
        'SINGLE_DETACHED': single_detached,
        'SEMI_DETACHED': semi_detached,
        'ROW_HOUSE': row_house,
        'APT_DUPLEX': apt_duplex,
        'APARTMENT_5PLUS': apt_5plus,
        'MEDIAN_INCOME': median_income
    })

df = pd.DataFrame(dwelling_data)
print(f"Generated {len(df)} FSA demographic records")
print(f"\nSample data:")
print(df.head(10).to_string())

cursor.execute("DELETE FROM FSA_DEMOGRAPHICS")

cursor.execute("""
    ALTER TABLE FSA_DEMOGRAPHICS ADD COLUMN IF NOT EXISTS TOTAL_DWELLINGS INT
""")
cursor.execute("""
    ALTER TABLE FSA_DEMOGRAPHICS ADD COLUMN IF NOT EXISTS SEMI_DETACHED INT
""")
cursor.execute("""
    ALTER TABLE FSA_DEMOGRAPHICS ADD COLUMN IF NOT EXISTS ROW_HOUSE INT
""")
cursor.execute("""
    ALTER TABLE FSA_DEMOGRAPHICS ADD COLUMN IF NOT EXISTS APT_DUPLEX INT
""")
conn.commit()

upload_df = df[['FSA', 'POPULATION', 'TOTAL_DWELLINGS', 'SINGLE_DETACHED', 
                'SEMI_DETACHED', 'ROW_HOUSE', 'APT_DUPLEX', 'APARTMENT_5PLUS', 'MEDIAN_INCOME']]

success, nchunks, nrows, _ = write_pandas(conn, upload_df, 'FSA_DEMOGRAPHICS')
print(f"\nLoaded {nrows} rows, success={success}")

cursor.execute("SELECT COUNT(*) FROM FSA_DEMOGRAPHICS")
print(f"Verification: {cursor.fetchone()[0]} rows in FSA_DEMOGRAPHICS")

cursor.close()
conn.close()
