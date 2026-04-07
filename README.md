# Ontario EV Grid Strain Analysis

Snowflake Streamlit-in-Snowflake (SiS) application analyzing the impact of EV adoption on Ontario's electrical grid.

## Features
- Interactive FSA polygon map showing grid stress scores driven by real IESO hourly residential demand data
- Hourly demand slider (12 AM - 11 PM) with IESO residential consumption
- Projection year slider (2025-2028) using per-FSA compound annual growth rates
- Instantaneous EV charging load model (BEV @ 7.2 kW L2, PHEV @ 3.3 kW) with time-of-day coincidence factors
- Grid stress score: 50% EV growth rate + 50% EV load as % of grid demand

## Data Sources
- **IESO**: Hourly Consumption by FSA (Dec 2024 - Nov 2025) — ~5M residential/small-commercial premises
- **Ontario Data Catalogue**: EV registrations by FSA, quarterly Q1 2022 - Q4 2025
- **Statistics Canada**: 2021 Census FSA boundaries (cartographic boundary file) and demographics

## Setup

### Prerequisites
- Snowflake account with ACCOUNTADMIN access
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli) or SnowSQL
- Python 3.11+ with `snowflake-connector-python`, `pandas`, `pyarrow`
- An External Access Integration for PyPI packages (required by SiS for `pydeck`)

### Step 1: Create database, tables, and stage
```sql
-- Run in Snowflake worksheet or via SnowSQL
SOURCE sql/01_setup.sql;
```

### Step 2: Load base data (FSA geography + EV registrations)
```sql
SOURCE sql/02_load_data.sql;
```

### Step 3: Load remaining data via Python scripts
```bash
# Generate FSA demographics (Census-modeled data)
python scripts/generate_census_data.py

# Load historical EV registration CSVs
python scripts/load_historical_ev_data.py

# Load IESO hourly demand + FSA boundaries
python scripts/load_ieso_and_boundaries.py
```

### Step 4: Create views
```sql
SOURCE sql/03_create_views.sql;
```

### Step 5: Deploy Streamlit app
Upload app files to the internal stage, then create the Streamlit app:
```bash
# From the repo root directory, via SnowSQL:
PUT file://grid_strain_sis/streamlit_app.py @ELECTRIFICATION_READINESS.STREAMLIT.STREAMLIT_STAGE/grid_strain_sis AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://grid_strain_sis/pyproject.toml @ELECTRIFICATION_READINESS.STREAMLIT.STREAMLIT_STAGE/grid_strain_sis AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://grid_strain_sis/environment.yml @ELECTRIFICATION_READINESS.STREAMLIT.STREAMLIT_STAGE/grid_strain_sis AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

Then run the deployment SQL (edit the `EXTERNAL_ACCESS_INTEGRATIONS` name to match your account):
```sql
SOURCE sql/04_deploy_streamlit.sql;
```

## Project Structure
```
├── grid_strain_sis/          # Grid Strain Analysis SiS app
│   ├── streamlit_app.py
│   ├── pyproject.toml
│   └── environment.yml
├── sql/
│   ├── 01_setup.sql          # Database, schema, table DDLs
│   ├── 02_load_data.sql      # ONTARIO_FSA + EV_REGISTRATIONS INSERTs
│   ├── 03_create_views.sql   # GRID_STRESS_ANALYSIS view
│   └── 04_deploy_streamlit.sql
├── data/
│   ├── ev_historical/        # 16 quarterly EV registration CSVs
│   ├── ieso_hourly_by_fsa.parquet
│   ├── fsa_boundaries.csv    # FSA polygon boundaries (WKT)
│   └── census_fsa_2021.csv
└── scripts/
    ├── generate_census_data.py
    ├── load_historical_ev_data.py
    ├── load_ieso_and_boundaries.py
    └── preprocess_ieso.py    # Reference: how IESO raw data was aggregated
```

## Snowflake Objects

| Object | Type | Description |
|--------|------|-------------|
| `ONTARIO_FSA` | Table | FSA coordinates, region, utility (586 rows) |
| `EV_REGISTRATIONS` | Table | Quarterly BEV/PHEV counts (9,057 rows) |
| `FSA_DEMOGRAPHICS` | Table | Census dwelling mix, income (569 rows) |
| `IESO_FSA_HOURLY_DEMAND` | Table | Hourly residential demand (12,408 rows) |
| `FSA_BOUNDARIES` | Table | Polygon geometries, GEOGRAPHY type (520 rows) |
| `GRID_STRESS_ANALYSIS` | View | Joins FSA + demographics + EV growth |
