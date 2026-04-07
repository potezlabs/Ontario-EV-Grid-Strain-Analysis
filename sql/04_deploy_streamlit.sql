-- =====================================================
-- Ontario EV Grid Strain Analysis
-- Step 4: Deploy Streamlit-in-Snowflake App
-- =====================================================
-- Prerequisites:
--   1. All tables populated (steps 1-3 + Python scripts)
--   2. An external access integration for pip packages
--      (required by SiS for pydeck). Adjust the name below
--      to match your account's integration.
-- =====================================================

USE DATABASE ELECTRIFICATION_READINESS;
USE SCHEMA STREAMLIT;

-- Upload Grid Strain app files to stage
-- Run these PUT commands from SnowSQL or the Snowflake CLI:
--
--   PUT file://grid_strain_sis/streamlit_app.py @STREAMLIT_STAGE/grid_strain_sis AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://grid_strain_sis/pyproject.toml @STREAMLIT_STAGE/grid_strain_sis AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://grid_strain_sis/environment.yml @STREAMLIT_STAGE/grid_strain_sis AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create Grid Strain Analysis Streamlit app
CREATE OR REPLACE STREAMLIT ELECTRIFICATION_READINESS.STREAMLIT.GRID_STRAIN_ANALYSIS
    FROM '@ELECTRIFICATION_READINESS.STREAMLIT.STREAMLIT_STAGE/grid_strain_sis'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'CUSTOMER_ANALYTICS_WH'
    -- Replace with your account's external access integration name:
    -- EXTERNAL_ACCESS_INTEGRATIONS = (YOUR_PYPI_ACCESS_INTEGRATION)
    TITLE = 'Ontario Grid Strain Analysis';
