-- =====================================================
-- Ontario EV Grid Strain Analysis
-- Step 3: Create Views
-- =====================================================

USE DATABASE ELECTRIFICATION_READINESS;
USE SCHEMA PUBLIC;

-- =====================================================
-- View 1: Grid Stress Analysis
-- Joins FSA geography + demographics + EV growth
-- Used by the Grid Strain SiS app
-- =====================================================

CREATE OR REPLACE VIEW GRID_STRESS_ANALYSIS AS
WITH ev_growth AS (
    SELECT 
        e1.FSA,
        COALESCE(e1.TOTAL_EV, 0) as ev_2022_q1,
        COALESCE(e2.TOTAL_EV, 0) as ev_2025_q4,
        COALESCE(e2.BEV_COUNT, 0) as bev_current,
        COALESCE(e2.PHEV_COUNT, 0) as phev_current,
        CASE 
            WHEN COALESCE(e1.TOTAL_EV, 0) >= 10 AND COALESCE(e2.TOTAL_EV, 0) > 0
            THEN POWER(e2.TOTAL_EV::FLOAT / e1.TOTAL_EV::FLOAT, 1.0/3.0) - 1
            WHEN COALESCE(e1.TOTAL_EV, 0) < 10 AND COALESCE(e2.TOTAL_EV, 0) >= 50
            THEN 0.50
            ELSE 0.25 
        END as cagr_3yr
    FROM EV_REGISTRATIONS e1
    JOIN EV_REGISTRATIONS e2 
        ON e1.FSA = e2.FSA
    WHERE e1.QUARTER = '2022-Q1' AND e2.QUARTER = '2025-Q4'
),
energy_calc AS (
    SELECT 
        d.FSA,
        d.POPULATION,
        d.TOTAL_DWELLINGS,
        d.SINGLE_DETACHED,
        d.APARTMENT_5PLUS,
        d.MEDIAN_INCOME,
        (d.SINGLE_DETACHED * 11000 +
         d.SEMI_DETACHED * 8500 +
         d.ROW_HOUSE * 7500 +
         d.APT_DUPLEX * 5500 +
         d.APARTMENT_5PLUS * 4500) as baseline_load_kwh,
        CASE WHEN d.TOTAL_DWELLINGS > 0 
             THEN (d.SINGLE_DETACHED + d.SEMI_DETACHED + d.ROW_HOUSE * 0.5)::FLOAT / d.TOTAL_DWELLINGS::FLOAT
             ELSE 0.5 
        END as home_charging_ratio
    FROM FSA_DEMOGRAPHICS d
)
SELECT 
    f.FSA,
    f.LAT,
    f.LON,
    f.REGION,
    f.UTILITY,
    ec.POPULATION,
    ec.TOTAL_DWELLINGS,
    ec.SINGLE_DETACHED,
    ec.APARTMENT_5PLUS,
    ec.MEDIAN_INCOME,
    ec.baseline_load_kwh,
    ROUND(ec.home_charging_ratio, 3) as home_charging_ratio,
    eg.ev_2022_q1,
    eg.ev_2025_q4,
    eg.bev_current,
    eg.phev_current,
    ROUND(eg.cagr_3yr * 100, 1) as ev_growth_rate_pct,
    (eg.bev_current * 4000 + eg.phev_current * 2000) as current_ev_load_kwh,
    ROUND(eg.ev_2025_q4 * POWER(1 + eg.cagr_3yr, 3)) as projected_ev_2028,
    ROUND(eg.ev_2025_q4 * POWER(1 + eg.cagr_3yr, 3) * 3500) as projected_ev_load_2028_kwh,
    ROUND(((eg.bev_current * 4000 + eg.phev_current * 2000)::FLOAT / NULLIF(ec.baseline_load_kwh, 0)::FLOAT) * 100, 2) as current_load_increase_pct,
    ROUND(1 + (1 - ec.home_charging_ratio) * 0.5, 2) as charging_constraint_factor,
    ROUND(
        LEAST(100,
            (LEAST(eg.cagr_3yr, 0.5) / 0.5) * 40 +
            (LEAST((eg.bev_current * 4000 + eg.phev_current * 2000)::FLOAT / NULLIF(ec.baseline_load_kwh, 0)::FLOAT, 0.15) / 0.15) * 30 +
            ((1 - ec.home_charging_ratio) * 30)
        ), 1
    ) as grid_stress_score
FROM ONTARIO_FSA f
JOIN energy_calc ec ON f.FSA = ec.FSA
JOIN ev_growth eg ON f.FSA = eg.FSA;
