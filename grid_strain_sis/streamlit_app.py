import streamlit as st
import pandas as pd
import pydeck as pdk
import numpy as np
import json
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Ontario Grid Strain Analysis",
    page_icon="⚡",
    layout="wide"
)

# --- Coincidence factor by hour (fraction of EVs actively charging at each hour) ---
# Based on utility studies of residential EV charging behavior.
# Peak coincidence 0.50 at 9-11 PM (everyone plugs in after arriving home).
# Trough ~0.05 midday (most EVs parked at work, not charging at home).
EV_COINCIDENCE_FACTOR = {
    1: 0.30, 2: 0.25, 3: 0.20, 4: 0.15, 5: 0.10, 6: 0.08,
    7: 0.05, 8: 0.05, 9: 0.05, 10: 0.05, 11: 0.05, 12: 0.06,
    13: 0.06, 14: 0.05, 15: 0.05, 16: 0.06, 17: 0.10, 18: 0.20,
    19: 0.30, 20: 0.40, 21: 0.50, 22: 0.50, 23: 0.45, 24: 0.35,
}

# Charger draw rates (kW per vehicle while actively charging)
BEV_CHARGER_KW = 7.2   # Level 2 typical: 240V / 30A
PHEV_CHARGER_KW = 3.3   # Level 1 / small L2: 240V / 16A

HOUR_LABELS = {
    1: "12 AM", 2: "1 AM", 3: "2 AM", 4: "3 AM", 5: "4 AM", 6: "5 AM",
    7: "6 AM", 8: "7 AM", 9: "8 AM", 10: "9 AM", 11: "10 AM", 12: "11 AM",
    13: "12 PM", 14: "1 PM", 15: "2 PM", 16: "3 PM", 17: "4 PM", 18: "5 PM",
    19: "6 PM", 20: "7 PM", 21: "8 PM", 22: "9 PM", 23: "10 PM", 24: "11 PM",
}


@st.cache_resource
def get_session():
    return get_active_session()


@st.cache_data(ttl=300)
def load_ieso_demand():
    """Load pre-aggregated IESO hourly demand data from Snowflake."""
    session = get_session()
    return session.sql("SELECT * FROM ELECTRIFICATION_READINESS.PUBLIC.IESO_FSA_HOURLY_DEMAND").to_pandas()


@st.cache_data(ttl=300)
def load_stress_base():
    """Load the base stress data from Snowflake (EV + FSA metadata)."""
    session = get_session()
    return session.sql("SELECT * FROM ELECTRIFICATION_READINESS.PUBLIC.GRID_STRESS_ANALYSIS").to_pandas()


@st.cache_data(ttl=300)
def load_fsa_boundaries():
    """Load FSA polygon boundaries as GeoJSON."""
    session = get_session()
    return session.sql("""
        SELECT FSA, TO_VARCHAR(ST_ASGEOJSON(GEOMETRY)) as GEOJSON
        FROM ELECTRIFICATION_READINESS.PUBLIC.FSA_BOUNDARIES
        WHERE GEOMETRY IS NOT NULL
    """).to_pandas()


def get_utilities(base_df):
    utilities = sorted(base_df['UTILITY'].dropna().unique().tolist())
    return ["All Utilities"] + utilities


def score_to_color(score):
    """Smooth gradient color for stress scores: green -> yellow/orange -> red."""
    if score < 40:
        t = score / 40
        r = int(22 + (134 - 22) * t)
        g = int(163 + (239 - 163) * t)
        b = int(74 + (124 - 74) * t)
        alpha = int(120 + t * 40)
    elif score < 70:
        t = (score - 40) / 30
        r = int(234 + (251 - 234) * t)
        g = int(179 + (146 - 179) * t)
        b = int(8 + (60 - 8) * t)
        alpha = int(150 + t * 30)
    else:
        t = min((score - 70) / 30, 1)
        r = int(249 + (153 - 249) * t)
        g = int(115 + (27 - 115) * t)
        b = int(22 + (27 - 22) * t)
        alpha = int(170 + t * 30)
    return [r, g, b, alpha]


def render_stress_legend():
    st.markdown("**Grid Stress Score**")
    cols = st.columns(3)
    with cols[0]:
        st.markdown('<div style="background: linear-gradient(to right, #16a34a, #86ef7c); height:20px; border-radius:4px;"></div>', unsafe_allow_html=True)
        st.caption("Low (0-40)")
    with cols[1]:
        st.markdown('<div style="background: linear-gradient(to right, #eab308, #fb923c); height:20px; border-radius:4px;"></div>', unsafe_allow_html=True)
        st.caption("Medium (40-70)")
    with cols[2]:
        st.markdown('<div style="background: linear-gradient(to right, #f97316, #991b1b); height:20px; border-radius:4px;"></div>', unsafe_allow_html=True)
        st.caption("High (70-100)")


def compute_hourly_stress(base_df, ieso_df, hour, projection_year=2025):
    """
    Recompute grid stress scores using actual IESO demand for a given hour.

    Uses instantaneous EV charging load: how many EVs are plugged in simultaneously
    at this hour (coincidence factor) x charger draw rate (kW).
    Projects EV counts forward from Q4 2025 baseline using per-FSA CAGR.
    """
    hour_demand = ieso_df[ieso_df['HOUR'] == hour][['FSA', 'AVG_DAILY_KWH', 'TOTAL_ANNUAL_KWH']].copy()
    hour_demand = hour_demand.rename(columns={'AVG_DAILY_KWH': 'ACTUAL_HOURLY_KWH'})

    merged = base_df.merge(hour_demand, on='FSA', how='left')
    merged['ACTUAL_HOURLY_KWH'] = merged['ACTUAL_HOURLY_KWH'].fillna(0)
    merged['TOTAL_ANNUAL_KWH'] = merged['TOTAL_ANNUAL_KWH'].fillna(0)

    # Project EV counts forward using per-FSA CAGR
    years_ahead = projection_year - 2025
    if years_ahead > 0:
        growth_factor = (1 + merged['EV_GROWTH_RATE_PCT'] / 100.0) ** years_ahead
        bev_count = (merged['BEV_CURRENT'] * growth_factor).round(0)
        phev_count = (merged['PHEV_CURRENT'] * growth_factor).round(0)
    else:
        bev_count = merged['BEV_CURRENT']
        phev_count = merged['PHEV_CURRENT']

    merged['BEV_PROJECTED'] = bev_count
    merged['PHEV_PROJECTED'] = phev_count
    merged['EV_PROJECTED_TOTAL'] = bev_count + phev_count

    # Instantaneous EV charging load at this hour (kW = kWh in one hour)
    coincidence = EV_COINCIDENCE_FACTOR.get(hour, 0.10)
    hourly_ev_load = (
        bev_count * coincidence * BEV_CHARGER_KW
        + phev_count * coincidence * PHEV_CHARGER_KW
    )

    merged['ACTUAL_LOAD_INCREASE_PCT'] = np.where(
        merged['ACTUAL_HOURLY_KWH'] > 0,
        (hourly_ev_load / merged['ACTUAL_HOURLY_KWH']) * 100,
        0
    ).round(2)

    cagr = merged['EV_GROWTH_RATE_PCT'] / 100.0
    growth_component = (np.minimum(cagr, 0.70) / 0.70) * 50

    load_ratio = np.where(
        merged['ACTUAL_HOURLY_KWH'] > 0,
        hourly_ev_load / merged['ACTUAL_HOURLY_KWH'],
        0
    )
    load_component = (np.minimum(load_ratio, 0.35) / 0.35) * 50

    merged['GRID_STRESS_SCORE'] = np.minimum(
        100, growth_component + load_component
    ).round(1)

    merged['HOURLY_EV_LOAD_KWH'] = hourly_ev_load.round(1)
    merged['HOUR'] = hour

    return merged


def create_stress_map(hourly_df, boundaries_df, center_lat=44.0, center_lon=-79.5, zoom=6, projection_year=2025):
    """Create a GeoJsonLayer polygon map colored by stress score."""
    merged = boundaries_df.merge(hourly_df, on='FSA', how='inner')

    geojson_features = []
    for _, row in merged.iterrows():
        geojson_str = row.get('GEOJSON')
        if not geojson_str or geojson_str == 'None' or pd.isna(geojson_str):
            continue
        try:
            geom = json.loads(geojson_str)
            if not geom or geom.get('type') not in ('Polygon', 'MultiPolygon'):
                continue
            score = float(row['GRID_STRESS_SCORE'])
            fill_color = score_to_color(score)
            line_color = [fill_color[0], fill_color[1], fill_color[2], 255]

            properties = {
                "FSA": row['FSA'],
                "fillColor": fill_color,
                "lineColor": line_color,
                "Region": str(row.get('REGION', '')),
                "Utility": str(row.get('UTILITY', '')),
                "Stress": round(score, 1),
                "Demand_kWh": int(round(float(row.get('ACTUAL_HOURLY_KWH', 0)))),
                "EV_Load_Pct": round(float(row.get('ACTUAL_LOAD_INCREASE_PCT', 0)), 2),
                "EVs_Baseline": int(float(row.get('EV_2025_Q4', 0))),
                "EVs_Projected": int(float(row.get('EV_PROJECTED_TOTAL', 0))),
                "Growth_Pct": round(float(row.get('EV_GROWTH_RATE_PCT', 0)), 1),
                "Proj_Year": projection_year,
            }

            feature = {"type": "Feature", "geometry": geom, "properties": properties}
            geojson_features.append(feature)
        except (json.JSONDecodeError, ValueError, TypeError, KeyError):
            continue

    geojson_data = {"type": "FeatureCollection", "features": geojson_features}

    polygon_layer = pdk.Layer(
        "GeoJsonLayer",
        data=geojson_data,
        opacity=0.8,
        stroked=True,
        filled=True,
        get_fill_color="properties.fillColor",
        get_line_color="properties.lineColor",
        get_line_width=2,
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
        highlight_color=[255, 255, 0, 100],
    )

    view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom, pitch=0)

    tooltip_html = """
    <b>{FSA}</b><br/>
    {Region} | {Utility}<br/>
    <hr style='margin:4px 0'/>
    Stress Score: <b>{Stress}</b><br/>
    Demand (this hour): <b>{Demand_kWh} kWh</b><br/>
    EV Load Impact: <b>{EV_Load_Pct}%</b><br/>
    <hr style='margin:4px 0'/>
    EVs (2025 Baseline): {EVs_Baseline}<br/>
    EVs ({Proj_Year} Projected): {EVs_Projected}<br/>
    Growth: {Growth_Pct}%/yr
    """

    return pdk.Deck(
        layers=[polygon_layer],
        initial_view_state=view_state,
        map_provider="",
        map_style="",
        tooltip={"html": tooltip_html, "style": {"background": "white", "color": "#333"}}
    )


def main():
    st.markdown("""
    <style>
    .main-header { font-size: 3.5rem; font-weight: bold; color: #1E90FF; margin-bottom: 0; }
    .sub-header { font-size: 1.4rem; color: #888; margin-top: 0; }
    .hour-display {
        font-size: 2rem; font-weight: bold; text-align: center;
        color: #1E90FF; padding: 0.2rem 0; margin-bottom: 0.5rem;
    }
    iframe[title="pydeck.io"] { height: 700px !important; }
    [data-testid="stDeckGlJsonChart"] { height: 700px !important; }
    [data-testid="stDeckGlJsonChart"] iframe { height: 700px !important; }
    [data-testid="stDeckGlJsonChart"] > div { height: 700px !important; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<p class="main-header">Ontario Grid Strain Analysis</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">EV Adoption Impact on Grid | IESO Actual Demand (Dec 2024 - Nov 2025)</p>', unsafe_allow_html=True)

    base_df = load_stress_base()
    ieso_df = load_ieso_demand()
    boundaries_df = load_fsa_boundaries()

    col1, col2 = st.columns(2)
    with col1:
        utilities = get_utilities(base_df)
        selected_utility = st.selectbox("Filter by Utility", utilities, index=0)
    with col2:
        view_options = {
            "Ontario Overview": {"lat": 44.0, "lon": -79.5, "zoom": 6},
            "Toronto / GTA": {"lat": 43.7, "lon": -79.4, "zoom": 9},
            "Ottawa Region": {"lat": 45.4, "lon": -75.7, "zoom": 9},
            "Southwestern Ontario": {"lat": 43.0, "lon": -81.0, "zoom": 8},
        }
        selected_view = st.selectbox("Map View", list(view_options.keys()))

    HOUR_OPTIONS = [HOUR_LABELS[h] for h in range(1, 25)]
    col_hour, col_year = st.columns([3, 1])
    with col_hour:
        selected_label = st.select_slider(
            "Hour of Day", options=HOUR_OPTIONS,
            value=HOUR_LABELS[1],
            key="hour_slider",
        )
    with col_year:
        projection_year = st.select_slider(
            "Projection Year", options=list(range(2025, 2029)),
            value=2025,
            key="year_slider",
        )
    current_hour = HOUR_OPTIONS.index(selected_label) + 1

    filtered_base = base_df.copy()
    if selected_utility and selected_utility != "All Utilities":
        filtered_base = filtered_base[filtered_base['UTILITY'] == selected_utility]

    hourly_df = compute_hourly_stress(filtered_base, ieso_df, current_hour, projection_year)

    hour_label = HOUR_LABELS[current_hour]
    total_demand_mwh = hourly_df['ACTUAL_HOURLY_KWH'].sum() / 1000
    year_label = f" ({projection_year} Projection)" if projection_year > 2025 else ""
    st.markdown(
        f'<div class="hour-display">{hour_label}{year_label} — Residential Demand (IESO): {total_demand_mwh:,.0f} MWh</div>',
        unsafe_allow_html=True
    )

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        total_evs = int(hourly_df['EV_PROJECTED_TOTAL'].sum())
        st.metric(f"Total EVs ({projection_year})", f"{total_evs:,}")
    with col2:
        avg_stress = hourly_df['GRID_STRESS_SCORE'].mean()
        st.metric("Avg Stress Score", f"{avg_stress:.1f}")
    with col3:
        high_stress_count = len(hourly_df[hourly_df['GRID_STRESS_SCORE'] >= 70])
        st.metric("High Stress FSAs", high_stress_count)
    with col4:
        avg_load_pct = hourly_df['ACTUAL_LOAD_INCREASE_PCT'].mean()
        st.metric("Avg EV Load Impact", f"{avg_load_pct:.2f}%")
    with col5:
        ev_load_mw = hourly_df['HOURLY_EV_LOAD_KWH'].sum() / 1000
        st.metric("EV Load (this hour)", f"{ev_load_mw:,.0f} MW")

    col_legend, _ = st.columns([1, 2])
    with col_legend:
        render_stress_legend()

    view = view_options[selected_view]
    deck = create_stress_map(hourly_df, boundaries_df, view["lat"], view["lon"], view["zoom"], projection_year)
    st.pydeck_chart(deck, use_container_width=True)
    st.markdown('<div style="margin-top: 200px;"></div>', unsafe_allow_html=True)

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 Highest Stress FSAs")
        top_stress = hourly_df.nlargest(10, 'GRID_STRESS_SCORE')[
            ['FSA', 'REGION', 'GRID_STRESS_SCORE', 'EV_GROWTH_RATE_PCT', 'ACTUAL_LOAD_INCREASE_PCT', 'EV_PROJECTED_TOTAL']
        ].copy()
        top_stress.columns = ['FSA', 'Region', 'Stress', 'Growth%', 'Load%', f'EVs {projection_year}']
        st.dataframe(top_stress, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Grid Stress Score Formula")
        st.markdown("""
        **Score = EV Growth (50%) + Load Increase (50%)**

        - **EV Growth**: 3-year CAGR capped at 70%/yr
          *How fast EV adoption is accelerating in this FSA*
        - **Load Increase**: Instantaneous EV charging draw / IESO hourly demand (capped at 35%)
          *Coincidence factor (% of EVs charging simultaneously) x charger kW rate*

        *BEV @ 7.2 kW (L2), PHEV @ 3.3 kW | Peak coincidence: 50% at 9-10 PM*
        """)

    st.markdown("---")
    st.caption("Data: IESO Hourly Consumption by FSA (Dec 2024 - Nov 2025), Ontario Data Catalogue (EV Registrations Q1 2022 - Q4 2025)")


if __name__ == "__main__":
    main()
