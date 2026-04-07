"""
Pre-aggregate IESO hourly FSA demand data (Dec 2024 - Nov 2025)
from ~740MB of raw CSVs into a compact parquet file.

Reads 12 monthly CSV files, filters to valid 3-char Ontario FSAs,
and produces per-FSA, per-hour averages across the full year.
"""

import pandas as pd
import glob
import os
import re

DATA_DIR = os.path.expanduser("~/Desktop/IESO_hourly_FSA_demand")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ieso_hourly_by_fsa.parquet")

FSA_PATTERN = re.compile(r"^[A-Z]\d[A-Z]$")


def load_all_ieso_files():
    """Load and concatenate all monthly IESO CSV files."""
    csv_files = sorted(glob.glob(os.path.join(DATA_DIR, "PUB_HourlyConsumptionByFSA_*.csv")))
    print(f"Found {len(csv_files)} IESO CSV files")

    frames = []
    for f in csv_files:
        month_label = os.path.basename(f).split("_")[1].replace("ConsumptionByFSA", "")
        print(f"  Reading {os.path.basename(f)} ...", end=" ")
        df = pd.read_csv(f, skiprows=3, dtype={"FSA": str, "HOUR": int})
        # Keep only valid 3-char Ontario FSAs
        df = df[df["FSA"].str.match(FSA_PATTERN, na=False)]
        print(f"{len(df):,} rows (3-char FSAs)")
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    print(f"\nTotal rows (3-char FSAs): {len(combined):,}")
    print(f"Distinct FSAs: {combined['FSA'].nunique()}")
    print(f"Date range: {combined['DATE'].min()} to {combined['DATE'].max()}")
    return combined


def aggregate_hourly(df):
    """
    For each FSA + HOUR, compute:
    - AVG_DAILY_KWH: average total kWh for that hour across all days in the year
    - TOTAL_ANNUAL_KWH: sum of all kWh for that FSA (same for all 24 hours)
    - PREMISE_COUNT: max premise count observed (stable proxy for size)
    """
    # Sum across customer types and price plans for each FSA/DATE/HOUR
    daily = (
        df.groupby(["FSA", "DATE", "HOUR"], as_index=False)
        .agg({"TOTAL_CONSUMPTION": "sum", "PREMISE_COUNT": "sum"})
    )

    # Count distinct days per FSA to get proper average
    day_counts = daily.groupby("FSA")["DATE"].nunique().reset_index()
    day_counts.columns = ["FSA", "NUM_DAYS"]

    # Average daily kWh per hour
    hourly_avg = (
        daily.groupby(["FSA", "HOUR"], as_index=False)
        .agg({"TOTAL_CONSUMPTION": "sum", "PREMISE_COUNT": "max"})
    )
    hourly_avg = hourly_avg.merge(day_counts, on="FSA")
    hourly_avg["AVG_DAILY_KWH"] = hourly_avg["TOTAL_CONSUMPTION"] / hourly_avg["NUM_DAYS"]

    # Annual total per FSA
    annual = daily.groupby("FSA")["TOTAL_CONSUMPTION"].sum().reset_index()
    annual.columns = ["FSA", "TOTAL_ANNUAL_KWH"]

    # Merge
    result = hourly_avg.merge(annual, on="FSA")
    result = result[["FSA", "HOUR", "AVG_DAILY_KWH", "TOTAL_ANNUAL_KWH", "PREMISE_COUNT"]]
    result = result.sort_values(["FSA", "HOUR"]).reset_index(drop=True)

    print(f"\nAggregated: {len(result):,} rows ({result['FSA'].nunique()} FSAs x 24 hours)")
    return result


def main():
    df = load_all_ieso_files()
    result = aggregate_hourly(df)

    result.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nSaved to {OUTPUT_FILE}")
    print(f"File size: {os.path.getsize(OUTPUT_FILE) / 1024:.0f} KB")

    # Print a sample
    sample_fsa = "M5V"
    sample = result[result["FSA"] == sample_fsa]
    if not sample.empty:
        print(f"\nSample: {sample_fsa} (downtown Toronto)")
        print(sample.to_string(index=False))


if __name__ == "__main__":
    main()
