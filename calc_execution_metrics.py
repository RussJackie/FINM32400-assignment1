#!/usr/bin/env python3
import argparse
import pandas as pd
from datetime import datetime

# Expected input columns (from fix_to_csv.py):
# OrderID,OrderTransactTime,ExecutionTransactTime,Symbol,Side,OrderQty,LimitPrice,AvgPx,LastMkt

TIME_FORMATS = [
    "%Y%m%d-%H:%M:%S.%f",  # e.g. 20250910-08:00:00.377
    "%Y%m%d-%H:%M:%S",     # fallback if no fractional seconds
]

def parse_fix_time(s: str):
    """Parse FIX-style timestamps with a couple of common formats."""
    if pd.isna(s):
        return pd.NaT
    s = str(s).strip()
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    # If nothing worked, return NaT so we can drop the bad row
    return pd.NaT

def main():
    ap = argparse.ArgumentParser(description="Compute per-exchange execution metrics from CSV.")
    ap.add_argument("--input_csv_file", required=True, help="Input CSV from fix_to_csv.py")
    ap.add_argument("--output_metrics_file", required=True, help="Output CSV with aggregated metrics")
    args = ap.parse_args()

    # Read
    df = pd.read_csv(args.input_csv_file)

    # Basic sanity: keep only rows with the required fields
    required_cols = [
        "OrderTransactTime", "ExecutionTransactTime",
        "LimitPrice", "AvgPx", "LastMkt"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input CSV: {missing}")

    # Parse timestamps -> datetime
    df["OrderDT"] = df["OrderTransactTime"].apply(parse_fix_time)
    df["ExecDT"] = df["ExecutionTransactTime"].apply(parse_fix_time)

    # Drop rows with bad/missing times or LastMkt
    df = df.dropna(subset=["OrderDT", "ExecDT", "LastMkt"])

    # Compute execution speed in seconds (non-negative guard)
    exec_secs = (df["ExecDT"] - df["OrderDT"]).dt.total_seconds()
    exec_secs = exec_secs.clip(lower=0)
    df["ExecSpeedSecs"] = exec_secs

    # Price improvement per fill: max(LimitPrice - AvgPx, 0)
    # (ensure numeric; coerce errors to NaN then drop)
    df["LimitPrice"] = pd.to_numeric(df["LimitPrice"], errors="coerce")
    df["AvgPx"] = pd.to_numeric(df["AvgPx"], errors="coerce")
    df = df.dropna(subset=["LimitPrice", "AvgPx"])

    df["PriceImprovement"] = (df["LimitPrice"] - df["AvgPx"]).clip(lower=0)

    # Group by LastMkt and take means
    agg = (
        df.groupby("LastMkt", dropna=True)
          .agg(
              AvgPriceImprovement=("PriceImprovement", "mean"),
              AvgExecSpeedSecs=("ExecSpeedSecs", "mean"),
          )
          .reset_index()
    )

    # Order columns exactly as required
    out = agg[["LastMkt", "AvgPriceImprovement", "AvgExecSpeedSecs"]]

    # Write
    out.to_csv(args.output_metrics_file, index=False)

if __name__ == "__main__":
    main()
