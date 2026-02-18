#!/usr/bin/env python3
"""
Save merged dataframe(s) to data_clean/merged.

Usage:
  python scripts/save_merged.py --in data_clean/tmp/m1.csv --out data_clean/merged/m1.csv

Notes:
- This script expects the input file to be a CSV that you already produced (e.g., m1).
- It creates the output folder if missing.
"""

import argparse
import os
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_path", required=True, help="Input CSV path (e.g., m1.csv)")
    parser.add_argument("--out", dest="out_path", required=True, help="Output CSV path")
    args = parser.parse_args()

    in_path = args.in_path
    out_path = args.out_path

    if not os.path.exists(in_path):
        raise FileNotFoundError(f"Input not found: {in_path}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    df = pd.read_csv(in_path)
    df.to_csv(out_path, index=False)

    print("Saved:", out_path)
    print("Rows:", len(df), "| Cols:", df.shape[1])


if __name__ == "__main__":
    main()