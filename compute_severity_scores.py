import pandas as pd
from pathlib import Path

INPUT = Path("outputs/exploit_labels_merged.csv")
OUTPUT = Path("outputs/severity_scored.csv")

if not INPUT.exists():
    raise SystemExit(f"Missing input file: {INPUT}")

df = pd.read_csv(INPUT)

weights = {"impact": 0.4, "practicality": 0.3, "awareness": 0.2, "resolvability": 0.1}
for col in weights:
    if col not in df.columns:
        df[col] = 0.5

df["severity_weighted_score"] = sum(df[col] * w for col, w in weights.items())
df.to_csv(OUTPUT, index=False)
print(f"✅ Saved severity-weighted dataset → {OUTPUT} ({len(df)} rows)")