import pandas as pd
import numpy as np

def sample_controls(df, n_controls=2, tvl_tol=0.2):
    positives = df[df.exploit_flag == 1]
    controls = []
    for _, row in positives.iterrows():
        subset = df[
            (df["category"] == row["category"]) &
            (abs(df["tvl_30d_avg_pre"] - row["tvl_30d_avg_pre"]) < tvl_tol * row["tvl_30d_avg_pre"]) &
            (df["exploit_flag"] == 0)
        ]
        if len(subset) > 0:
            controls.append(subset.sample(min(n_controls, len(subset))))
    if len(controls) == 0:
        return pd.DataFrame()
    return pd.concat(controls, ignore_index=True)

if __name__ == "__main__":
    df = pd.read_csv("outputs/panel_contract_level_with_exploits.csv")
    controls = sample_controls(df, n_controls=2)
    combined = pd.concat([df[df.exploit_flag == 1], controls])
    combined.to_csv("outputs/panel_balanced_controls.csv", index=False)
    print(f"✅ Sampled {len(controls)} controls for {df.exploit_flag.sum()} exploits")