import os
import pandas as pd
import numpy as np

PANEL_PATH = "data_clean/panel_protocol_year.csv"
M1_PATH = "data_clean/m1_exploit_audit.csv"
OUT_PATH = "analysis/targets_verified.csv"
OUT_SAMPLE_PATH = "analysis/targets_verified_sampled.csv"

def tvl_bin(s: pd.Series, q=4):
    # quantile bins; fallback if many zeros
    try:
        return pd.qcut(s, q=q, labels=[f"Q{i+1}" for i in range(q)])
    except Exception:
        return pd.cut(s, bins=[-1, 0, 1e6, 1e7, 1e9, np.inf],
                      labels=["0", "0-1m", "1-10m", "10m-1b", "1b+"], include_lowest=True)


def stratified_sample(df: pd.DataFrame, n_per_stratum: int = 5, random_state: int = 42) -> pd.DataFrame:
    """Sample up to n_per_stratum rows per stratum for tool-based analysis."""
    if "stratum" not in df.columns:
        raise ValueError("Expected column 'stratum' for stratified sampling")

    def _sample(group: pd.DataFrame) -> pd.DataFrame:
        n = min(len(group), n_per_stratum)
        return group.sample(n=n, random_state=random_state)

    return df.groupby("stratum", group_keys=False).apply(_sample).reset_index(drop=True)

def main():
    panel = pd.read_csv(PANEL_PATH, low_memory=False)

    # protocol-level aggregates from panel
    g = panel.groupby("slug", dropna=False).agg(
        name=("name", "first"),
        category=("category_llama", "first"),
        chains=("chains_llama", "first"),
        max_tvl=("tvl", "max"),
        ever_exploited=("exploited_this_year", "max"),
        ever_audited=("has_audit", "max"),
        any_top_firm=("any_top_firm", "max"),
        audit_firm_count=("audit_firm_count", "max"),
    ).reset_index()

    g["max_tvl"] = pd.to_numeric(g["max_tvl"], errors="coerce").fillna(0.0)
    g["tvl_bin"] = tvl_bin(g["max_tvl"])
    g["chain_main"] = g["chains"].astype(str).str.split(";").str[0].replace("nan", np.nan)

    # optional: whether exploited event exists in m1
    m1 = pd.read_csv(M1_PATH, low_memory=False)
    if "slug" in m1.columns:
        m1 = m1[m1["slug"].notna()]
        exploited_slugs = set(m1["slug"].unique().tolist())
        g["is_exploited_eventlevel"] = g["slug"].isin(exploited_slugs).astype(int)
    else:
        g["is_exploited_eventlevel"] = g["ever_exploited"]

    # placeholders for contract extraction stage
    g["contract_address"] = ""
    g["compiler_version"] = ""
    g["verified_source_found"] = 0  # will be updated later

    # Stratified sampling suggestion columns
    g["stratum"] = (
        "exploited=" + g["ever_exploited"].astype(int).astype(str) +
        "|audited=" + g["ever_audited"].astype(int).astype(str) +
        "|tvl=" + g["tvl_bin"].astype(str) +
        "|chain=" + g["chain_main"].astype(str)
    )

    # Keep only DeFiLlama-mapped protocols (slug notna) and a few key cols
    out = g[g["slug"].notna()].copy()
    out = out[[
        "slug", "name", "category", "chain_main", "chains",
        "max_tvl", "tvl_bin",
        "ever_exploited", "is_exploited_eventlevel",
        "ever_audited", "any_top_firm", "audit_firm_count",
        "contract_address", "compiler_version", "verified_source_found",
        "stratum"
    ]]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print("Saved:", OUT_PATH, "rows=", len(out))

    sampled = stratified_sample(out, n_per_stratum=5, random_state=42)
    sampled.to_csv(OUT_SAMPLE_PATH, index=False)
    print("Saved:", OUT_SAMPLE_PATH, "rows=", len(sampled), "(n_per_stratum=5)")

if __name__ == "__main__":
    main()