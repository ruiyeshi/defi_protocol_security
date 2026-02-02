import json
import os
from pathlib import Path

import pandas as pd

# -----------------------------
# Config
# -----------------------------
# Allow overriding from shell:
#   SLITHER_OUTDIR=outputs/slither_defi_v8 MIN_FINDINGS=1 python build_slither_dataset.py
#   (noT: do NOT wrap in [] — those brackets break in zsh)
OUTDIR = Path(os.environ.get("SLITHER_OUTDIR", "outputs/slither_defi_v8"))
MIN_FINDINGS = int(os.environ.get("MIN_FINDINGS", "1"))  # your preference: OK contracts with >=1 finding

# Optional enrichment inputs
PROTOCOL_MAP_CSV = os.environ.get("PROTOCOL_MAP_CSV", "").strip()
SECURITY_MASTER_CSV = os.environ.get(
    "SECURITY_MASTER_CSV",
    "data_raw/audits/protocol_security_review_master.csv",
).strip()

PROGRESS_PATH = OUTDIR / "slither_defi_progress.csv"
if not PROGRESS_PATH.exists():
    raise FileNotFoundError(f"Progress file not found: {PROGRESS_PATH}")

progress = pd.read_csv(PROGRESS_PATH, low_memory=False)

# Normalize expected columns
for c in ["chain", "address", "ok", "json_path", "returncode", "err_tail"]:
    if c not in progress.columns:
        # some runs may not have all columns; keep going
        progress[c] = None

# Lowercase chain/address for stable joins
progress["chain"] = progress["chain"].astype(str).str.lower()
progress["address"] = progress["address"].astype(str).str.lower()

# Only OK rows with a JSON path
ok_rows = progress.loc[progress["ok"].fillna(0).astype(int) == 1].copy()
ok_rows = ok_rows.loc[ok_rows["json_path"].fillna("").astype(str).str.len() > 0].copy()

# Filter to JSONs that actually exist and are non-empty
ok_rows["json_path"] = ok_rows["json_path"].astype(str)
exists_mask = ok_rows["json_path"].apply(lambda p: os.path.exists(p) and os.path.getsize(p) > 0)
ok_rows = ok_rows.loc[exists_mask].copy()

# -----------------------------
# Helpers
# -----------------------------

def safe_load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return None


def extract_detectors(j: dict):
    """Return list of detector dicts from a Slither JSON-like dict."""
    if not isinstance(j, dict):
        return []
    results = j.get("results")
    if isinstance(results, dict):
        dets = results.get("detectors")
        if isinstance(dets, list):
            return dets
    return []


# -----------------------------
# Parse
# -----------------------------
rows = []
contract_rows = []

# Debug counters (to explain 'ok but not parsed')
skip_counts = {
    "missing_json_path": 0,
    "json_missing_on_disk": 0,
    "json_empty": 0,
    "json_load_failed": 0,
    "no_detectors_array": 0,
}

parsed_keys = set()

for _, r in ok_rows.iterrows():
    chain = str(r.get("chain", "")).lower()
    address = str(r.get("address", "")).lower()
    jp = r.get("json_path", "")

    if not isinstance(jp, str) or not jp:
        skip_counts["missing_json_path"] += 1
        continue

    p = Path(jp)
    if not p.exists():
        skip_counts["json_missing_on_disk"] += 1
        continue
    if p.stat().st_size == 0:
        skip_counts["json_empty"] += 1
        continue

    j = safe_load_json(p)
    if not isinstance(j, dict):
        skip_counts["json_load_failed"] += 1
        continue

    dets = extract_detectors(j)
    if not isinstance(dets, list):
        dets = []

    # Mark as parsed as soon as JSON is readable (even if 0 detectors)
    parsed_keys.add(f"{chain}|{address}")

    # Contract-level summary
    contract_rows.append(
        {
            "chain": chain,
            "address": address,
            "json_path": str(p),
            "pragma": r.get("pragma", None),
            "solc_picked": r.get("solc_picked", None),
            "returncode": r.get("returncode", None),
            "n_findings": int(len(dets)),
        }
    )

    if len(dets) == 0:
        # keep the contract row, but nothing to add to findings
        skip_counts["no_detectors_array"] += 1
        continue

    # One row per detector hit
    for d in dets:
        if not isinstance(d, dict):
            continue
        rows.append(
            {
                "chain": chain,
                "address": address,
                "check": d.get("check", ""),
                "impact": d.get("impact", ""),
                "confidence": d.get("confidence", ""),
                "description": (d.get("description") or "")[:500],
                "markdown": (d.get("markdown") or "")[:500],
            }
        )

findings = pd.DataFrame(rows)
contracts_all_ok = pd.DataFrame(contract_rows)

# Stable unique contract key (chain|address) for cross-chain uniqueness
if len(contracts_all_ok) > 0 and "contract_id" not in contracts_all_ok.columns:
    contracts_all_ok["contract_id"] = (
        contracts_all_ok["chain"].astype(str) + "|" + contracts_all_ok["address"].astype(str)
    )

# -----------------------------
# Contract-level features (for publishable tables)
# -----------------------------
if len(contracts_all_ok) == 0:
    contract_level = contracts_all_ok.copy()
    contract_level["has_findings"] = False
    contract_level["impact_High"] = 0
    contract_level["impact_Medium"] = 0
    contract_level["has_high"] = False
    contract_level["has_high_or_medium"] = False
else:
    # Start from all OK contracts (keep zeros!)
    contract_level = contracts_all_ok.copy()
    if len(contract_level) > 0 and "contract_id" not in contract_level.columns:
        contract_level["contract_id"] = (
            contract_level["chain"].astype(str) + "|" + contract_level["address"].astype(str)
        )
    contract_level["has_findings"] = contract_level["n_findings"].fillna(0).astype(int) > 0

    if len(findings) > 0:
        # impact counts per contract
        pivot_impact = (
            findings.pivot_table(
                index=["chain", "address"],
                columns="impact",
                values="check",
                aggfunc="size",
                fill_value=0,
            )
            .reset_index()
        )
        # Normalize column names like impact_High, impact_Medium, ...
        for col in list(pivot_impact.columns):
            if col in ("chain", "address"):
                continue
            pivot_impact.rename(columns={col: f"impact_{col}"}, inplace=True)

        pivot_conf = (
            findings.pivot_table(
                index=["chain", "address"],
                columns="confidence",
                values="check",
                aggfunc="size",
                fill_value=0,
            )
            .reset_index()
        )
        for col in list(pivot_conf.columns):
            if col in ("chain", "address"):
                continue
            pivot_conf.rename(columns={col: f"conf_{col}"}, inplace=True)

        contract_level = contract_level.merge(pivot_impact, on=["chain", "address"], how="left")
        contract_level = contract_level.merge(pivot_conf, on=["chain", "address"], how="left")

    # Fill missing impact/conf columns with 0
    for c in ["impact_High", "impact_Medium"]:
        if c not in contract_level.columns:
            contract_level[c] = 0
        contract_level[c] = contract_level[c].fillna(0).astype(int)

    contract_level["has_high"] = contract_level["impact_High"].fillna(0).astype(int) > 0
    contract_level["has_high_or_medium"] = (
        contract_level["impact_High"].fillna(0).astype(int)
        + contract_level["impact_Medium"].fillna(0).astype(int)
    ) > 0

# Preferred analysis subset (if you want to drop 0-finding contracts)
if len(contract_level) == 0:
    contracts = contract_level.copy()
else:
    contracts = contract_level.loc[contract_level["n_findings"].fillna(0).astype(int) >= MIN_FINDINGS].copy()

# Ensure contract_id exists in contracts
if len(contracts) > 0 and "contract_id" not in contracts.columns:
    contracts["contract_id"] = contracts["chain"].astype(str) + "|" + contracts["address"].astype(str)

# -----------------------------
# Aggregations
# -----------------------------

def _vc(series: pd.Series, name: str):
    out = series.value_counts().reset_index()
    out.columns = [name, "count"]
    return out

OUT = OUTDIR / "dataset"
OUT.mkdir(parents=True, exist_ok=True)

# Always write raw tables
findings.to_csv(OUT / "slither_findings.csv", index=False)
contracts_all_ok.to_csv(OUT / "slither_contracts_all_ok.csv", index=False)
contract_level.to_csv(OUT / "contract_level_features.csv", index=False)
contracts.to_csv(OUT / "slither_contracts.csv", index=False)

# -----------------------------
# Optional enrichment: protocol mapping + security master
# -----------------------------

def _norm_chain_addr(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "chain" in df.columns:
        df["chain"] = df["chain"].astype(str).str.lower().str.strip()
    if "address" in df.columns:
        df["address"] = df["address"].astype(str).str.lower().str.strip()
    return df


def _clean_slug(s):
    if s is None:
        return None
    s = str(s).strip().lower()
    if s in ("", "nan", "none", "unknown", "unmapped"):
        return None
    return s


# Enrich contract-level tables with protocol_slug_final / has_slug_final
protocol_map_path = Path(PROTOCOL_MAP_CSV) if PROTOCOL_MAP_CSV else None
sec_master_path = Path(SECURITY_MASTER_CSV) if SECURITY_MASTER_CSV else None

if protocol_map_path and protocol_map_path.exists():
    pm = pd.read_csv(protocol_map_path, low_memory=False)
    pm = _norm_chain_addr(pm)

    # Flexible slug construction across mapping versions
    cand_cols = [
        "protocol_slug_final",
        "slug_from_canonical",
        "canonical_slug",
        "slug_from_llama",
        "llama_slug",
        "protocol_id_final",
        "protocol_slug",
        "slug",
        "protocol_id",
    ]

    use_col = next((c for c in cand_cols if c in pm.columns), None)
    if use_col is None:
        raise ValueError(
            "PROTOCOL_MAP_CSV has no usable slug column. Expected one of: "
            + ", ".join(cand_cols)
            + f". Got columns: {pm.columns.tolist()}"
        )

    # Priority: canonical > llama > precomputed > legacy
    pm["protocol_slug_final"] = None
    if "slug_from_canonical" in pm.columns:
        pm["protocol_slug_final"] = pm["slug_from_canonical"].map(_clean_slug)
    if "canonical_slug" in pm.columns:
        pm.loc[pm["protocol_slug_final"].isna(), "protocol_slug_final"] = pm["canonical_slug"].map(_clean_slug)
    if "slug_from_llama" in pm.columns:
        pm.loc[pm["protocol_slug_final"].isna(), "protocol_slug_final"] = pm["slug_from_llama"].map(_clean_slug)
    if "llama_slug" in pm.columns:
        pm.loc[pm["protocol_slug_final"].isna(), "protocol_slug_final"] = pm["llama_slug"].map(_clean_slug)

    # fallback to whatever usable column exists
    pm.loc[pm["protocol_slug_final"].isna(), "protocol_slug_final"] = pm[use_col].map(_clean_slug)

    pm["has_slug_final"] = pm["protocol_slug_final"].notna()

    # Keep optional label columns if present
    keep_cols = ["chain", "address", "protocol_slug_final", "has_slug_final"]
    for opt in ["protocol", "protocol_name", "category", "tvl", "chains", "slug"]:
        if opt in pm.columns and opt not in keep_cols:
            keep_cols.append(opt)

    pm_small = pm[keep_cols].drop_duplicates(["chain", "address"])

    contract_level_plus = _norm_chain_addr(contract_level).merge(
        pm_small, on=["chain", "address"], how="left"
    )
    contracts_plus = _norm_chain_addr(contracts).merge(
        pm_small, on=["chain", "address"], how="left"
    )
    contracts_all_ok_plus = _norm_chain_addr(contracts_all_ok).merge(
        pm_small, on=["chain", "address"], how="left"
    )

    # Ensure contract_id exists after merges
    for _df in [contract_level_plus, contracts_plus, contracts_all_ok_plus]:
        if len(_df) > 0 and "contract_id" not in _df.columns:
            _df["contract_id"] = _df["chain"].astype(str) + "|" + _df["address"].astype(str)

    # Fill unmapped bucket for convenience
    for df_ in [contract_level_plus, contracts_plus, contracts_all_ok_plus]:
        if "protocol_slug_final" in df_.columns:
            df_["protocol_slug_final"] = df_["protocol_slug_final"].fillna("unmapped")
        if "has_slug_final" in df_.columns:
            df_["has_slug_final"] = df_["has_slug_final"].fillna(False).astype(bool)

    contract_level_plus.to_csv(OUT / "contract_level_features_plus_protocol.csv", index=False)
    contracts_plus.to_csv(OUT / "slither_contracts_plus_protocol.csv", index=False)
    contracts_all_ok_plus.to_csv(OUT / "slither_contracts_all_ok_plus_protocol.csv", index=False)

    print("Enrichment: wrote *_plus_protocol.csv tables")

    # Add security master flags + merge metadata (only if file exists)
    if sec_master_path and sec_master_path.exists():
        sec = pd.read_csv(sec_master_path, low_memory=False)
        if "slug" not in sec.columns:
            raise ValueError(
                f"SECURITY_MASTER_CSV must contain a 'slug' column. Got columns: {sec.columns.tolist()}"
            )

        sec = sec.copy()
        sec["slug"] = sec["slug"].astype(str).str.strip().str.lower()
        sec_slugs = set(sec["slug"].dropna())

        for df_ in [contract_level_plus, contracts_plus, contracts_all_ok_plus]:
            df_["in_security_master"] = df_["protocol_slug_final"].isin(sec_slugs)

        # Merge security metadata onto contract-level tables
        contract_level_plus_sec = contract_level_plus.merge(
            sec, left_on="protocol_slug_final", right_on="slug", how="left"
        )
        contracts_plus_sec = contracts_plus.merge(
            sec, left_on="protocol_slug_final", right_on="slug", how="left"
        )

        contract_level_plus_sec.to_csv(
            OUT / "contract_level_features_plus_protocol_security.csv", index=False
        )
        contracts_plus_sec.to_csv(
            OUT / "slither_contracts_plus_protocol_security.csv", index=False
        )

        # Protocol-level aggregation + security merge
        prot = (
            contract_level_plus.groupby("protocol_slug_final", dropna=False)
            .agg(
                n_contracts_ok=("contract_id", "nunique") if "contract_id" in contract_level_plus.columns else ("address", "nunique"),
                avg_findings_per_contract=("n_findings", "mean"),
                share_has_high_or_medium=("has_high_or_medium", "mean"),
                share_in_security_master=("in_security_master", "mean"),
                any_in_security_master=("in_security_master", "max"),
                has_slug_final=("has_slug_final", "max"),
            )
            .reset_index()
        )

        prot = prot.merge(sec, left_on="protocol_slug_final", right_on="slug", how="left")
        prot.to_csv(OUT / "protocol_level_features_plus_security.csv", index=False)

        print("protocols:", prot["protocol_slug_final"].nunique())
        print("protocols in security master:", int(prot["any_in_security_master"].sum()))
        print("Enrichment: wrote *_plus_protocol_security.csv and protocol_level_features_plus_security.csv")


else:
    # Only print a soft hint; don't fail existing pipelines.
    if PROTOCOL_MAP_CSV:
        print(f"PROTOCOL_MAP_CSV set but file not found: {PROTOCOL_MAP_CSV}")
    else:
        print("PROTOCOL_MAP_CSV not set; skipping protocol/security enrichment")

# Starter RQ tables (only if findings non-empty)
if len(findings) > 0:
    top_detectors = _vc(findings["check"], "check").head(30)
    impact_dist = _vc(findings["impact"], "impact")

    # Findings by chain (counts) + contract-level coverage per chain
    by_chain_findings = findings.groupby(["chain"]).size().sort_values(ascending=False).reset_index(name="findings")

    # Per-chain contract stats
    by_chain_contracts = (
        contract_level.groupby(["chain"]).agg(
            n_contracts_ok=("contract_id", "nunique") if "contract_id" in contract_level.columns else ("address", "count"),
            avg_findings_per_contract=("n_findings", "mean"),
            share_has_high_or_medium=("has_high_or_medium", "mean"),
        ).reset_index()
    )

    # High/Medium-only detector ranking
    sec = findings.loc[findings["impact"].isin(["High", "Medium"])].copy()
    top_detectors_high_medium = _vc(sec["check"], "check").head(50) if len(sec) > 0 else pd.DataFrame(columns=["check", "count"])

    # Detector mix: for each detector, what share of its findings are High/Medium?
    det_tot = findings.groupby(["check"]).size().reset_index(name="n_total")
    det_hm = sec.groupby(["check"]).size().reset_index(name="n_high_medium") if len(sec) > 0 else pd.DataFrame({"check": [], "n_high_medium": []})
    det_mix = det_tot.merge(det_hm, on="check", how="left")
    det_mix["n_high_medium"] = det_mix["n_high_medium"].fillna(0).astype(int)
    det_mix["p_high_medium"] = det_mix["n_high_medium"] / det_mix["n_total"].clip(lower=1)
    det_mix = det_mix.sort_values(["p_high_medium", "n_total"], ascending=[False, False])

    top_detectors.to_csv(OUT / "top_detectors.csv", index=False)
    impact_dist.to_csv(OUT / "impact_distribution.csv", index=False)
    by_chain_findings.to_csv(OUT / "findings_by_chain.csv", index=False)
    by_chain_contracts.to_csv(OUT / "contracts_by_chain.csv", index=False)
    top_detectors_high_medium.to_csv(OUT / "top_detectors_high_medium.csv", index=False)
    det_mix.to_csv(OUT / "detector_high_medium_share.csv", index=False)

# -----------------------------
# Diagnostics: OK but not parsed
# -----------------------------
# OK contracts that have a JSON path, but we did not manage to parse (readable) JSON for them
ok_rows_keys = (ok_rows["chain"].astype(str).str.lower() + "|" + ok_rows["address"].astype(str).str.lower())
missing_mask = ~ok_rows_keys.isin(parsed_keys)
missing = ok_rows.loc[missing_mask, ["chain", "address", "json_path", "returncode", "err_tail"]].copy()
missing.to_csv(OUT / "ok_but_not_parsed.csv", index=False)

# -----------------------------
# Print summary
# -----------------------------
print("OUTDIR:", OUTDIR)
print("Progress rows:", len(progress))
print("OK rows w/ existing JSON:", len(ok_rows))
print("Parsed OK JSONs:", len(contracts_all_ok))
print(f"Contracts (>= {MIN_FINDINGS} findings):", len(contracts))
print("Findings rows:", len(findings))
if len(contract_level) > 0:
    print("share_has_high_or_medium:", round(float(contract_level["has_high_or_medium"].mean()), 4))
print("Wrote:", OUT)
if protocol_map_path and protocol_map_path.exists():
    print("PROTOCOL_MAP_CSV:", str(protocol_map_path))
if sec_master_path and sec_master_path.exists():
    print("SECURITY_MASTER_CSV:", str(sec_master_path))
print("ok_but_not_parsed rows:", len(missing))
print("Skip counters:")
for k, v in skip_counts.items():
    print(f"  - {k}: {v}")