#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
from _utils import ROOT, DATA_RAW, DATA_FINAL

# Inputs
panel_csv   = DATA_FINAL / "contracts" / "contracts_panel.csv"
comps_csv   = DATA_FINAL / "contracts" / "composite_scores.csv"
pairs_csv   = DATA_FINAL / "contracts" / "case_control_pairs.csv"
certik_csv  = DATA_RAW   / "contracts" / "audit_metadata_certik.csv"
defis_csv   = DATA_RAW   / "contracts" / "audit_metadata_defisafety.csv"
exploits_csv= DATA_RAW   / "contracts" / "exploits_labeled.csv"

OUT = ROOT / "analysis" / "master_security_dataset.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

# Load
P = pd.read_csv(panel_csv) if panel_csv.exists() else pd.DataFrame()
C = pd.read_csv(comps_csv) if comps_csv.exists() else pd.DataFrame()
E = pd.read_csv(exploits_csv) if exploits_csv.exists() else pd.DataFrame()
A1 = pd.read_csv(certik_csv) if certik_csv.exists() else pd.DataFrame()
A2 = pd.read_csv(defis_csv) if defis_csv.exists() else pd.DataFrame()

if P.empty:
    raise SystemExit("❌ contracts_panel.csv missing. Run build_contract_panel.py first.")

# Collapse to protocol–chain level for outcomes; keep contract-level for counts later if needed
PC = P.groupby(["protocol_name","chain","category"], as_index=False).agg(
    n_contracts=("contract_address","nunique"),
    lines_of_code=("lines_of_code","sum"),
    proxy_any=("proxy","max"),
    delegate_any=("delegatecall","max")
)

# Composites: average or max at protocol–chain
if not C.empty:
    CC = C.groupby(["protocol_name","chain"], as_index=False).agg(
        idx_access_auth=("idx_access_auth","mean"),
        idx_oracle_market=("idx_oracle_market","mean"),
        idx_reentrancy=("idx_reentrancy","mean"),
        idx_dos=("idx_dos","mean"),
        idx_arithmetic=("idx_arithmetic","mean"),
    )
    PC = PC.merge(CC, on=["protocol_name","chain"], how="left")

# Exploits → protocol-level; if multiple dates, take first and max loss
if not E.empty:
    EX = E.groupby(["protocol_name"], as_index=False).agg(
        exploit_date=("date","min"),
        loss_usd=("loss_usd","max"),
        sources=("source", lambda x: ",".join(sorted(set(x.dropna().astype(str)))))
    )
    EX["Exploit"] = 1
    PC = PC.merge(EX[["protocol_name","Exploit","exploit_date","loss_usd","sources"]], on="protocol_name", how="left")
else:
    PC["Exploit"]=0

# Audits
if not A1.empty:
    a1 = A1.rename(columns={"audit_score":"certik_score","audit_date":"certik_date"})
    a1 = a1.groupby("protocol_name", as_index=False).agg(certik_score=("certik_score","max"), certik_date=("certik_date","max"))
    PC = PC.merge(a1, on="protocol_name", how="left")
if not A2.empty:
    a2 = A2.rename(columns={"audit_score":"defisafety_score","audit_date":"defisafety_date"})
    a2 = a2.groupby("protocol_name", as_index=False).agg(defisafety_score=("defisafety_score","max"), defisafety_date=("defisafety_date","max"))
    PC = PC.merge(a2, on="protocol_name", how="left")

PC["AnyAudit"] = ((PC.get("certik_score").notna()) | (PC.get("defisafety_score").notna())).astype(int)

# Save
PC.to_csv(OUT, index=False)
print(f"✅ Master dataset saved → {OUT} (rows={len(PC)})")
print("Columns:", ", ".join(list(PC.columns)))