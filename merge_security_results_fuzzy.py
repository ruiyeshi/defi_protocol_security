import pandas as pd
from fuzzywuzzy import process, fuzz

# Load audit + vulnerability datasets
audit = pd.read_csv("data_raw/contracts/audit_metadata_full.csv")
vuln = pd.read_csv("data_raw/contracts/slither_vulnerabilities.csv")

# Normalize protocol names
audit["protocol_name"] = audit["protocol_name"].astype(str).str.lower().str.strip()
vuln["protocol_name"] = vuln["protocol_name"].astype(str).str.lower().str.strip()

# Fuzzy-match audit names to vulnerability names
matches = []
for name in audit["protocol_name"]:
    match_result = process.extractOne(name, vuln["protocol_name"], scorer=fuzz.token_sort_ratio)
    if match_result:
        best_match, score = match_result[0], match_result[1]
        if score >= 80:  # Match confidence threshold
            row = vuln[vuln["protocol_name"] == best_match].iloc[0].to_dict()
            row["matched_audit_name"] = name
            row["match_score"] = score
            matches.append(row)

vuln_matched = pd.DataFrame(matches)

# Merge based on matched names
merged = pd.merge(
    audit,
    vuln_matched,
    left_on="protocol_name",
    right_on="matched_audit_name",
    how="outer"
)

# Save output
out_path = "data_raw/contracts/defi_security_master_fuzzy.csv"
merged.to_csv(out_path, index=False)
print(f"✅ Fuzzy-matched master dataset saved → {out_path} ({len(merged)} rows)")
