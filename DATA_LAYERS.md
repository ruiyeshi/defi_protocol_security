# Data Layers & Canonical Files (Gold vs Intermediate)

## Join Keys
- Protocol key: `slug` (DefiLlama)
- Contract key: (`chain`, `address`)
- Panel index: (`slug`, `month`) or (`slug`, `year`)

---

## Exploits (DeFiLlama hacks)
GOLD:
- data_raw/exploits/exploit_events_defillama.csv

INTERMEDIATE / ARCHIVE:
- exploit_events_defillama_bad1970.csv
- defillama_hacks_processed.csv (if not final schema)

NOTES:
- Must have `slug` (or name->slug mapping) to join to protocol panel.

---

## Audits
RAW SOURCES (ARCHIVE, reproducibility):
- audit_events_code4rena.csv
- audit_events_sherlock.csv
- audit_events_github_reports.csv
- audit_events_github_strict.csv
- audit_events_firm_archives.csv

NORMALIZED EVENT TABLES (for event-time analysis):
- audit_events_long.csv
- audit_events_long_plus_firm_archives.csv
- audit_events_firm_archives_enriched.csv
- audit_events_firm_archives_mapped.csv

GOLD (protocol-level aggregate for main panel):
- audit_master_with_slug_defi_only.csv

DIAGNOSTICS:
- unmatched_audit_events.csv
- unmatched_audit_protocols.csv
- manual_name_to_slug.csv

---

## Contracts
PROTOCOL->CONTRACT MAP (bridge table; must contain slug+chain+address):
GOLD (pick one):
- master_contracts_llama_adapters_clean.csv (preferred)

VERIFICATION ENRICHMENT (contract-level):
GOLD (latest):
- verified_contracts_merged_v*.csv (choose latest)

STATIC ANALYSIS OUTPUTS:
- slither_vulnerabilities.csv

CHECKPOINTS:
- state.json

---

## External Benchmark (not joinable into DeFi panel)
- contracts_clean.csv / SC_4label.csv

Use for:
- validating static analysis pipeline (precision/recall signals)
- robustness appendixs