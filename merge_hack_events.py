"""
merge_hack_events.py
====================

Merge DeFiLlama hack dataset (CSV) with extra scraped hacks (JSON) into a single CSV.

Inputs (expected in current directory):
  - defillama_hacks_processed.csv
  - hacks_extra.json

Output:
  - merged_hack_events.csv
"""

from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple


DEFILLAMA_CSV = Path("defillama_hacks_processed.csv")
EXTRA_JSON = Path("hacks_extra.json")
OUT_CSV = Path("merged_hack_events.csv")


def norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    # remove punctuation/spaces
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def parse_defillama_date(s: str) -> Optional[str]:
    """
    DefiLlama exploit_date example: '3/22/24' (M/D/YY) or sometimes '03/22/2024'
    Return ISO 'YYYY-MM-DD' or None.
    """
    if not s:
        return None
    s = s.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y", "%-m/%-d/%y", "%-m/%-d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().isoformat()
        except Exception:
            continue
    # last resort: try letting datetime parse a few common variants
    try:
        dt = datetime.fromisoformat(s)
        return dt.date().isoformat()
    except Exception:
        return None


def parse_extra_date(s: str) -> Optional[str]:
    """
    Extra JSON date is already ISO most of the time.
    """
    if not s:
        return None
    s = str(s).strip()
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.date().isoformat()
    except Exception:
        return None


def load_defillama_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with DEFILLAMA_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Skip completely empty rows
            if not any((v or "").strip() for v in r.values()):
                continue
            rows.append(r)
    return rows


def load_extra_events() -> List[Dict[str, Any]]:
    return json.loads(EXTRA_JSON.read_text(encoding="utf-8"))


def main() -> None:
    if not DEFILLAMA_CSV.exists() or not EXTRA_JSON.exists():
        raise SystemExit("Both defillama_hacks_processed.csv and hacks_extra.json must exist in the current directory.")

    defi_rows = load_defillama_rows()
    extra_events = load_extra_events()

    print("Loaded DefiLlama rows:", len(defi_rows))
    print("Loaded extra events:", len(extra_events))

    if not defi_rows:
        raise SystemExit("DefiLlama CSV loaded 0 rows. Check file content/headers.")
    if not extra_events:
        print("Warning: hacks_extra.json is empty. Proceeding with DefiLlama only.")

    print("DefiLlama columns:", list(defi_rows[0].keys()))
    print("Extra keys:", list(extra_events[0].keys()) if extra_events else [])

    # Build lookup for extra events by (name,date)
    extra_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for e in extra_events:
        name = norm_name(e.get("protocol", ""))
        date = parse_extra_date(e.get("date", ""))
        if not name or not date:
            continue
        extra_map[(name, date)] = e

    merged: List[Dict[str, Any]] = []
    matched = 0

    for r in defi_rows:
        name_raw = r.get("name", "") or ""
        date_raw = r.get("exploit_date", "") or ""

        key_name = norm_name(name_raw)
        key_date = parse_defillama_date(date_raw)

        out = dict(r)  # start with DefiLlama columns

        # Attach extra fields if matched
        if key_name and key_date and (key_name, key_date) in extra_map:
            e = extra_map[(key_name, key_date)]
            matched += 1
            out["extra_source"] = e.get("source", "")
            out["extra_attack_method"] = e.get("attack_method", "")
            out["extra_loss_usd"] = e.get("loss_usd", "")
            out["extra_protocol"] = e.get("protocol", "")
            out["extra_date"] = e.get("date", "")
        else:
            out["extra_source"] = ""
            out["extra_attack_method"] = ""
            out["extra_loss_usd"] = ""
            out["extra_protocol"] = ""
            out["extra_date"] = ""

        merged.append(out)

    print("Matched rows:", matched)
    print("Writing:", OUT_CSV)

    # Always write output; never crash on empty
    fieldnames = list(merged[0].keys()) if merged else []
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        if not fieldnames:
            f.write("")
        else:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(merged)

    print("Done.")


if __name__ == "__main__":
    main()
