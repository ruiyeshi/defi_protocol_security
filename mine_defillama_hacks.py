#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mine_defillama_hacks.py

Fetch DefiLlama hacks and output:
  data_raw/exploits/exploit_events_defillama.csv

NO GARBAGE:
  - require protocol_name_raw (fallback to name/title fields)
  - require evidence_url if available (still write row if missing, but notes will track it)

Note:
  DefiLlama's exact fields can evolve. This script is defensive.
"""

from __future__ import annotations

from pathlib import Path
import os
import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

OUT = Path("data_raw/exploits/exploit_events_defillama.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

API_CANDIDATES = [
    "https://api.llama.fi/hacks",
    "https://api.llama.fi/hack",
    "https://api.llama.fi/hacks/list",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
}

COLS = [
    "protocol_name_raw",
    "source",
    "exploit_date",
    "loss_usd",
    "chain",
    "exploit_type",
    "evidence_url",
    "notes",
]

def _get_json(url: str, timeout: int = 25) -> Any:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _parse_loss(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).replace(",", "").strip()
    m = re.match(r"^\$?\s*([0-9]*\.?[0-9]+)\s*([kKmMbB])?$", s)
    if not m:
        return None
    v = float(m.group(1))
    suf = (m.group(2) or "").lower()
    mult = {"k": 1e3, "m": 1e6, "b": 1e9}.get(suf, 1.0)
    return v * mult

def _coerce_list(obj: Any) -> List[Dict]:
    # DefiLlama might return list directly or wrap it
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for key in ["hacks", "data", "list", "items", "results"]:
            if isinstance(obj.get(key), list):
                return [x for x in obj[key] if isinstance(x, dict)]
    return []

def main():
    timeout = int(os.getenv("LLAMA_TIMEOUT", "25"))
    sleep_sec = float(os.getenv("LLAMA_SLEEP_SEC", "0.2"))
    last_err = None
    data = None
    used_url = None

    for url in API_CANDIDATES:
        try:
            data = _get_json(url, timeout=timeout)
            used_url = url
            break
        except Exception as e:
            last_err = e

    if data is None:
        raise SystemExit(f"DefiLlama: all endpoints failed. Last error: {last_err}")

    items = _coerce_list(data)
    if not items:
        raise SystemExit(f"DefiLlama: endpoint returned no list-like payload. url={used_url}")

    rows = []
    for it in items:
        name = (it.get("name") or it.get("protocol") or it.get("project") or it.get("title") or "").strip()
        if not name:
            continue

        # date field candidates
        dt = (
            it.get("date")
            or it.get("exploit_date")
            or it.get("timestamp")
            or it.get("time")
            or ""
        )

        # chain candidates
        chain = it.get("chain") or it.get("chains") or it.get("ecosystem") or ""
        if isinstance(chain, list):
            chain = ",".join([str(x) for x in chain if x])

        # type/technique candidates
        exploit_type = it.get("classification") or it.get("category") or it.get("technique") or it.get("attack") or ""

        # url candidates
        evidence_url = it.get("url") or it.get("link") or it.get("source_url") or ""

        # loss candidates
        loss = (
            it.get("loss")
            or it.get("loss_usd")
            or it.get("amount")
            or it.get("stolen")
            or None
        )

        rows.append({
            "protocol_name_raw": name,
            "source": "defillama",
            "exploit_date": dt,
            "loss_usd": _parse_loss(loss),
            "chain": str(chain or ""),
            "exploit_type": str(exploit_type or ""),
            "evidence_url": str(evidence_url or ""),
            "notes": f"llama_endpoint={used_url}".strip(),
        })

        time.sleep(sleep_sec)

    df = pd.DataFrame(rows, columns=COLS)

    # Clean + parse date
    df["protocol_name_raw"] = df["protocol_name_raw"].fillna("").astype(str).str.strip()
    df = df[df["protocol_name_raw"] != ""].copy()
    df["exploit_date"] = pd.to_datetime(df["exploit_date"], errors="coerce", utc=True)

    # Don’t drop rows without evidence_url (DefiLlama sometimes lacks it),
    # but keep them visible
    df["evidence_url"] = df["evidence_url"].fillna("").astype(str).str.strip()

    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(df)} rows -> {OUT}")
    print(f"   source endpoint: {used_url}")

if __name__ == "__main__":
    main()