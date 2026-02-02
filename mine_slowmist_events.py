from __future__ import annotations

from pathlib import Path
import json
import re
import pandas as pd
from bs4 import BeautifulSoup

# Output
OUT = Path("data_raw/exploits/exploit_events_slowmist.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

# Inputs you ALREADY have in your repo (per your find output)
IN_JSON = Path("data_raw/exploits_raw/slowmist_news.json")
IN_HTML = Path("data_raw/exploits_raw/_cache/slowmist_hackfeed.html")

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

def _parse_loss(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        v = float(x)
        return v if v > 0 else None
    s = str(x).strip().replace(",", "")
    # $4.8M / 4.8m / 4800000
    m = re.match(r"^\$?\s*([0-9]*\.?[0-9]+)\s*([kKmMbB])?\s*$", s)
    if not m:
        return None
    v = float(m.group(1))
    suf = (m.group(2) or "").lower()
    mult = {"k": 1e3, "m": 1e6, "b": 1e9}.get(suf, 1.0)
    v = v * mult
    return v if v > 0 else None

def _clean_str(x) -> str:
    return ("" if x is None else str(x)).strip()

def _write_empty():
    pd.DataFrame(columns=COLS).to_csv(OUT, index=False)
    print(f"✅ Wrote empty CSV with headers -> {OUT}")

def _from_slowmist_news_json(p: Path) -> pd.DataFrame:
    raw = json.loads(p.read_text(encoding="utf-8"))

    # Accept list[dict] or dict with a list field
    items = None
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        # common keys you might have
        for k in ["data", "items", "list", "results"]:
            if isinstance(raw.get(k), list):
                items = raw[k]
                break

    if not isinstance(items, list):
        return pd.DataFrame(columns=COLS)

    rows = []
    for it in items:
        if not isinstance(it, dict):
            continue

        title = _clean_str(it.get("title") or it.get("name"))
        project = _clean_str(it.get("project") or it.get("project_name") or it.get("protocol") or it.get("victim"))
        url = _clean_str(it.get("url") or it.get("link") or it.get("source_url"))

        # NO GARBAGE
        proto = project or title
        if not proto or not url:
            continue

        dt = it.get("date") or it.get("publish_time") or it.get("time") or it.get("published_at") or ""
        loss = it.get("loss_usd") or it.get("amount") or it.get("loss") or None
        chain = it.get("chain") or it.get("ecosystem") or ""
        exploit_type = it.get("category") or it.get("attack_method") or it.get("attackMethod") or ""

        rows.append({
            "protocol_name_raw": proto,
            "source": "slowmist",
            "exploit_date": dt,
            "loss_usd": _parse_loss(loss),
            "chain": _clean_str(chain),
            "exploit_type": _clean_str(exploit_type),
            "evidence_url": url,
            "notes": _clean_str(title),
        })

    df = pd.DataFrame(rows, columns=COLS)
    if df.empty:
        return df

    df["exploit_date"] = pd.to_datetime(df["exploit_date"], errors="coerce", utc=True)
    df["protocol_name_raw"] = df["protocol_name_raw"].astype(str).str.strip()
    df["evidence_url"] = df["evidence_url"].astype(str).str.strip()
    df = df[(df["protocol_name_raw"] != "") & (df["evidence_url"] != "")]
    df = df.drop_duplicates(subset=["evidence_url"], keep="first")
    return df

def _from_cached_html(p: Path) -> pd.DataFrame:
    html = p.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    rows = []

    # Best-effort generic extraction: look for links that resemble incident detail pages
    # If your cache structure differs, we still avoid garbage rows.
    for a in soup.find_all("a", href=True):
        href = _clean_str(a.get("href"))
        text = _clean_str(a.get_text(" ", strip=True))
        if not href or not text:
            continue
        # heuristics: keep only hacked.slowmist.io links or slowmist medium posts
        if ("slowmist" not in href) and ("hacked" not in href):
            continue

        # treat link text as title/protocol candidate
        proto = text
        if not proto:
            continue

        rows.append({
            "protocol_name_raw": proto,
            "source": "slowmist",
            "exploit_date": None,
            "loss_usd": None,
            "chain": "",
            "exploit_type": "",
            "evidence_url": href,
            "notes": "from_cached_html",
        })

    df = pd.DataFrame(rows, columns=COLS)
    if df.empty:
        return df

    df["protocol_name_raw"] = df["protocol_name_raw"].astype(str).str.strip()
    df["evidence_url"] = df["evidence_url"].astype(str).str.strip()
    df = df[(df["protocol_name_raw"] != "") & (df["evidence_url"] != "")]
    df = df.drop_duplicates(subset=["evidence_url"], keep="first")
    return df

def main():
    frames = []

    if IN_JSON.exists():
        dfj = _from_slowmist_news_json(IN_JSON)
        print(f"✅ SlowMist: from JSON {IN_JSON} -> rows={len(dfj)}")
        if not dfj.empty:
            frames.append(dfj)
    else:
        print(f"⚠️ SlowMist: missing {IN_JSON}")

    if not frames and IN_HTML.exists():
        dfh = _from_cached_html(IN_HTML)
        print(f"✅ SlowMist: from cached HTML {IN_HTML} -> rows={len(dfh)}")
        if not dfh.empty:
            frames.append(dfh)

    if not frames:
        print("⚠️ SlowMist: no usable local sources found (JSON/HTML). Writing empty.")
        _write_empty()
        return

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["evidence_url"], keep="first")
    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(df)} rows -> {OUT}")

if __name__ == "__main__":
    main()