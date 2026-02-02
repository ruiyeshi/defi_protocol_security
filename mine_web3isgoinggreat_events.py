from __future__ import annotations

from pathlib import Path
import time
import re
import requests
import pandas as pd

OUT = Path("data_raw/exploits/exploit_events_web3isgoinggreat.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

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

BASE = "https://www.web3isgoinggreat.com"
API = f"{BASE}/web1"

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

HINT_WORDS = re.compile(r"\b(hack|hacked|exploit|drain|drained|breach|stolen)\b", re.I)

def main():
    rows = []
    cursor = None
    pages = 0

    while True:
        params = {"direction": "next"}
        if cursor:
            params["cursor"] = cursor

        r = requests.get(API, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()

        pages += 1

        # try common shapes
        items = data.get("items") or data.get("entries") or data.get("data") or []
        if not isinstance(items, list) or len(items) == 0:
            break

        for it in items:
            # best-effort field names
            title = (it.get("title") or it.get("headline") or "").strip()
            body = (it.get("body") or it.get("description") or it.get("content") or "").strip()
            themes = it.get("themes") or it.get("tags") or []
            if isinstance(themes, str):
                themes = [themes]

            dt = it.get("date") or it.get("published_at") or it.get("created_at") or ""
            url = (it.get("url") or it.get("link") or "").strip()
            if url and url.startswith("/"):
                url = BASE + url

            text = f"{title} {body}".strip()
            is_hack_theme = any(str(t).lower() == "hack" for t in themes)
            is_hack_text = bool(HINT_WORDS.search(text))

            if not (is_hack_theme or is_hack_text):
                continue
            if not url:
                continue

            # protocol_name_raw is hard here; use a conservative placeholder:
            proto = title if title else "unknown"
            rows.append({
                "protocol_name_raw": proto,
                "source": "web3isgoinggreat",
                "exploit_date": dt,
                "loss_usd": None,
                "chain": "",
                "exploit_type": "hack",
                "evidence_url": url,
                "notes": "",
            })

        # next cursor
        cursor = data.get("next") or data.get("nextCursor") or data.get("cursor_next")
        print(f"pages={pages} kept_total={len(rows)} next={cursor}")

        if not cursor:
            break
        time.sleep(0.25)

    df = pd.DataFrame(rows, columns=COLS)
    if not df.empty:
        df["exploit_date"] = pd.to_datetime(df["exploit_date"], errors="coerce", utc=True)
        df = df.drop_duplicates(subset=["evidence_url"], keep="first")

    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(df)} rows -> {OUT}")

if __name__ == "__main__":
    main()