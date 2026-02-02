from __future__ import annotations

from pathlib import Path
import time
import re
import requests
import pandas as pd

OUT = Path("data_raw/exploits/exploit_events_rekt.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

BASE = "https://rekt.news"
API = f"{BASE}/wp-json/wp/v2/posts"

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

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

def guess_protocol_from_title(title: str) -> str:
    t = (title or "").strip()
    t = re.sub(r"\s*\|\s*rekt\.news\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*[-–—]\s*rekt\s*$", "", t, flags=re.I)
    t = re.sub(r"\s+rekt\s*$", "", t, flags=re.I)
    return t.strip(" -–—|:").strip()

def parse_loss_from_text(text: str) -> float | None:
    if not text:
        return None
    matches = re.findall(r"\$\s*([0-9]{1,3}(?:[0-9,]{0,})?(?:\.[0-9]+)?)\s*([kKmMbB])?", text)
    if not matches:
        return None
    best = 0.0
    for num, suf in matches:
        try:
            v = float(num.replace(",", ""))
        except Exception:
            continue
        mult = {"k": 1e3, "m": 1e6, "b": 1e9}.get((suf or "").lower(), 1.0)
        best = max(best, v * mult)
    return best if best > 0 else None

def main():
    rows = []
    page = 1
    per_page = 100  # max is often 100 on WP

    while True:
        params = {
            "per_page": per_page,
            "page": page,
            "_fields": "link,date,title,yoast_head_json,content",
        }
        r = requests.get(API, params=params, headers=HEADERS, timeout=30)
        if r.status_code == 400 and "rest_post_invalid_page_number" in r.text:
            break
        r.raise_for_status()

        items = r.json()
        if not items:
            break

        for it in items:
            url = (it.get("link") or "").strip()
            dt = it.get("date") or ""
            title_obj = it.get("title") or {}
            title = (title_obj.get("rendered") or "").strip()
            if not url or not title:
                continue

            proto = guess_protocol_from_title(title)
            if not proto or proto.lower() in {"untitled", "rekt"}:
                continue

            # optional: loss from yoast meta / content text
            yo = it.get("yoast_head_json") or {}
            desc = (yo.get("description") or "").strip()

            # WP content can be huge; safe best-effort
            content = it.get("content") or {}
            content_text = (content.get("rendered") or "")
            loss = parse_loss_from_text(desc) or parse_loss_from_text(content_text)

            rows.append({
                "protocol_name_raw": proto,
                "source": "rekt",
                "exploit_date": dt,
                "loss_usd": loss,
                "chain": "",
                "exploit_type": "",
                "evidence_url": url,
                "notes": f"title={title}",
            })

        print(f"page={page} fetched={len(items)} kept_total={len(rows)}")
        page += 1
        time.sleep(0.4)

    df = pd.DataFrame(rows, columns=COLS)
    if not df.empty:
        df["exploit_date"] = pd.to_datetime(df["exploit_date"], errors="coerce", utc=True)
        df = df.drop_duplicates(subset=["evidence_url"], keep="first")

    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(df)} rows -> {OUT}")

if __name__ == "__main__":
    main()