import json
import pandas as pd
from pathlib import Path
import re

IN_PATH = Path("data_raw/exploits_raw/rekt_news.json")
OUT_PATH = Path("data_processed/exploits_normalized_rekt.csv")

def safe_str(v):
    """Return a clean string no matter what JSON value we get (None, int, etc.)."""
    if isinstance(v, str):
        return v
    if v is None:
        return ""
    # fall back to string representation
    return str(v)

def extract_loss(text):
    text = safe_str(text)
    match = re.search(r"\$([\d,.]+)", text)
    if match:
        return match.group(1).replace(",", "")
    return None

def normalize_rekt_news():
    print("🔍 Normalizing Rekt.News dataset...")
    # Be tolerant to empty / malformed file
    raw = IN_PATH.read_text() if IN_PATH.exists() else "[]"
    try:
        data = json.loads(raw) or []
    except Exception as e:
        print(f"⚠️ Could not load JSON from {IN_PATH}: {e}")
        data = []

    # Accept both a list of articles or a dict with 'items'
    if isinstance(data, dict) and "items" in data:
        data = data["items"]

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.touch(exist_ok=True)

    records = []
    for item in data:
        # always sanitize first
        title = safe_str(item.get("title"))
        raw_date = item.get("date")
        date = safe_str(raw_date).strip() if raw_date else ""
        url   = safe_str(item.get("url"))
        text  = safe_str(item.get("text"))

        # Basic heuristic extraction
        protocol = title.split("-")[0].strip() if "-" in title else title.strip()
        loss_usd = extract_loss(text)

        exploit_type = None
        lower = text.lower()
        for keyword in ["rug", "rugpull", "reentrancy", "oracle", "flash loan", "flash-loan", "exploit"]:
            if keyword in lower:
                exploit_type = keyword
                break

        records.append({
            "source": "Rekt.News",
            "protocol_name": protocol,
            "exploit_date": date,
            "loss_usd": loss_usd,
            "exploit_type": exploit_type,
            "summary": text[:300],
            "url": url,
            "raw_title": title,
        })

    df = pd.DataFrame(records)
    try:
        df.to_csv(OUT_PATH, index=False)
        print(f"✅ Normalized {len(df)} entries → {OUT_PATH}")
    except PermissionError:
        print(f"❌ Permission denied when writing to {OUT_PATH}. Try running with sudo or adjust file permissions.")

if __name__ == "__main__":
    normalize_rekt_news()