import json
import pandas as pd
from pathlib import Path
import re

IN_PATH = Path("data_raw/exploits_raw/rekt_news.json")
OUT_PATH = Path("data_processed/exploits_normalized_rekt.csv")

def extract_loss(text):
    match = re.search(r"\$([\d,.]+)", text)
    if match:
        return match.group(1).replace(",", "")
    return None

def normalize_rekt_news():
    print("🔍 Normalizing Rekt.News dataset...")
    data = json.loads(IN_PATH.read_text())
    records = []
    
    for item in data:
        text = item.get("text", "")
        title = item.get("title", "").strip()
        date = item.get("date", "").strip()
        url = item.get("url", "")
        
        # Basic heuristic extraction
        protocol = title.split("-")[0].strip() if "-" in title else title
        loss_usd = extract_loss(text)
        exploit_type = None
        for keyword in ["rug", "reentrancy", "oracle", "flash loan", "exploit"]:
            if keyword in text.lower():
                exploit_type = keyword
                break
        
        records.append({
            "source": "Rekt.News",
            "protocol_name": protocol,
            "exploit_date": date,
            "loss_usd": loss_usd,
            "exploit_type": exploit_type,
            "summary": text[:300],
            "url": url
        })
    
    df = pd.DataFrame(records)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"✅ Normalized {len(df)} entries → {OUT_PATH}")

if __name__ == "__main__":
    normalize_rekt_news()