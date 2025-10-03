#!/usr/bin/env python3
"""
fetch_audit_certik_fallback.py
Fully production-ready fallback scraper for CertiK audit data.
- No API dependency (CertiK API deprecated)
- Cloudscraper stealth bypass for 403/anti-bot
- Randomized headers & retry mechanism
- Parses audit score and date from HTML
"""

import os
import time
import random
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm
import requests

# === Paths ===
ROOT_DIR = Path(__file__).resolve()
while ROOT_DIR.name != "defi_protocol_security" and ROOT_DIR.parent != ROOT_DIR:
    ROOT_DIR = ROOT_DIR.parent

DATA_DIR = ROOT_DIR / "data_raw" / "contracts"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = DATA_DIR / "audit_metadata_certik.csv"

# === User-Agent rotation ===
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Edge/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/127.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Mobile/15E148 Safari/604.1"
]

# === Initialize cloudscraper ===
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "darwin", "mobile": False}
)

# === Dynamically load top DeFi projects ===
print("🌐 Fetching top DeFi projects from DeFiLlama...")
try:
    r = requests.get("https://api.llama.fi/protocols", timeout=30)
    llama_projects = [p["name"].lower().replace(" ", "-") for p in r.json() if "name" in p]
    PROTOCOLS = llama_projects[:120]  # Limit to top 120
    print(f"✅ Loaded {len(PROTOCOLS)} protocol names from DeFiLlama.")
except Exception as e:
    print(f"⚠️ Failed to fetch from DeFiLlama, falling back to static list: {e}")
    PROTOCOLS = [
        "aave", "curve", "uniswap", "pancakeswap", "balancer", "compound",
        "sushi", "lido", "venus", "frax", "1inch", "pendle", "synapse",
        "maverick", "radiate", "camelot", "traderjoe", "raydium", "osmosis"
    ]

# === HTML parsing helper ===
def parse_certik_page(protocol):
    url = f"https://www.certik.com/projects/{protocol.lower()}"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.certik.com/"
    }

    for attempt in range(3):
        try:
            # --- main request ---
            r = scraper.get(url, headers=headers, timeout=25)
            
            # --- handle status codes ---
            if r.status_code == 404:
                print(f"⚠️ Page not found for {protocol}")
                return None
            if r.status_code == 403:
                print(f"🚫 Forbidden (403) for {protocol}, switching user-agent & retrying...")
                headers["User-Agent"] = (
                    f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_{random.randint(1,7)}) "
                    f"AppleWebKit/537.{random.randint(30,50)} (KHTML, like Gecko) "
                    f"Chrome/{random.randint(110,126)}.0.{random.randint(1000,6000)}.0 Safari/537.{random.randint(30,50)}"
                )
                time.sleep(random.uniform(3, 6))
                continue
            if r.status_code != 200:
                print(f"❌ Failed {protocol} (status {r.status_code})")
                return None
            
            # --- success, parse HTML ---
            soup = BeautifulSoup(r.text, "html.parser")
            score_tag = soup.find("div", class_="score__value") or soup.find("span", class_="score")
            score = score_tag.text.strip() if score_tag else None

            # extract audit date
            audit_date = None
            for tag in soup.find_all("div"):
                txt = tag.text.strip()
                if "Audit" in txt and any(ch.isdigit() for ch in txt):
                    audit_date = txt.split()[-1]
                    break

            return {
                "protocol_name": protocol,
                "audit_firm": "CertiK",
                "audit_score": score,
                "audit_link": url,
                "audit_date": audit_date
            }

        except Exception as e:
            print(f"⚠️ Error fetching {protocol}: {e}")
            time.sleep(random.uniform(2, 5))
            continue


# === Main ===
if __name__ == "__main__":
    print("🔍 Fetching CertiK audit metadata (stealth mode, HTML only)...")
    results = []

    for protocol in tqdm(PROTOCOLS):
        result = parse_certik_page(protocol)
        if result:
            results.append(result)
        time.sleep(random.uniform(1.2, 2.0))  # polite delay

    if results:
        df = pd.DataFrame(results)
        df.drop_duplicates(subset=["protocol_name"], inplace=True)
        df.to_csv(OUT_CSV, index=False)
        print(f"✅ Saved enhanced CertiK metadata → {OUT_CSV} ({len(df)} rows)")
    else:
        print("❌ No data retrieved — CertiK may require captcha bypass or VPN.")