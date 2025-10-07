import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import time

OUT = Path("data_raw/exploits_raw/defillama_hacks.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DeFiResearchBot/1.0)"
}

def fetch_csv():
    url = "https://defillama.com/hacks.csv"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print("CSV fetch failed:", e)
    return None

def parse_csv(text):
    from io import StringIO
    df = pd.read_csv(StringIO(text))
    return df

def scrape_html():
    url = "https://defillama.com/hacks"
    r = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("No table found in HTML")
        return pd.DataFrame()

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) != len(headers):
            continue
        data = dict(zip(headers, cols))
        rows.append(data)
    return pd.DataFrame(rows)

def fetch_api():
    url = "https://pro-api.llama.fi/api/hacks"  # or /api/hacks
    # Or maybe just https://api.llama.fi/hacks — check docs
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        j = r.json()
        # Depending on JSON structure, convert to DataFrame
        if "data" in j:
            return pd.DataFrame(j["data"])
        # fallback: if it's a list
        if isinstance(j, list):
            return pd.DataFrame(j)
    except Exception as e:
        print("API fetch failed:", e)
    return pd.DataFrame()

def main():
    # Try CSV
    csv_text = fetch_csv()
    if csv_text:
        print("✅ Fetched CSV version")
        df = parse_csv(csv_text)
    else:
        print("⚠️ CSV fetch failed, trying HTML scrape")
        df = scrape_html()
        if df.empty:
            print("⚠️ HTML scrape failed, trying API fallback")
            df = fetch_api()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print("Saved hacks data →", OUT)
    print("Rows:", len(df))

if __name__ == "__main__":
    main()