import requests
import csv
import time

OUTPUT_FILE = "data_raw/contracts/verified_contracts_extended.csv"
PROTOCOLS_URL = "https://api.llama.fi/protocols"

def get_protocols():
    r = requests.get(PROTOCOLS_URL, timeout=30)
    r.raise_for_status()
    return r.json()

def get_contracts_for_protocol(slug):
    urls = [
        f"https://api.llama.fi/protocol/{slug}",
        f"https://api.llama.fi/summary/{slug}",
    ]
    addresses = set()
    chains = set()
    data = {}

    for url in urls:
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                break
        except Exception as e:
            print(f"⚠️ Error fetching {slug} from {url}: {e}")

    # Crawl possible fields
    def crawl(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k.lower() in ["address", "contractaddress", "contract"]:
                    if isinstance(v, str) and len(v) > 5:
                        addresses.add(v)
                if k.lower() == "chain" and isinstance(v, str):
                    chains.add(v)
                crawl(v)
        elif isinstance(obj, list):
            for i in obj:
                crawl(i)

    crawl(data)
    return list(addresses), list(chains)

def main():
    protocols = get_protocols()
    rows = []
    total = len(protocols)
    print(f"🔍 Found {total} protocols from DeFiLlama")

    for i, p in enumerate(protocols[:150]):  # limit for now
        slug = p.get("slug", "")
        name = p.get("name", "")
        print(f"🔎 Fetching verified contracts for {name}...")
        addrs, chains = get_contracts_for_protocol(slug)
        if addrs:
            for addr in addrs:
                rows.append({
                    "protocol": name,
                    "slug": slug,
                    "chain": ",".join(chains) if chains else "unknown",
                    "contract_addr": addr
                })
        time.sleep(0.5)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["protocol", "slug", "chain", "contract_addr"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Saved verified contracts → {OUTPUT_FILE} ({len(rows)} rows)")

if __name__ == "__main__":
    main()