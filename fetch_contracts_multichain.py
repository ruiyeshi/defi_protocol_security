import pandas as pd, requests, time, logging, json, os
from tqdm import tqdm
from config_loader import load_api_key
from typing import List
from datetime import datetime
import random

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Chain explorer API URLs and keys
chains = {
    "ethereum": {
        "url": "https://api.etherscan.io/api",
        "key": os.getenv("ETHERSCAN_KEY") or "YOUR_ETHERSCAN_KEY"
    },
    "arbitrum": {
        "url": "https://api.arbiscan.io/api",
        "key": os.getenv("ARBISCAN_KEY") or "YOUR_ARBITRUM_KEY"
    },
    "optimism": {
        "url": "https://api-optimistic.etherscan.io/api",
        "key": os.getenv("OPTIMISTIC_ETHERSCAN_KEY") or "YOUR_OPTIMISM_KEY"
    },
    "bsc": {
        "url": "https://api.bscscan.com/api",
        "key": os.getenv("BSCSCAN_KEY") or "YOUR_BSCSCAN_KEY"
    },
    "polygon": {
        "url": "https://api.polygonscan.com/api",
        "key": os.getenv("POLYGONSCAN_KEY") or "YOUR_POLYGON_KEY"
    },
    "base": {
        "url": "https://api.basescan.org/api",
        "key": os.getenv("BASESCAN_KEY") or "YOUR_BASE_KEY"
    }
}

# Optionally, you can provide a list of keys per chain for rotation
API_KEYS = {
    # "ethereum": ["key1", "key2"],
    # ...
}

# Helper to get an API key for a chain (rotate if multiple)
def get_api_key(chain):
    if chain in API_KEYS and API_KEYS[chain]:
        return random.choice(API_KEYS[chain])
    return chains[chain]["key"]

# Retry decorator for API calls
def retry_request(fn, max_attempts=3, wait=1):
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            logging.warning(f"Request failed (attempt {attempt+1}/{max_attempts}): {e}")
            time.sleep(wait * (attempt+1))
    return None

# Read protocols
df = pd.read_csv("data_raw/contracts/verified_contracts_expanded.csv")
logging.info(f"Processing {len(df)} contracts from verified_contracts_expanded.csv")
rows = []
json_rows = []

def is_valid_address(addr):
    # Basic check for 0x-prefixed 40-hex string
    return isinstance(addr, str) and addr.startswith("0x") and len(addr) == 42

for idx, row in tqdm(df.iterrows(), total=len(df)):
    name = row.get("name", "")
    address = row.get("contract_address", "")
    chain = row.get("chain", "")
    if not is_valid_address(address):
        logging.info(f"Skipping invalid or missing address for {name} on {chain}: {address}")
        continue
    if chain not in chains:
        logging.info(f"Skipping unsupported chain for {name}: {chain}")
        continue
    found = False
    info = chains[chain]
    base_url = info["url"]
    api_key = get_api_key(chain)
    url = f"{base_url}?module=contract&action=getsourcecode&address={address}&apikey={api_key}"
    def req():
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code}")
        return resp.json()
    r = retry_request(req, max_attempts=5, wait=1)
    if not r:
        logging.error(f"Failed to fetch for {name} {chain} {address}")
        continue
    if r.get("result") and isinstance(r["result"], list) and len(r["result"]) > 0:
        res = r["result"][0]
        if res.get("SourceCode"):
            # Add timestamp and compiler version
            timestamp = datetime.utcnow().isoformat() + "Z"
            compiler_version = res.get("CompilerVersion", "")
            row_obj = {
                "protocol_name": name,
                "chain": chain,
                "contract_address": address,
                "source_code": res["SourceCode"],
                "compiler_version": compiler_version,
                "timestamp_utc": timestamp
            }
            rows.append(row_obj)
            json_rows.append(row_obj)
            found = True
            logging.info(f"Found verified contract for {name} on {chain}")
    # Rate limiting: sleep a bit between requests (randomized)
    time.sleep(0.3 + random.uniform(0, 0.4))
    if not found:
        logging.info(f"No verified contract found for {name} ({address})")

out = pd.DataFrame(rows)
csv_path = "data_raw/contracts/fetched_contract_sources.csv"
json_path = "data_raw/contracts/fetched_contract_sources.json"
out.to_csv(csv_path, index=False)
with open(json_path, "w") as f:
    json.dump(json_rows, f, indent=2)
logging.info(f"✅ Saved verified contracts → {csv_path} ({len(out)} rows)")
logging.info(f"✅ Saved verified contracts JSON → {json_path} ({len(json_rows)} rows)")