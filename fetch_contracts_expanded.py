#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# NOTE: For real-time logging, forcibly unbuffer stdout and log file, and add heartbeat.
import sys
import threading
import signal
import os, re, json, time, asyncio, logging
print("🚀 Script started... loading .env and initializing asyncio", flush=True)
import os, re, json, time, asyncio, logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from dotenv import load_dotenv

# ────────────────────────────────────────────────────────────────────────────────
# ENV & PATHS
# ────────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
if not ETHERSCAN_API_KEY:
    raise SystemExit("Missing ETHERSCAN_API_KEY in .env")

GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "").strip()               # optional
ETHERSCAN_BASE  = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api").strip()

DATA_RAW = ROOT / "data_raw"
OUT_DIR  = ROOT / "outputs"
ADDR_DIR = DATA_RAW / "addrs_mined"
LOGS     = ROOT / "logs"

for p in [DATA_RAW, OUT_DIR, ADDR_DIR, LOGS, DATA_RAW / "contracts"]:
    p.mkdir(parents=True, exist_ok=True)

# --- Logging: force unbuffered output ---
class UnbufferedStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        try:
            self.flush()
        except Exception:
            pass

class UnbufferedFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        try:
            self.flush()
        except Exception:
            pass

log_file_path = LOGS / "fetch_contracts_expanded.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        UnbufferedFileHandler(log_file_path, mode="a", encoding="utf-8"),
        UnbufferedStreamHandler(sys.stdout)
    ]
)

# Heartbeat: log progress every minute
def heartbeat():
    while True:
        logging.info("💓 Heartbeat: script still running at %s", time.strftime("%Y-%m-%d %H:%M:%S"))
        for h in logging.getLogger().handlers:
            try: h.flush()
            except Exception: pass
        time.sleep(60)
threading.Thread(target=heartbeat, daemon=True).start()

# ────────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────────────────────────────────────
LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"

GH_ADAPTER_DIRS: List[Tuple[str, str]] = [("DefiLlama", "DefiLlama-Adapters")]
GH_YIELD_DIRS: List[Tuple[str, str]] = [("DefiLlama", "yield-server")]
GH_CODE_SEARCH = "https://api.github.com/search/code"

CHAIN_IDS: Dict[str, int] = {
    "ethereum": 1, "arbitrum": 42161, "base": 8453, "optimism": 10,
    "polygon": 137, "bsc": 56, "avalanche": 43114, "fantom": 250,
    "gnosis": 100, "linea": 59144, "scroll": 534352, "blast": 81457,
}

ADDR_RE = re.compile(r"0x[a-fA-F0-9]{40}")
CHAIN_ALIASES = {
    "ethereum":"ethereum","mainnet":"ethereum","eth":"ethereum",
    "arbitrum":"arbitrum","arbitrum-one":"arbitrum",
    "optimism":"optimism","op":"optimism",
    "polygon":"polygon","matic":"polygon",
    "bsc":"bsc","binance":"bsc","binance-smart-chain":"bsc",
    "avalanche":"avalanche","avax":"avalanche",
    "base":"base","fantom":"fantom","ftm":"fantom",
    "gnosis":"gnosis","xdai":"gnosis",
    "linea":"linea","scroll":"scroll","blast":"blast",
}

ZERO_ADDR = "0x0000000000000000000000000000000000000000"

def norm_chain(s: str) -> Optional[str]:
    if not s:
        return None
    return CHAIN_ALIASES.get(s.strip().lower())

# ────────────────────────────────────────────────────────────────────────────────
# HTTP HELPERS
# ────────────────────────────────────────────────────────────────────────────────
def gh_headers():
    h = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

async def http_get_json(session: aiohttp.ClientSession, url: str, *, headers=None, params=None, quiet_404=True):
    for i in range(4):
        try:
            async with session.get(url, headers=headers, params=params, timeout=45) as r:
                if r.status == 404 and quiet_404:
                    return None

                # Backoff for transient server/rate-limit errors
                if r.status in (429, 500, 502, 503):
                    await asyncio.sleep(2 ** i)
                    continue

                # 🔹 New: handle GitHub 403 rate limit
                if r.status == 403:
                    logging.warning(f"GitHub 403 for {url} — backing off 60s")
                    await asyncio.sleep(60)
                    continue

                r.raise_for_status()
                if "application/json" in r.headers.get("Content-Type", ""):
                    return await r.json()
                return await r.text()

        except Exception as e:
            logging.warning(f"Network fail GET {url}: {e}")
            await asyncio.sleep(1.5 * (i + 1))
    return None

async def http_get_text(session: aiohttp.ClientSession, url: str, *, headers=None):
    try:
        async with session.get(url, headers=headers, timeout=60) as r:
            r.raise_for_status()
            return await r.text()
    except Exception as e:
        logging.warning(f"Network fail GET TEXT {url}: {e}")
        return ""

# ────────────────────────────────────────────────────────────────────────────────
# DATA HELPERS (top protocols + simple rate limiter)
# ────────────────────────────────────────────────────────────────────────────────
async def get_top_protocols(session, top_n, allow_cats):
    raw = await http_get_json(session, LLAMA_PROTOCOLS_URL)
    if not raw:
        return []
    df = pd.DataFrame(raw)[["name", "symbol", "category", "tvl", "slug", "chains"]]
    if allow_cats:
        df = df[df["category"].isin(allow_cats)]
    return df.sort_values("tvl", ascending=False).head(top_n).to_dict("records")

class RateLimiter:
    def __init__(self, rps=3):
        self.gap = 1.0 / max(rps, 1)
        self.t = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            sleep = max(0.0, self.t + self.gap - now)
            if sleep > 0:
                await asyncio.sleep(sleep)
            self.t = time.monotonic()

# ────────────────────────────────────────────────────────────────────────────────
# GITHUB MINING HELPERS (needed by mine_addresses_for_slug)
# ────────────────────────────────────────────────────────────────────────────────
async def list_github_dir(session, owner: str, repo: str, subpath: str) -> List[dict]:
    """List files in a GitHub directory using the contents API."""
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{subpath}"
    res = await http_get_json(session, url, headers=gh_headers())
    return res if isinstance(res, list) else []

async def search_github_code(session, owner: str, repo: str, slug: str) -> List[dict]:
    """Fallback GitHub code search for files mentioning a slug."""
    q = f"repo:{owner}/{repo} {slug}"
    params = {"q": q, "per_page": 10}
    res = await http_get_json(session, GH_CODE_SEARCH, headers=gh_headers(), params=params, quiet_404=True)
    if not isinstance(res, dict) or not res.get("items"):
        return []
    items = res["items"]
    return [{"name": Path(i["path"]).name, "path": i["path"], "html_url": i.get("html_url", "")} for i in items]

def mine_addresses(text: str) -> List[str]:
    """Extract all Ethereum addresses from a text blob."""
    return list(set(re.findall(r"0x[a-fA-F0-9]{40}", text)))

def chain_hints(text: str) -> List[str]:
    """Look for mentions of supported chains in adapter source text."""
    hints = []
    for k in CHAIN_ALIASES.keys():
        if re.search(rf"\b{k}\b", text, re.IGNORECASE):
            hints.append(CHAIN_ALIASES[k])
    return list(set(hints))

def mine_addresses(text: str) -> List[str]:
    """Extract all Ethereum addresses from a text blob."""
    return list(set(re.findall(r"0x[a-fA-F0-9]{40}", text)))

# Graceful fallback when GitHub rate limits or fails
async def safe_list_github_dir(session, owner, repo, path):
    try:
        return await list_github_dir(session, owner, repo, path)
    except Exception as e:
        logging.warning(f"⚠️ GitHub directory fetch failed for {repo}/{path}: {e}")
        return []

async def safe_search_github_code(session, owner, repo, slug):
    try:
        return await search_github_code(session, owner, repo, slug)
    except Exception as e:
        logging.warning(f"⚠️ GitHub code search failed for {slug}: {e}")
        return []
        
async def mine_addresses_for_slug(session, slug: str) -> List[dict]:
    """
    Hybrid mining strategy:
      ① GitHub adapter scan (DefiLlama + yield-server)
      ② Etherscan verified contract registry (search by name/symbol hints)
      ③ Merge + deduplicate results
    """
    bag: List[dict] = []

    async def _consume_files(file_list: List[dict], owner: str, repo: str, subdesc: str):
        nonlocal bag
        for it in file_list:
            name = it.get("name", "")
            if "download_url" in it:
                txt = await http_get_text(session, it["download_url"], headers=gh_headers())
            else:
                path = it.get("path", "")
                if not path:
                    continue
                url = f"https://raw.githubusercontent.com/{owner}/{repo}/master/{path}"
                txt = await http_get_text(session, url, headers=gh_headers())

            if not txt:
                continue
            for m in ADDR_RE.finditer(txt or ""):
                addr = m.group(0).lower()
                if addr == ZERO_ADDR:
                    continue
                hints = chain_hints(txt or "")
                bag.append({
                    "address": addr,
                    "guess": hints[0] if hints else None,
                    "context": f"{owner}/{repo}/{subdesc}/{name}"
                })

    # ──────────────── (1) DefiLlama adapters ────────────────
    for owner, repo in GH_ADAPTER_DIRS:
        files = await safe_list_github_dir(session, owner, repo, f"projects/{slug}")
        await _consume_files(files, owner, repo, f"projects/{slug}")

    # ──────────────── (2) yield-server ────────────────
    for owner, repo in GH_YIELD_DIRS:
        files = await safe_list_github_dir(session, owner, repo, f"projects/{slug}")
        await _consume_files(files, owner, repo, f"src/adaptors/{slug}")

    # ──────────────── (3) fallback code search ────────────────
    if not bag:
        for owner, repo in GH_ADAPTER_DIRS + GH_YIELD_DIRS:
            results = await safe_search_github_code(session, owner, repo, slug)
            await _consume_files(results, owner, repo, f"search:{slug}")

    # ──────────────── (4) verified registry expansion ────────────────
    # Try querying Etherscan registry to find other verified contracts
    # that mention this protocol’s name or symbol (e.g., Aave, Compound, GMX)
    base_v1 = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/api")
    params = {
        "module": "account",
        "action": "getsourcecode",
        "apikey": ETHERSCAN_API_KEY
    }
    for chain, cid in CHAIN_IDS.items():
        params["chainid"] = cid
        for kw in [slug, slug.replace("-", ""), slug.split("-")[0]]:
            url = f"{base_v1}?module=contract&action=getsourcecode&address={kw}&apikey={ETHERSCAN_API_KEY}"
            try:
                res = await http_get_json(session, url, headers=gh_headers(), quiet_404=True)
                if not res or "result" not in res:
                    continue
                for r in res["result"]:
                    addr = (r.get("Address") or "").lower()
                    if addr and addr not in [x["address"] for x in bag]:
                        bag.append({
                            "address": addr,
                            "guess": chain,
                            "context": f"etherscan-registry:{slug}"
                        })
            except Exception:
                continue

    # ──────────────── (5) Deduplication ────────────────
    seen = set()
    uniq = []
    for it in bag:
        if it["address"] not in seen:
            uniq.append(it)
            seen.add(it["address"])

    return uniq


async def main():
    TOP = int(os.getenv("TOP_N_PROTOCOLS", "40"))
    CAP = int(os.getenv("PER_PROTOCOL_CAP", "120"))
    PROT_CONC = int(os.getenv("PROTOCOL_CONCURRENCY", "6"))
    RPS = float(os.getenv("ETHERSCAN_RPS", "3"))
    ALLOW = os.getenv(
        "ALLOW_CATEGORIES",
        "Dex,Lending,Derivatives,Stablecoins,Bridges,Yield,CDP,Perpetuals"
    ).split(",")

    contracts_csv = DATA_RAW / "contracts" / "verified_contracts_expanded.csv"
    checkpoint    = OUT_DIR / "checkpoints_contracts.json"

    if contracts_csv.exists():
        print(f"Resuming existing file: {contracts_csv}")
        logging.info("RESUME: Will skip protocols already in outputs/checkpoints_contracts.json")
    state = {"done": []}
    if checkpoint.exists():
        try:
            state = json.loads(checkpoint.read_text())
        except Exception:
            pass

    limiter = RateLimiter(rps=RPS)
    rows: List[dict] = []

    async with aiohttp.ClientSession() as session:
        protos = await get_top_protocols(session, TOP, ALLOW)
        print(f"Loaded {len(protos)} protocols to mine...")
        tasks = []
        sem = asyncio.Semaphore(PROT_CONC)

        async def handle_protocol(p):
            slug = p["slug"]
            if slug in state["done"]:
                logging.info(f"⏭️ Skipping {slug} (already in checkpoint)")
                return []
            async with sem:
                try:
                    logging.info(f"🧠 Mining protocol: {slug}")
                    mined = await mine_addresses_for_slug(session, slug)
                    rows_local = [
                        {
                            "slug": slug,
                            "address": m["address"],
                            "guess": m["guess"],
                            "category": p.get("category"),
                            "tvl": p.get("tvl"),
                            "chains": ";".join(p.get("chains", [])),
                            "scrape_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for m in mined
                    ]

                    # save local results
                    (ADDR_DIR / f"{slug}.json").write_text(json.dumps(mined, indent=2))
                    state["done"].append(slug)
                    checkpoint.write_text(json.dumps(state))
                    logging.info(f"✅ Finished {slug} — {len(mined)} addresses mined")
                    return rows_local

                except Exception as e:
                    logging.error(f"❌ Error mining {slug}: {e}")
                    return []

        # schedule all protocols concurrently
        for p in protos:
            tasks.append(handle_protocol(p))

        results = await asyncio.gather(*tasks)
        rows = [r for batch in results for r in batch]

    # ───────────────────────────────────────
    # ────────────────────────────────────────────────
# ────────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    async def runner():
        results = await main()
        rows = results if isinstance(results, list) else []

        contracts_csv = Path("data_raw/contracts/verified_contracts_expanded.csv")

        if rows:
            df = pd.DataFrame(rows)
            if contracts_csv.exists():
                df_existing = pd.read_csv(contracts_csv)
                combined = pd.concat([df_existing, df], ignore_index=True)
                combined.drop_duplicates(subset=["protocol", "address"], inplace=True)
                combined.to_csv(contracts_csv, index=False)
                logging.info(f"🧩 Appended {len(df)} new rows → total {len(combined)} rows → {contracts_csv}")
            else:
                df.to_csv(contracts_csv, index=False)
                logging.info(f"✅ Wrote {len(df)} rows → {contracts_csv}")
        else:
            logging.warning("⚠️ No new contracts mined this run.")

    asyncio.run(runner())