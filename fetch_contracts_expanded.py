#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_contracts_expanded.py

Goal:
  Expand data_raw/contracts/verified_contracts_expanded.csv with MANY verified contracts per protocol,
  across multiple EVM chains supported by Etherscan v2 (chainid).

Pipeline:
  1) Sample protocols from DeFiLlama (optionally stratified by category × chain × TVL tier)
  2) Mine candidate addresses from DefiLlama adapter repos (and code search fallback)
  3) Verify each address via Etherscan v2 getsourcecode on a best-effort chain search
  4) Append only VERIFIED contracts to verified_contracts_expanded.csv (dedup by slug,address,chain)
"""

import sys
import threading
import os
import re
import json
import time
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from dotenv import load_dotenv

print("🚀 Script started... loading .env and initializing asyncio", flush=True)

# ────────────────────────────────────────────────────────────────────────────────
# ENV & PATHS
# ────────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
if not ETHERSCAN_API_KEY:
    raise SystemExit("Missing ETHERSCAN_API_KEY in .env")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()  # optional
ETHERSCAN_BASE = os.getenv(
    "ETHERSCAN_BASE_URL",
    os.getenv("ETHERSCAN_V2_URL", "https://api.etherscan.io/v2/api"),
).strip()

DATA_RAW = ROOT / "data_raw"
OUT_DIR = ROOT / "outputs"
ADDR_DIR = DATA_RAW / "addrs_mined"
LOGS = ROOT / "logs"

for p in [DATA_RAW, OUT_DIR, ADDR_DIR, LOGS, DATA_RAW / "contracts"]:
    p.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# LOGGING (unbuffered + heartbeat)
# ────────────────────────────────────────────────────────────────────────────────
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
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        UnbufferedFileHandler(log_file_path, mode="a", encoding="utf-8"),
        UnbufferedStreamHandler(sys.stdout),
    ],
)

def heartbeat():
    while True:
        logging.info("💓 Heartbeat: still running at %s", time.strftime("%Y-%m-%d %H:%M:%S"))
        for h in logging.getLogger().handlers:
            try:
                h.flush()
            except Exception:
                pass
        time.sleep(60)

threading.Thread(target=heartbeat, daemon=True).start()

# ────────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────────────────────────────────────
LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"

GH_ADAPTER_DIRS: List[Tuple[str, str]] = [("DefiLlama", "DefiLlama-Adapters")]
GH_YIELD_DIRS: List[Tuple[str, str]] = [("DefiLlama", "yield-server")]
GH_CODE_SEARCH = "https://api.github.com/search/code"

# Etherscan v2 chain IDs (add more if needed)
CHAIN_IDS: Dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "base": 8453,
    "optimism": 10,
    "polygon": 137,
    "bsc": 56,
    "avalanche": 43114,
    "fantom": 250,
    "gnosis": 100,
    "linea": 59144,
    "scroll": 534352,
    "blast": 81457,
}

# Preferred order when trying chains for verification
CHAIN_PREF: List[str] = [
    "ethereum", "arbitrum", "optimism", "base",
    "polygon", "bsc", "avalanche", "fantom",
    "gnosis", "linea", "scroll", "blast",
]

ADDR_RE = re.compile(r"0x[a-fA-F0-9]{40}")
CHAIN_ALIASES = {
    "ethereum": "ethereum", "mainnet": "ethereum", "eth": "ethereum",
    "arbitrum": "arbitrum", "arbitrum-one": "arbitrum",
    "optimism": "optimism", "op": "optimism",
    "polygon": "polygon", "matic": "polygon",
    "bsc": "bsc", "binance": "bsc", "binance-smart-chain": "bsc",
    "avalanche": "avalanche", "avax": "avalanche",
    "base": "base",
    "fantom": "fantom", "ftm": "fantom",
    "gnosis": "gnosis", "xdai": "gnosis",
    "linea": "linea",
    "scroll": "scroll",
    "blast": "blast",
}

ZERO_ADDR = "0x0000000000000000000000000000000000000000"

def norm_chain(s: str) -> Optional[str]:
    if not s:
        return None
    return CHAIN_ALIASES.get(str(s).strip().lower())

def parse_csv_list(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in str(s).split(",")]
    return [p for p in parts if p]

# ────────────────────────────────────────────────────────────────────────────────
# HTTP HELPERS
# ────────────────────────────────────────────────────────────────────────────────
def gh_headers():
    h = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

async def http_get_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers=None,
    params=None,
    quiet_404: bool = True,
):
    for i in range(5):
        try:
            async with session.get(url, headers=headers, params=params, timeout=60) as r:
                if r.status == 404 and quiet_404:
                    return None

                # transient / rate-limit
                if r.status in (429, 500, 502, 503):
                    await asyncio.sleep(2 ** i)
                    continue

                # GitHub 403 rate limit
                if r.status == 403 and "api.github.com" in url:
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

async def http_get_text(session: aiohttp.ClientSession, url: str, *, headers=None) -> str:
    try:
        async with session.get(url, headers=headers, timeout=90) as r:
            r.raise_for_status()
            return await r.text()
    except Exception as e:
        logging.warning(f"Network fail GET TEXT {url}: {e}")
        return ""

class RateLimiter:
    def __init__(self, rps: float = 2.0):
        self.gap = 1.0 / max(float(rps), 0.1)
        self.t = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            sleep = max(0.0, self.t + self.gap - now)
            if sleep > 0:
                await asyncio.sleep(sleep)
            self.t = time.monotonic()

async def etherscan_get_source(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    chainid: int,
    address: str,
) -> Optional[dict]:
    """Etherscan v2 getsourcecode."""
    await limiter.wait()
    params = {
        "chainid": chainid,
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": ETHERSCAN_API_KEY,
    }
    res = await http_get_json(session, ETHERSCAN_BASE, params=params, quiet_404=True)
    if not isinstance(res, dict):
        return None
    if str(res.get("status")) != "1":
        return None
    result = res.get("result")
    if not isinstance(result, list) or not result:
        return None
    return result[0]

def is_verified_source(r0: dict) -> bool:
    """Heuristic: verified if SourceCode non-empty OR ABI not 'not verified'."""
    sc = (r0.get("SourceCode") or "").strip()
    abi = (r0.get("ABI") or "").strip()
    if sc and sc not in ("0x",):
        return True
    if abi and "not verified" not in abi.lower():
        return True
    return False

def candidate_chains(guess: Optional[str], proto_chains: List[str]) -> List[str]:
    """Try mined hint -> protocol chains -> global preference."""
    cands: List[str] = []
    g = norm_chain(guess or "")
    if g:
        cands.append(g)

    for ch in (proto_chains or []):
        nc = norm_chain(ch)
        if nc and nc not in cands:
            cands.append(nc)

    for ch in CHAIN_PREF:
        if ch not in cands:
            cands.append(ch)

    return cands

# ────────────────────────────────────────────────────────────────────────────────
# PROTOCOL SAMPLING
# ────────────────────────────────────────────────────────────────────────────────
async def get_protocols_stratified(
    session: aiohttp.ClientSession,
    top_n: int,
    allow_cats: List[str],
    allow_chains: List[str],
    per_cell: int = 8,
) -> List[dict]:
    """Stratified sampling across (category × primary_chain × TVL tier)."""
    raw = await http_get_json(session, LLAMA_PROTOCOLS_URL)
    if not raw:
        return []

    df = pd.DataFrame(raw)
    keep_cols = ["name", "symbol", "category", "tvl", "slug", "chains"]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    df["tvl"] = pd.to_numeric(df.get("tvl"), errors="coerce").fillna(0.0)
    df = df[df["slug"].notna()]

    if allow_cats:
        allow_set = set([c.strip() for c in allow_cats if c.strip()])
        df = df[df["category"].isin(allow_set)]

    def _primary_chain(chains):
        if isinstance(chains, list) and chains:
            for c in chains:
                nc = norm_chain(c)
                if nc:
                    return nc
        if isinstance(chains, str) and chains:
            return norm_chain(chains)
        return None

    df["primary_chain"] = df["chains"].apply(_primary_chain)
    df = df[df["primary_chain"].notna()]

    if allow_chains:
        allow_set = set([norm_chain(c) for c in allow_chains if norm_chain(c)])
        df = df[df["primary_chain"].isin(allow_set)]

    q80 = df["tvl"].quantile(0.80)
    q40 = df["tvl"].quantile(0.40)

    def _tier(x):
        if x >= q80:
            return "large"
        if x >= q40:
            return "mid"
        return "small"

    df["tvl_tier"] = df["tvl"].apply(_tier)

    sampled = []
    for (cat, ch, tier), g in df.groupby(["category", "primary_chain", "tvl_tier"], dropna=True):
        g2 = g.sort_values("tvl", ascending=False)
        sampled.append(g2.head(per_cell))

    out = pd.concat(sampled, ignore_index=True) if sampled else df
    out = out.sort_values("tvl", ascending=False)
    out = out.drop_duplicates(subset=["slug"], keep="first")
    out = out.head(int(top_n))
    return out.to_dict("records")

async def get_protocols_top(session: aiohttp.ClientSession, top_n: int, allow_cats: List[str]) -> List[dict]:
    raw = await http_get_json(session, LLAMA_PROTOCOLS_URL)
    if not raw:
        return []
    df = pd.DataFrame(raw)[["name", "symbol", "category", "tvl", "slug", "chains"]]
    if allow_cats:
        df = df[df["category"].isin(allow_cats)]
    return df.sort_values("tvl", ascending=False).head(int(top_n)).to_dict("records")

# ────────────────────────────────────────────────────────────────────────────────
# GITHUB MINING
# ────────────────────────────────────────────────────────────────────────────────
async def list_github_dir(session: aiohttp.ClientSession, owner: str, repo: str, subpath: str) -> List[dict]:
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{subpath}"
    res = await http_get_json(session, url, headers=gh_headers())
    return res if isinstance(res, list) else []

async def search_github_code(session: aiohttp.ClientSession, owner: str, repo: str, slug: str) -> List[dict]:
    q = f"repo:{owner}/{repo} {slug}"
    params = {"q": q, "per_page": 10}
    res = await http_get_json(session, GH_CODE_SEARCH, headers=gh_headers(), params=params, quiet_404=True)
    if not isinstance(res, dict) or not res.get("items"):
        return []
    items = res["items"]
    return [{"name": Path(i["path"]).name, "path": i["path"], "html_url": i.get("html_url", "")} for i in items]

def chain_hints(text: str) -> List[str]:
    hints = []
    for k in CHAIN_ALIASES.keys():
        if re.search(rf"\b{k}\b", text, re.IGNORECASE):
            hints.append(CHAIN_ALIASES[k])
    return list(set(hints))

async def safe_list_github_dir(session: aiohttp.ClientSession, owner: str, repo: str, path: str) -> List[dict]:
    try:
        return await list_github_dir(session, owner, repo, path)
    except Exception as e:
        logging.warning(f"⚠️ GitHub dir fetch failed {repo}/{path}: {e}")
        return []

async def safe_search_github_code(session: aiohttp.ClientSession, owner: str, repo: str, slug: str) -> List[dict]:
    try:
        return await search_github_code(session, owner, repo, slug)
    except Exception as e:
        logging.warning(f"⚠️ GitHub code search failed for {slug}: {e}")
        return []

async def mine_addresses_for_slug(session: aiohttp.ClientSession, slug: str) -> List[dict]:
    """Mine candidate addresses for a protocol slug from DefiLlama repos."""
    bag: List[dict] = []

    async def _consume_files(file_list: List[dict], owner: str, repo: str, subdesc: str):
        nonlocal bag
        for it in file_list:
            name = it.get("name", "")
            txt = ""
            if it.get("download_url"):
                txt = await http_get_text(session, it["download_url"], headers=gh_headers())
            else:
                path = it.get("path", "")
                if not path:
                    continue
                # Prefer main branch; fallback to master
                url_main = f"https://raw.githubusercontent.com/{owner}/{repo}/main/{path}"
                txt = await http_get_text(session, url_main, headers=gh_headers())
                if not txt:
                    url_master = f"https://raw.githubusercontent.com/{owner}/{repo}/master/{path}"
                    txt = await http_get_text(session, url_master, headers=gh_headers())

            if not txt:
                continue

            hints = chain_hints(txt)
            for m in ADDR_RE.finditer(txt):
                addr = m.group(0).lower()
                if addr == ZERO_ADDR:
                    continue
                bag.append(
                    {
                        "address": addr,
                        "guess": hints[0] if hints else None,
                        "context": f"{owner}/{repo}/{subdesc}/{name}",
                    }
                )

    # (1) DefiLlama adapters
    for owner, repo in GH_ADAPTER_DIRS:
        files = await safe_list_github_dir(session, owner, repo, f"projects/{slug}")
        await _consume_files(files, owner, repo, f"projects/{slug}")

    # (2) yield-server
    for owner, repo in GH_YIELD_DIRS:
        files = await safe_list_github_dir(session, owner, repo, f"projects/{slug}")
        await _consume_files(files, owner, repo, f"src/adaptors/{slug}")

    # (3) fallback code search
    if not bag:
        for owner, repo in GH_ADAPTER_DIRS + GH_YIELD_DIRS:
            results = await safe_search_github_code(session, owner, repo, slug)
            await _consume_files(results, owner, repo, f"search:{slug}")

    # Dedup by address
    seen = set()
    uniq = []
    for it in bag:
        a = it.get("address")
        if not a or a in seen:
            continue
        seen.add(a)
        uniq.append(it)

    return uniq

# ────────────────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────────────────
async def main() -> List[dict]:
    TOP = int(os.getenv("TOP_N_PROTOCOLS", "600"))
    CAP = int(os.getenv("PER_PROTOCOL_CAP", "400"))
    PROT_CONC = int(os.getenv("PROTOCOL_CONCURRENCY", "4"))
    RPS = float(os.getenv("ETHERSCAN_RPS", "2.0"))

    ALLOW_CATEGORIES = parse_csv_list(os.getenv("ALLOW_CATEGORIES", ""))
    ALLOW_CHAINS = parse_csv_list(os.getenv("ALLOW_CHAINS", ""))
    PER_CELL = int(os.getenv("STRATUM_PER_CELL", "8"))
    USE_STRATIFIED = os.getenv("USE_STRATIFIED", "1").strip().lower() in ("1", "true", "yes")

    contracts_csv = DATA_RAW / "contracts" / "verified_contracts_expanded.csv"
    checkpoint = OUT_DIR / "checkpoints_contracts.json"

    if contracts_csv.exists():
        logging.info(f"Resuming existing file: {contracts_csv}")

    state = {"done": []}
    if checkpoint.exists():
        try:
            state = json.loads(checkpoint.read_text(encoding="utf-8"))
            if not isinstance(state.get("done"), list):
                state = {"done": []}
        except Exception:
            state = {"done": []}

    limiter = RateLimiter(rps=RPS)
    rows: List[dict] = []

    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if USE_STRATIFIED:
            protos = await get_protocols_stratified(session, TOP, ALLOW_CATEGORIES, ALLOW_CHAINS, per_cell=PER_CELL)
        else:
            protos = await get_protocols_top(session, TOP, ALLOW_CATEGORIES)

        logging.info(f"Loaded {len(protos)} protocols to mine...")

        sem = asyncio.Semaphore(PROT_CONC)

        async def handle_protocol(p: dict) -> List[dict]:
            slug = p.get("slug")
            if not slug:
                return []
            if slug in state["done"]:
                logging.info(f"⏭️ Skipping {slug} (already in checkpoint)")
                return []

            async with sem:
                try:
                    logging.info(f"🧠 Mining protocol: {slug}")
                    mined = await mine_addresses_for_slug(session, slug)

                    # cap mined addresses (pre-verify)
                    if CAP and len(mined) > CAP:
                        mined = mined[:CAP]

                    # Save mined candidates
                    (ADDR_DIR / f"{slug}.json").write_text(json.dumps(mined, indent=2), encoding="utf-8")

                    verified_rows: List[dict] = []
                    proto_chain_list = p.get("chains", []) or []

                    for m in mined:
                        addr = m.get("address")
                        if not addr:
                            continue

                        picked_chain = None
                        meta = None
                        for ch in candidate_chains(m.get("guess"), proto_chain_list):
                            cid = CHAIN_IDS.get(ch)
                            if not cid:
                                continue
                            r0 = await etherscan_get_source(session, limiter, cid, addr)
                            if not r0:
                                continue
                            if not is_verified_source(r0):
                                continue
                            picked_chain = ch
                            meta = r0
                            break

                        if not picked_chain or not meta:
                            continue

                        verified_rows.append(
                            {
                                "slug": slug,
                                "address": addr,
                                "chain": picked_chain,
                                "category": p.get("category"),
                                "tvl": p.get("tvl"),
                                "chains": ";".join(p.get("chains", []) or []),
                                "scrape_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "context": m.get("context"),
                                "contract_name": meta.get("ContractName"),
                                "compiler_version": meta.get("CompilerVersion"),
                            }
                        )

                    state["done"].append(slug)
                    checkpoint.write_text(json.dumps(state), encoding="utf-8")

                    logging.info(f"✅ Finished {slug} — mined {len(mined)} addresses, verified {len(verified_rows)}")
                    return verified_rows

                except Exception as e:
                    logging.error(f"❌ Error mining {slug}: {e}")
                    return []

        tasks = [handle_protocol(p) for p in protos]
        results = await asyncio.gather(*tasks)
        rows = [r for batch in results for r in batch]

    return rows

if __name__ == "__main__":
    async def runner():
        rows = await main()

        contracts_csv = ROOT / "data_raw" / "contracts" / "verified_contracts_expanded.csv"

        if not rows:
            logging.warning("⚠️ No new verified contracts found in this run.")
            return

        df_new = pd.DataFrame(rows)

        if contracts_csv.exists():
            df_existing = pd.read_csv(contracts_csv, encoding="utf-8-sig")
            combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            combined = df_new

        # Dedup by protocol + address + chain
        combined.drop_duplicates(subset=["slug", "address", "chain"], inplace=True)
        combined.to_csv(contracts_csv, index=False)

        logging.info(f"🧩 Appended {len(df_new)} new rows → total {len(combined)} rows → {contracts_csv}")

    asyncio.run(runner())