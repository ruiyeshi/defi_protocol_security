#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import csv
import asyncio
import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from dotenv import load_dotenv

# ────────────────────────────────────────────────────────────────────────────────
# Paths + ENV
# ────────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

DATA_RAW = ROOT / "data_raw"
LOGS = ROOT / "logs"
OUT_DIR = ROOT / "outputs"
REPOS_DIR = ROOT / "repos"

for p in [DATA_RAW, LOGS, OUT_DIR, DATA_RAW / "contracts", REPOS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
if not ETHERSCAN_API_KEY:
    raise SystemExit("Missing ETHERSCAN_API_KEY in .env")

ETHERSCAN_BASE_URL = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/api").strip()
LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"

TOP_N_PROTOCOLS = int(os.getenv("TOP_N_PROTOCOLS", "2000"))
PROTOCOL_CONCURRENCY = int(os.getenv("PROTOCOL_CONCURRENCY", "6"))
ETHERSCAN_RPS = float(os.getenv("ETHERSCAN_RPS", "3.0"))

# Category filter
_raw_cats = (os.getenv("ALLOW_CATEGORIES", "All") or "All").strip()
ALLOW_CATEGORIES = _raw_cats

# Output files
CONTRACTS_CSV_LOCAL = DATA_RAW / "contracts" / "verified_contracts_local.csv"
CHECKPOINT = OUT_DIR / "checkpoints_contracts_local.json"

log_file_path = LOGS / "fetch_contracts_local.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ],
)

ADDR_RE = re.compile(r"0x[a-fA-F0-9]{40}")
ZERO_ADDR = "0x0000000000000000000000000000000000000000"

# EVM chain IDs supported by Etherscan v1 style (via chainid parameter on some setups)
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

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def run(cmd: List[str], cwd: Optional[Path] = None) -> str:
    return subprocess.check_output(cmd, cwd=str(cwd) if cwd else None, text=True, stderr=subprocess.DEVNULL)

def ensure_repo(local_path: Path, git_url: str, name: str) -> None:
    if local_path.exists() and (local_path / ".git").exists():
        logging.info(f"✅ Repo exists: {name}, pulling latest…")
        subprocess.run(["git", "-C", str(local_path), "pull", "--ff-only"], check=False)
        return
    logging.info(f"⬇️ Cloning {name}…")
    subprocess.run(["git", "clone", "--depth", "1", git_url, str(local_path)], check=True)

class RateLimiter:
    def __init__(self, rps: float):
        self.gap = 1.0 / max(rps, 0.1)
        self._t = 0.0
        self._lock = asyncio.Lock()

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            sleep = max(0.0, self._t + self.gap - now)
            if sleep > 0:
                await asyncio.sleep(sleep)
            self._t = time.monotonic()

def append_rows_csv(path: Path, rows: List[dict]) -> None:
    """Append rows immediately so progress is never lost on Ctrl+C."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if not file_exists:
            w.writeheader()
        w.writerows(rows)
        f.flush()

def load_checkpoint() -> dict:
    if CHECKPOINT.exists():
        try:
            return json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        except Exception:
            return {"done": []}
    return {"done": []}

def save_checkpoint(state: dict) -> None:
    CHECKPOINT.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def mine_addresses_from_dir(repo_path: Path, rel_dir: Path) -> List[str]:
    """Scan a specific directory for EVM addresses using ripgrep."""
    d = repo_path / rel_dir
    if not d.exists():
        return []
    try:
        out = run(["rg", "-n", "0x[a-fA-F0-9]{40}", str(d)], cwd=repo_path)
    except Exception:
        return []
    addrs = {m.group(0).lower() for m in ADDR_RE.finditer(out)}
    addrs.discard(ZERO_ADDR)
    return list(addrs)

def mine_addresses_for_slug(adapters_repo: Path, yield_repo: Path, slug: str) -> List[str]:
    """High-recall mining: scan canonical folders first, fallback to slug grep."""
    bag = set()

    # DefiLlama-Adapters: projects/<slug>/
    for a in mine_addresses_from_dir(adapters_repo, Path("projects") / slug):
        bag.add(a)

    # yield-server: src/adaptors/<slug>/ (sometimes)
    for a in mine_addresses_from_dir(yield_repo, Path("src") / "adaptors" / slug):
        bag.add(a)

    # fallback: grep slug in repos, then extract addresses from matched output
    if not bag:
        for repo_path in [adapters_repo, yield_repo]:
            try:
                out = run(["rg", "-n", "-i", slug, str(repo_path)], cwd=repo_path)
            except Exception:
                continue
            for m in ADDR_RE.finditer(out):
                addr = m.group(0).lower()
                if addr != ZERO_ADDR:
                    bag.add(addr)

    return list(bag)

async def http_get_json(session: aiohttp.ClientSession, url: str, *, params: dict) -> Optional[dict]:
    try:
        async with session.get(url, params=params, timeout=45) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None

async def is_verified(session: aiohttp.ClientSession, limiter: RateLimiter, address: str, chain_id: int) -> bool:
    await limiter.wait()
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": ETHERSCAN_API_KEY,
        "chainid": chain_id,
    }
    res = await http_get_json(session, ETHERSCAN_BASE_URL, params=params)
    if not res or "result" not in res:
        return False
    try:
        result = res["result"]
        if isinstance(result, list) and len(result) > 0:
            # Verified contracts usually have non-empty SourceCode
            sc = (result[0].get("SourceCode") or "")
            return len(sc.strip()) > 0
    except Exception:
        return False
    return False

async def get_protocols(session: aiohttp.ClientSession) -> List[dict]:
    raw = await http_get_json(session, LLAMA_PROTOCOLS_URL, params={})
    if not raw:
        return []
    df = pd.DataFrame(raw)[["name", "symbol", "category", "tvl", "slug", "chains"]]

    if ALLOW_CATEGORIES.lower() != "all":
        allow = [x.strip() for x in ALLOW_CATEGORIES.split(",") if x.strip()]
        if allow:
            df = df[df["category"].isin(allow)]

    df = df.sort_values("tvl", ascending=False).head(TOP_N_PROTOCOLS)
    return df.to_dict("records")

# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
async def main():
    adapters_repo = REPOS_DIR / "DefiLlama-Adapters"
    yield_repo = REPOS_DIR / "yield-server"

    ensure_repo(adapters_repo, "https://github.com/DefiLlama/DefiLlama-Adapters.git", "DefiLlama-Adapters")
    ensure_repo(yield_repo, "https://github.com/DefiLlama/yield-server.git", "yield-server")

    state = load_checkpoint()
    done = set(state.get("done", []))

    limiter = RateLimiter(rps=ETHERSCAN_RPS)
    sem = asyncio.Semaphore(PROTOCOL_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        protos = await get_protocols(session)
        logging.info(f"Loaded {len(protos)} protocols (top {TOP_N_PROTOCOLS}).")

        async def handle(p: dict):
            slug = p.get("slug")
            if not slug or slug in done:
                return

            async with sem:
                mined = mine_addresses_for_slug(adapters_repo, yield_repo, slug)
                verified_rows: List[dict] = []

                # Try verification across EVM chains listed by DeFiLlama
                proto_chains = [str(c).lower() for c in (p.get("chains") or [])]

                # Map proto chains to known EVM chain ids (rough matching)
                evm_candidates = []
                for ch in proto_chains:
                    if ch in CHAIN_IDS:
                        evm_candidates.append(ch)
                # If none, still try ethereum as a fallback (some protocols omit chains)
                if not evm_candidates:
                    evm_candidates = ["ethereum"]

                verified_count = 0
                for addr in mined:
                    ok_any = False
                    for ch in evm_candidates:
                        cid = CHAIN_IDS.get(ch)
                        if not cid:
                            continue
                        ok = await is_verified(session, limiter, addr, cid)
                        if ok:
                            ok_any = True
                            verified_count += 1
                            # store the first successful chain as `guess`
                            verified_rows.append({
                                "slug": slug,
                                "address": addr,
                                "guess": ch,
                                "category": p.get("category"),
                                "tvl": p.get("tvl"),
                                "chains": ";".join(p.get("chains") or []),
                                "scrape_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                            })
                            break

                    # If you also want to keep unverified addresses, you can append them too.
                    # For now we store only verified to match your previous schema.

                # Write progress immediately
                append_rows_csv(CONTRACTS_CSV_LOCAL, verified_rows)

                # checkpoint regardless of success
                done.add(slug)
                state["done"] = sorted(list(done))
                save_checkpoint(state)

                logging.info(f"✅ Finished {slug} — mined {len(mined)} addrs, verified {verified_count}")

        await asyncio.gather(*[handle(p) for p in protos])

if __name__ == "__main__":
    asyncio.run(main())
