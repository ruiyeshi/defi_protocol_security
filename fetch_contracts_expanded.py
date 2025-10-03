
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
print("🚀 Script started... loading .env and initializing asyncio")
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

logging.basicConfig(
    filename=LOGS / "fetch_contracts_expanded.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger("").addHandler(console)

# ────────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────────────────────────────────────
LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"

# where DefiLlama keeps adapters
GH_ADAPTER_DIRS: List[Tuple[str, str]] = [
    ("DefiLlama", "DefiLlama-Adapters"),  # /projects/<slug> (many but not all)
]
GH_YIELD_DIRS: List[Tuple[str, str]] = [
    ("DefiLlama", "yield-server"),        # /src/adaptors/<slug> (perps / yield / lend)
]

# GitHub code search fallback (find *any* file that mentions the slug)
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
                if r.status in (429, 500, 502, 503):
                    await asyncio.sleep(2**i)
                    continue
                r.raise_for_status()
                if "application/json" in r.headers.get("Content-Type", ""):
                    return await r.json()
                return await r.text()
        except Exception as e:
            await asyncio.sleep(1.5 * (i+1))
    return None

async def http_get_text(session: aiohttp.ClientSession, url: str, *, headers=None):
    try:
        async with session.get(url, headers=headers, timeout=60) as r:
            r.raise_for_status()
            return await r.text()
    except Exception:
        return ""

# ────────────────────────────────────────────────────────────────────────────────
# GITHUB MINING (adapters + code search fallback)
# ────────────────────────────────────────────────────────────────────────────────
async def list_github_dir(session, owner, repo, subpath) -> List[dict]:
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{subpath}"
    res = await http_get_json(session, url, headers=gh_headers())
    return res if isinstance(res, list) else []

async def search_github_code(session, owner, repo, slug) -> List[dict]:
    """Fallback: search files that contain the slug string anywhere in the repo."""
    q = f"repo:{owner}/{repo} {slug}"
    params = {"q": q, "per_page": 10}
    res = await http_get_json(session, GH_CODE_SEARCH, headers=gh_headers(), params=params, quiet_404=True)
    if not isinstance(res, dict) or not res.get("items"):
        return []
    items = res["items"]
    # Normalize to {name, html_url, path, url, ...} like contents API-ish
    return [{"name": Path(i["path"]).name, "path": i["path"], "html_url": i.get("html_url", "")} for i in items]

def mine_addresses(txt: str) -> List[str]:
    return list({m.group(0).lower() for m in ADDR_RE.finditer(txt)})

def chain_hints(txt: str) -> List[str]:
    found=set()
    for w in CHAIN_ALIASES:
        if re.search(rf"\b{re.escape(w)}\b", txt, re.IGNORECASE):
            found.add(CHAIN_ALIASES[w])
    return list(found)

async def mine_addresses_for_slug(session, slug: str) -> List[dict]:
    """Try dedicated adapter folders first; if nothing, code-search fallback."""
    bag: List[dict] = []

    async def _consume_files(file_list: List[dict], owner: str, repo: str, subdesc: str):
        nonlocal bag
        for it in file_list:
            name = it.get("name", "")
            # fetch raw (contents API gives "download_url"; code-search does not)
            if "download_url" in it:
                txt = await http_get_text(session, it["download_url"], headers=gh_headers())
            else:
                # code search result → fetch raw via contents API path
                path = it.get("path", "")
                if not path:
                    continue
                url = f"https://raw.githubusercontent.com/{owner}/{repo}/master/{path}"
                txt = await http_get_text(session, url, headers=gh_headers())

            if not txt:
                continue
            for addr in mine_addresses(txt):
                if addr == "0x0000000000000000000000000000000000000000":
                    continue
                hints = chain_hints(txt)
                bag.append({
                    "address": addr,
                    "guess"  : hints[0] if hints else None,
                    "context": f"{owner}/{repo}/{subdesc}/{name}"
                })

    # Try DefiLlama adapters: projects/<slug>
    for owner, repo in GH_ADAPTER_DIRS:
        files = await list_github_dir(session, owner, repo, f"projects/{slug}")
        await _consume_files(files, owner, repo, f"projects/{slug}")

    # Try yield-server: src/adaptors/<slug>
    for owner, repo in GH_YIELD_DIRS:
        files = await list_github_dir(session, owner, repo, f"src/adaptors/{slug}")
        await _consume_files(files, owner, repo, f"src/adaptors/{slug}")

    if not bag:
        # Fallback: code search
        for owner, repo in GH_ADAPTER_DIRS + GH_YIELD_DIRS:
            results = await search_github_code(session, owner, repo, slug)
            await _consume_files(results, owner, repo, f"search:{slug}")

    return bag

# ────────────────────────────────────────────────────────────────────────────────
# ETHERSCAN (proxy-aware)
# ────────────────────────────────────────────────────────────────────────────────
# ---------- Etherscan unified (V2 compatible) ----------
async def etherscan_getsourcecode(session, chain_id: int, address: str):
    """
    Use Etherscan API v2 format for multi-chain support.
    Falls back to v1 endpoint if v2 fails.
    """
    base_v2 = os.getenv("ETHERSCAN_V2_URL", "https://api.etherscan.io/v2/api")
    base_v1 = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/api")

    params_v2 = {
        "chainid": chain_id,
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": ETHERSCAN_API_KEY
    }

    try:
        async with session.get(base_v2, params=params_v2, timeout=45) as r:
            if r.status == 200:
                data = await r.json()
                if isinstance(data, dict) and data.get("status") == "1":
                    return data
                elif "deprecated" in (data.get("result") or "").lower():
                    logging.warning("⚠️ V2 endpoint deprecated notice, retrying v1...")
                else:
                    return data
    except Exception as e:
        logging.warning(f"V2 Etherscan fail {address}: {e}")

    # fallback to v1
    params_v1 = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": ETHERSCAN_API_KEY
    }
    try:
        async with session.get(base_v1, params=params_v1, timeout=45) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logging.warning(f"V1 Etherscan fail {address}: {e}")
    return None

async def resolve_verified_source(session, chain: str, address: str) -> Optional[dict]:
    """Return a verified source record (following proxies)."""
    chain_id = CHAIN_IDS[chain]
    j = await etherscan_getsourcecode(session, chain_id, address)
    if not j:
        return None

    res = j.get("result")
    if not isinstance(res, list) or not res:
        return None
    r0 = res[0]

    # Direct verified?
    src = (r0.get("SourceCode") or "").strip()
    message = str(j.get("message", "")).upper()
    abi_str = str(r0.get("ABI", ""))

    if src and "NOT VERIFIED" not in abi_str.upper():
        r0["_resolved_address"] = address
        return r0

    # Proxy → follow Implementation
    if r0.get("Proxy") in ("1", 1, True):
        impl = (r0.get("Implementation") or "").strip()
        if impl and impl.lower().startswith("0x"):
            j2 = await etherscan_getsourcecode(session, chain_id, impl)
            if isinstance(j2, dict) and isinstance(j2.get("result"), list) and j2["result"]:
                r2 = j2["result"][0]
                src2 = (r2.get("SourceCode") or "").strip()
                abi2 = str(r2.get("ABI", ""))
                if src2 and "NOT VERIFIED" not in abi2.upper():
                    r2["_resolved_address"] = impl
                    r2["_proxy_address"]    = address
                    return r2

    # Not verified
    return None

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
# MAIN
# ────────────────────────────────────────────────────────────────────────────────
async def get_top_protocols(session, top_n, allow_cats):
    raw = await http_get_json(session, LLAMA_PROTOCOLS_URL)
    df = pd.DataFrame(raw)[["name", "symbol", "category", "tvl", "slug", "chains"]]
    if allow_cats:
        df = df[df["category"].isin(allow_cats)]
    return df.sort_values("tvl", ascending=False).head(top_n).to_dict("records")

async def main():
    # Small defaults so you can test quickly, then scale up:
    TOP = int(os.getenv("TOP_N_PROTOCOLS", "40"))        # try 40 first
    CAP = int(os.getenv("PER_PROTOCOL_CAP", "120"))      # keep it moderate
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

        for p in protos:
            slug = p["slug"]
            if slug in state["done"]:
                print(f"⏭️  Skipping mined {slug}")
                addr_file = ADDR_DIR / f"{slug}.json"
                addrs = json.loads(addr_file.read_text()) if addr_file.exists() else []
            else:
                mined = await mine_addresses_for_slug(session, slug)
                # De-dupe + cap
                seen = set()
                addrs = []
                for it in mined:
                    a = it["address"].lower()
                    if a in seen:
                        continue
                    seen.add(a); addrs.append(it)
                    if len(addrs) >= CAP:
                        break
                (ADDR_DIR / f"{slug}.json").write_text(json.dumps(addrs, indent=2))
                state["done"].append(slug)
                checkpoint.write_text(json.dumps(state))

            if not addrs:
                continue

            # probable chains for each mined address
            for it in addrs:
                a = it["address"].lower()
                # prefer hints, else use protocol’s listed chains, else ethereum
                proto_chains = [norm_chain(c) for c in (p.get("chains") or [])]
                proto_chains = [c for c in proto_chains if c in CHAIN_IDS]
                candidates = []
                if it.get("guess") in CHAIN_IDS:
                    candidates = [it["guess"]]
                elif proto_chains:
                    candidates = proto_chains[:3]  # try a few
                else:
                    candidates = ["ethereum"]

                verified = None
                for ch in candidates:
                    await limiter.wait()
                    rec = await resolve_verified_source(session, ch, a)
                    if rec:
                        verified = (ch, rec)
                        break

                if not verified:
                    # keep the log light; comment the next line to silence
                    # logging.info(f"no verified src for {a} (slug={slug})")
                    continue

                ch, rec = verified
                src = (rec.get("SourceCode") or "")
                rows.append({
                    "protocol": p["name"],
                    "category": p["category"],
                    "tvl": p["tvl"],
                    "slug": slug,
                    "chain": ch,
                    "chain_id": CHAIN_IDS[ch],
                    "contract_address": rec.get("_resolved_address", a).lower(),
                    "proxy_address": rec.get("_proxy_address", ""),
                    "contract_name": rec.get("ContractName", ""),
                    "compiler": rec.get("CompilerVersion", ""),
                    "source_code": src[:200000],  # cap to keep csv workable
                })

    if rows:
        df = pd.DataFrame(rows).drop_duplicates(subset=["chain","contract_address"])
        df.to_csv(contracts_csv, index=False)
        print(f"✅ wrote {contracts_csv} with {len(df)} rows")
    else:
        print("⚠️ No verified contracts found")

if __name__ == "__main__":
    asyncio.run(main())