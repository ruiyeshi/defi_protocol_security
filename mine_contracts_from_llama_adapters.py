#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mine_contracts_from_llama_adapters.py

Goal:
  Build a large seed pool of real DeFi protocol contract addresses by mining
  DeFiLlama TVL adapter source files from GitHub:
    https://github.com/DefiLlama/DefiLlama-Adapters

Inputs:
  data_raw/contracts/defillama_top_protocols.csv (or llama_protocols.csv style)
    Accepted columns (any subset ok):
      - slug  (preferred)
      - name OR protocol OR protocol_name (fallback label)
      - category (optional)
      - chains (optional; often ';' separated)
      - tvl (optional; used for top-N selection)

Outputs:
  data_raw/contracts/master_contracts_llama_adapters.csv
    columns: slug, protocol, chain, address, category, source, evidence_url, notes

  Optionally also merges into:
  data_raw/contracts/master_contracts.csv   (if it exists)

Notes:
  - Uses GitHub API to get the repository tree once (fast matching).
  - Uses simple regex to extract addresses (0x...40 hex).
  - Attempts to infer chain from nearby "ethereum:", "bsc:", etc.
  - Uses caching under outputs/_cache/llama_adapters/ to reduce requests.
"""

from __future__ import annotations

import os
import re
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# -----------------------
# Config / Paths
# -----------------------
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

INFILE = ROOT / "data_raw" / "contracts" / "defillama_top_protocols.csv"
OUT = ROOT / "data_raw" / "contracts" / "master_contracts_llama_adapters.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

MERGE_MASTER = ROOT / "data_raw" / "contracts" / "master_contracts.csv"

CACHE_DIR = ROOT / "outputs" / "_cache" / "llama_adapters"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

REPO = "DefiLlama/DefiLlama-Adapters"

TREE_APIS = [
    f"https://api.github.com/repos/{REPO}/git/trees/main?recursive=1",
    f"https://api.github.com/repos/{REPO}/git/trees/master?recursive=1",
]

RAW_BASES = [
    f"https://raw.githubusercontent.com/{REPO}/main/",
    f"https://raw.githubusercontent.com/{REPO}/master/",
]

# Tuning
TOP_N = int(os.getenv("TOP_N_PROTOCOLS", "2000"))
SLEEP_SEC = float(os.getenv("GITHUB_SLEEP_SEC", "0.2"))

# Matching / mining behavior
MAX_CANDIDATE_FILES_PER_PROTOCOL = int(os.getenv("MAX_CANDIDATE_FILES_PER_PROTOCOL", "6"))
MINE_ALL_ADAPTERS = os.getenv("MINE_ALL_ADAPTERS", "0").strip() in {"1", "true", "True"}
# When mining all adapters, cap how many files we fetch to avoid runaway runs
MAX_ADAPTER_FILES_TOTAL = int(os.getenv("MAX_ADAPTER_FILES_TOTAL", "6000"))

# -----------------------
# Helpers
# -----------------------

ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")

# common chain keys that appear in adapters

KNOWN_CHAINS = [
    "ethereum", "arbitrum", "optimism", "base", "avalanche", "polygon", "bsc",
    "fantom", "celo", "gnosis", "linea", "scroll", "zksync", "starknet",
    "mantle", "metis", "blast", "sei", "near", "solana", "tron", "bitcoin",
]

# normalize common chain synonyms / tokens to your vocabulary
CHAIN_ALIASES: Dict[str, str] = {
    "eth": "ethereum",
    "mainnet": "ethereum",
    "ethereum-mainnet": "ethereum",
    "arbitrum one": "arbitrum",
    "arb": "arbitrum",
    "arbitrum-one": "arbitrum",
    "op": "optimism",
    "optimistic": "optimism",
    "optimistic-ethereum": "optimism",
    "avax": "avalanche",
    "avalanche-c": "avalanche",
    "matic": "polygon",
    "polygon-pos": "polygon",
    "bnb": "bsc",
    "binance": "bsc",
    "binance-smart-chain": "bsc",
    "bscmainnet": "bsc",
    "ftm": "fantom",
    "xdai": "gnosis",
    "gnosis-chain": "gnosis",
    "zksync-era": "zksync",
}

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace(" ", "-").replace("_", "-")
    # keep only [a-z0-9-]
    s = re.sub(r"[^a-z0-9-]", "", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s

def req_json(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=40)
    if r.status_code in (403, 429):
        # backoff + retry once
        time.sleep(6)
        r = requests.get(url, headers=HEADERS, timeout=40)
    r.raise_for_status()
    return r.json()

def req_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=40)
    if r.status_code in (403, 429):
        time.sleep(6)
        r = requests.get(url, headers=HEADERS, timeout=40)
    r.raise_for_status()
    return r.text

def load_tree_index() -> List[str]:
    cache = CACHE_DIR / "tree.json"
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))

    last_err = None
    for tree_api in TREE_APIS:
        try:
            j = req_json(tree_api)
            paths = []
            for it in j.get("tree", []):
                p = it.get("path", "")
                if p.startswith("projects/") and (p.endswith(".js") or p.endswith(".ts") or p.endswith(".mjs")):
                    paths.append(p)
            cache.write_text(json.dumps(paths, ensure_ascii=False, indent=2), encoding="utf-8")
            return paths
        except Exception as e:
            last_err = e

    raise SystemExit(f"Failed to fetch repo tree from GitHub. Last error: {last_err}")

def candidate_paths_for_key(key: str) -> List[str]:
    """Common adapter conventions in DefiLlama-Adapters."""
    k = norm(key)
    if not k:
        return []
    return [
        f"projects/{k}.js",
        f"projects/{k}.ts",
        f"projects/{k}/index.js",
        f"projects/{k}/index.ts",
    ]

def best_match_paths(slug: str, name: str, all_paths: List[str]) -> List[str]:
    """
    Return a ranked list of candidate adapter file paths.
    Strategy:
      1) exact expected paths for slug
      2) exact expected paths for name
      3) basename equals key (projects/<key>.js|.ts)
      4) contains matches in path (folder/file)
    """
    set_paths = set(all_paths)
    slug_k = norm(slug)
    name_k = norm(name)
    keys = [k for k in [slug_k, name_k] if k]

    ranked: List[str] = []

    # (1) + (2) exact expected
    for k in keys:
        for p in candidate_paths_for_key(k):
            if p in set_paths and p not in ranked:
                ranked.append(p)

    # (3) basename equals
    for k in keys:
        for ext in (".js", ".ts"):
            p = f"projects/{k}{ext}"
            if p in set_paths and p not in ranked:
                ranked.append(p)

    # (4) contains matches
    if keys:
        hits = []
        for p in all_paths:
            lp = p.lower()
            for k in keys:
                if (f"/{k}/" in lp) or lp.endswith(f"/{k}.js") or lp.endswith(f"/{k}.ts"):
                    hits.append(p)
                    break
        # prefer shorter paths first
        hits = sorted(set(hits), key=len)
        for p in hits:
            if p not in ranked:
                ranked.append(p)

    return ranked

def infer_chain_from_context(text: str, addr: str) -> str:
    """Infer chain for an address using nearby context."""
    if not text or not addr:
        return ""

    lower = text.lower()
    addr_l = addr.lower()

    def _norm_chain_token(tok: str) -> str:
        tok = (tok or "").strip().lower()
        tok = CHAIN_ALIASES.get(tok, tok)
        return tok if tok in KNOWN_CHAINS else ""

    # Split into lines and find the first line that contains the address
    lines = lower.splitlines()
    hit_idx = None
    for i, ln in enumerate(lines):
        if addr_l in ln:
            hit_idx = i
            break

    # Scan a window of lines around the address
    if hit_idx is not None:
        start = max(0, hit_idx - 60)
        end = min(len(lines), hit_idx + 25)

        # A) explicit: chain: "ethereum"
        for j in range(start, end):
            m = re.search(r'chain\s*:\s*["\']([a-z0-9\-]+)["\']', lines[j])
            if m:
                c = _norm_chain_token(m.group(1))
                if c:
                    return c

        # B) find the nearest preceding chain key like: ethereum: {  or  "ethereum": {
        key_pat = re.compile(r'^\s*["\']?([a-z0-9\-]+)["\']?\s*:\s*\{')
        for j in range(hit_idx, start - 1, -1):
            m = key_pat.search(lines[j])
            if m:
                c = _norm_chain_token(m.group(1))
                if c:
                    return c

        # C) forward scan (sometimes comes after)
        for j in range(hit_idx, end):
            m = key_pat.search(lines[j])
            if m:
                c = _norm_chain_token(m.group(1))
                if c:
                    return c

        # D) same-line quick hit
        ln = lines[hit_idx]
        for c in KNOWN_CHAINS:
            if re.search(rf"(^|[^a-z0-9]){re.escape(c)}\s*[:\"']", ln):
                return c

    # Fallback: broader character window
    idx = lower.find(addr_l)
    if idx != -1:
        window = lower[max(0, idx - 1500): min(len(lower), idx + 1500)]
        for c in KNOWN_CHAINS:
            if re.search(rf"(^|[^a-z0-9]){re.escape(c)}\s*[:\"']", window):
                return c

        m = re.search(r'chain\s*:\s*["\']([a-z0-9\-]+)["\']', window)
        if m:
            c = _norm_chain_token(m.group(1))
            if c:
                return c

    return ""

def infer_chain_from_path(path: str) -> str:
    p = (path or "").lower()

    # explicit synonyms
    if "/bsc/" in p or "binance" in p or "bep20" in p:
        return "bsc"
    if "polygon" in p or "/matic/" in p:
        return "polygon"
    if "optimism" in p or "/op/" in p:
        return "optimism"

    candidates = [
        "ethereum", "arbitrum", "base", "polygon", "bsc", "optimism",
        "avalanche", "fantom", "celo", "linea", "scroll", "mantle",
        "blast", "metis", "sei",
    ]

    for c in candidates:
        if f"/{c}/" in p or f"_{c}." in p or f"-{c}." in p:
            return c
        if p.endswith(f"/{c}.ts") or p.endswith(f"/{c}.js") or p.endswith(f"/{c}.mjs"):
            return c
    return ""

def fetch_adapter_source(path_in_repo: str) -> str:
    safe = path_in_repo.replace("/", "__")
    cache = CACHE_DIR / f"{safe}.txt"
    if cache.exists():
        return cache.read_text(encoding="utf-8", errors="ignore")

    last_err = None
    for raw_base in RAW_BASES:
        url = raw_base + path_in_repo
        try:
            txt = req_text(url)
            cache.write_text(txt, encoding="utf-8")
            time.sleep(SLEEP_SEC)
            return txt
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Failed to fetch adapter file {path_in_repo}. Last error: {last_err}")

def main():
    if not INFILE.exists():
        raise SystemExit(f"Missing input: {INFILE}")

    proto = pd.read_csv(INFILE)

    # Normalize input columns
    cols = [c.strip() for c in proto.columns]
    proto.columns = cols

    # Resolve a human label column
    label_col = None
    for c in ["name", "protocol", "protocol_name"]:
        if c in proto.columns:
            label_col = c
            break

    if "slug" not in proto.columns:
        if label_col is not None:
            proto["slug"] = proto[label_col].astype(str).map(norm)
        else:
            raise SystemExit("Input must contain 'slug' or one of: name/protocol/protocol_name")

    if label_col is None:
        proto["name"] = proto["slug"]
        label_col = "name"
    elif "name" not in proto.columns:
        proto["name"] = proto[label_col]

    if "category" not in proto.columns:
        proto["category"] = ""

    if "chains" not in proto.columns:
        proto["chains"] = ""

    # take top N by tvl if available
    if "tvl" in proto.columns:
        proto = proto.sort_values("tvl", ascending=False)

    proto = proto.head(TOP_N).copy()

    print(f"Protocols considered: {len(proto)} (TOP_N={TOP_N})")

    all_paths = load_tree_index()
    print(f"Adapter candidate files in repo tree: {len(all_paths)}")

    if MINE_ALL_ADAPTERS:
        # Mine every adapter file (projects/*.js|.ts) and set protocol_guess from path.
        # Mapping to llama slugs can be done later.
        rows = []
        paths = all_paths[:MAX_ADAPTER_FILES_TOTAL]
        print(f"MINE_ALL_ADAPTERS=1 -> fetching up to {len(paths)} adapter files")

        for idx, p in enumerate(paths, start=1):
            try:
                src = fetch_adapter_source(p)
            except Exception as e:
                if idx % 200 == 0:
                    print(f"fetch_failures so far (sample): {e}")
                continue

            addrs = sorted(set(m.group(0).lower() for m in ADDRESS_RE.finditer(src)))
            if not addrs:
                continue

            # protocol guess = folder or filename under projects/
            # examples: projects/aave/index.js -> aave ; projects/uniswap.js -> uniswap
            proto_guess = p.replace("projects/", "")
            proto_guess = re.sub(r"(/index)?\.(js|ts)$", "", proto_guess)
            proto_guess = proto_guess.split("/")[0]

            evidence_url = RAW_BASES[0] + p

            for a in addrs:
                # Best ROI: infer chain from filepath first; then refine from text context
                ch = infer_chain_from_path(p)
                if not ch:
                    ch = infer_chain_from_context(src, a)

                rows.append({
                    "slug": "",
                    "protocol": proto_guess,
                    "chain": ch,
                    "address": a,
                    "category": "",
                    "source": "defillama_adapter_all",
                    "evidence_url": evidence_url,
                    "notes": f"adapter_path={p}",
                })

            if idx % 200 == 0:
                print(f"files_seen={idx} | rows_so_far={len(rows)}")

        out = pd.DataFrame(rows)
        out["chain"] = out["chain"].fillna("").astype(str).str.strip().str.lower()
        out["address"] = out["address"].fillna("").astype(str).str.strip().str.lower()
        out = out[out["address"].str.startswith("0x") & (out["address"].str.len() == 42)].copy()

        # keep unique chain+address when chain known; also keep unknown chain (but won't verify)
        out = out.drop_duplicates(subset=["chain", "address", "protocol"], keep="first")

        out.to_csv(OUT, index=False)

        uniq = out[["chain", "address"]].drop_duplicates().shape[0] if not out.empty else 0
        print(f"✅ Wrote: {OUT} | rows={len(out)} | unique(chain,address)={uniq}")
        print(out["chain"].replace("", "(blank)").value_counts().head(20).to_string())
        return

    rows = []
    matched = 0
    nofile = 0

    for i, r in proto.iterrows():
        slug = str(r.get("slug", "") or "").strip()
        name = str(r.get("name", "") or slug).strip()
        category = str(r.get("category", "") or "").strip()
        chains = str(r.get("chains", "") or "").strip()

        if not slug:
            continue

        cands = best_match_paths(slug, name, all_paths)
        if not cands:
            nofile += 1
            continue

        matched += 1
        # try up to N candidate files until we get addresses
        src = ""
        used_path = None
        addrs = []
        for p_try in cands[:MAX_CANDIDATE_FILES_PER_PROTOCOL]:
            try:
                src_try = fetch_adapter_source(p_try)
            except Exception:
                continue
            addrs_try = sorted(set(m.group(0).lower() for m in ADDRESS_RE.finditer(src_try)))
            if addrs_try:
                src = src_try
                used_path = p_try
                addrs = addrs_try
                break

        if not used_path or not addrs:
            continue

        evidence_url = RAW_BASES[0] + used_path

        # infer chain per address if possible; else fallback to any chain list from input
        chain_list = []
        if chains:
            chain_list = [x.strip().lower() for x in re.split(r"[;,]", chains) if x.strip()]

        for a in addrs:
            ch = infer_chain_from_context(src, a)
            if not ch:
                ch = infer_chain_from_path(used_path)
            if not ch and chain_list:
                # if only one chain listed, assign it
                if len(chain_list) == 1:
                    ch = chain_list[0]

            rows.append({
                "slug": slug,
                "protocol": name,
                "chain": ch,
                "address": a,
                "category": category,
                "source": "defillama_adapter",
                "evidence_url": evidence_url,
                "notes": f"adapter_path={used_path}",
            })

        if matched % 50 == 0:
            print(f"matched_files={matched} | nofile={nofile} | rows_so_far={len(rows)}")

    out = pd.DataFrame(rows)
    if out.empty:
        print("⚠️ No rows produced (unexpected). Check GitHub access / input slugs.")
        out.to_csv(OUT, index=False)
        return

    # basic cleaning
    out["chain"] = out["chain"].fillna("").astype(str).str.strip().str.lower()
    out["address"] = out["address"].fillna("").astype(str).str.strip().str.lower()
    out = out[out["address"].str.startswith("0x") & (out["address"].str.len() == 42)].copy()

    # keep only unique chain+address when chain known; also keep unknown chain (but won't verify)
    out = out.drop_duplicates(subset=["chain", "address", "slug"], keep="first")

    out.to_csv(OUT, index=False)
    uniq = out[["chain", "address"]].drop_duplicates().shape[0]
    print(f"✅ Wrote: {OUT} | rows={len(out)} | unique(chain,address)={uniq}")
    print("Top chains (including blanks):")
    print(out["chain"].replace("", "(blank)").value_counts().head(20).to_string())

    # Optional merge into master_contracts.csv
    if MERGE_MASTER.exists():
        m = pd.read_csv(MERGE_MASTER)
        # normalize expected columns in master
        if "address" not in m.columns and "contract_address" in m.columns:
            m = m.rename(columns={"contract_address": "address"})
        if "protocol" not in m.columns and "protocol_name" in m.columns:
            m = m.rename(columns={"protocol_name": "protocol"})
        for c in ["slug","protocol","chain","address","category"]:
            if c not in m.columns:
                m[c] = ""

        keep_cols = ["slug","protocol","chain","address","category"]
        merged = pd.concat([m[keep_cols], out[keep_cols]], ignore_index=True)
        merged["chain"] = merged["chain"].fillna("").astype(str).str.strip().str.lower()
        merged["address"] = merged["address"].fillna("").astype(str).str.strip().str.lower()
        merged = merged.drop_duplicates(subset=["chain","address"], keep="first")
        merged.to_csv(MERGE_MASTER, index=False)
        print(f"✅ Merged into: {MERGE_MASTER} | rows={len(merged)}")

if __name__ == "__main__":
    main()