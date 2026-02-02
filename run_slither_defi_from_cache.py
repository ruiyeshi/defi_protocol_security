#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
import time
import re
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple, List


def _env_bool(name: str, default: str = "0") -> bool:
    """Parse boolean-ish environment variables."""
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y"}


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


# --- Path environment variable reader supporting shell-style variables ---
def _env_path(name: str, default: str) -> Path:
    """Read a path-like env var and expand shell variables like $PWD and ~."""
    raw = os.getenv(name, default)
    raw = os.path.expandvars(raw)
    raw = os.path.expanduser(raw)
    return Path(raw)

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None
def _require_pandas() -> None:
    if pd is None:
        print("\n❌ pandas is not installed in the current Python environment.")
        print("Fix: activate your venv and run:  pip install pandas\n")
        raise SystemExit(1)
import sys
PROJECT_ROOT = Path(__file__).resolve().parent

# -------------------------
# Paths / I/O
# -------------------------
CACHE = _env_path("CACHE_DIR", "data_raw/contracts/source_cache")
OUT = _env_path("OUTDIR", "outputs/slither_defi")
WORK = OUT / "work"
JSONDIR = OUT / "json"
PROGRESS = OUT / "slither_defi_progress.csv"

OUT.mkdir(parents=True, exist_ok=True)
WORK.mkdir(parents=True, exist_ok=True)
JSONDIR.mkdir(parents=True, exist_ok=True)

MAX_FILES = int(os.getenv("MAX_FILES", "0"))  # 0 = all
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
# Timeout (seconds) for solc-select install/use calls
SOLC_SELECT_TIMEOUT = int(os.getenv("SOLC_SELECT_TIMEOUT", "25"))

# If 1, will attempt `solc-select install <version>` when pragma requests an exact version not installed.
AUTO_INSTALL_SOLC = _env_bool("AUTO_INSTALL_SOLC", "0")


# If 1, retry rows that previously failed (ok==0). This keeps ok==1 rows and re-attempts the rest.
RETRY_FAILED = _env_bool("RETRY_FAILED", "0")

# If 1, allow multiple concurrent runs that write to the same progress CSV (NOT recommended).
# Default is 0 which enables a simple lockfile to prevent progress corruption.
ALLOW_CONCURRENT = _env_bool("ALLOW_CONCURRENT", "0")

# If set (e.g., 0.6.0), skip contracts whose first pragma is below this (helps avoid very old solc quirks).

MIN_SOLC = _env_str("MIN_SOLC", "")
SKIP_BELOW_MIN_SOLC = _env_bool("SKIP_BELOW_MIN_SOLC", "0")

# If 1, when pragma requests an exact solc version that isn't installed, fall back to the closest
# installed patch within the same major.minor (e.g., pragma=0.5.16 -> try 0.5.x installed).
# WARNING: this is less strict than exact matching; use only if you accept approximation.
ALLOW_FALLBACK_EXACT = _env_bool("ALLOW_FALLBACK_EXACT", "0")

# Extra raw args forwarded to solc via Slither (e.g., include paths / base path):
#   SOLC_ARGS_EXTRA='--allow-paths . --base-path . --include-path node_modules'
SOLC_ARGS_EXTRA = _env_str("SOLC_ARGS_EXTRA", "")

# Cap solc choice for broad pragmas (e.g., ^0.8.0, >=0.8.0). Recommended: MAX_SOLC=0.8.26
# This avoids picking very new solc versions that may break Crytic/Slither pipelines on some projects.
MAX_SOLC = _env_str("MAX_SOLC", "")

#
# Optional: repo-level node_modules containing OZ packages. Default: <repo>/node_modules
NODE_MODULES_DIR = _env_path("NODE_MODULES_DIR", str(PROJECT_ROOT / "node_modules"))
if not NODE_MODULES_DIR.exists():
    print(f"⚠️ NODE_MODULES_DIR not found: {NODE_MODULES_DIR}. Set NODE_MODULES_DIR=... or run: npm i @openzeppelin/contracts @openzeppelin/contracts-upgradeable")

# If SOLC_ARGS_EXTRA is not provided, build a safe default that greatly improves import resolution.
# NOTE: base-path/include-path flags are not supported by very old solc versions, so we will
# apply these args only when the picked solc version is >= 0.6.0.
NODE_MODULES_ABS = str(NODE_MODULES_DIR.resolve())
SOLC_ARGS_DEFAULT = f"--allow-paths .,{NODE_MODULES_ABS} --base-path . --include-path . --include-path {NODE_MODULES_ABS}"
SOLC_ARGS_EFFECTIVE = SOLC_ARGS_EXTRA if SOLC_ARGS_EXTRA else SOLC_ARGS_DEFAULT

# Optional: vendor libraries to satisfy common imports.
# Create these folders locally if you want higher compile success:
#   data_raw/contracts/vendor/solmate
#   data_raw/contracts/vendor/solady
#   data_raw/contracts/vendor/@openzeppelin/contracts
VENDOR = _env_path("VENDOR_DIR", "data_raw/contracts/vendor")

# -------------------------
# Helpers
# -------------------------

def tail(s: str, n: int = 700) -> str:
    s = (s or "").strip().replace("\n", " ")
    return s[-n:] if len(s) > n else s


def proc_tail(stdout: str, stderr: str, returncode: int, n: int = 700) -> str:
    out = (stderr or "").strip()
    if not out:
        out = (stdout or "").strip()
    if not out:
        out = f"(no output) returncode={returncode}"
    return tail(out, n=n)


def unwrap_etherscan_source(s: str) -> str:
    """Etherscan sometimes wraps Standard-JSON with double braces {{...}}.
    It may also be a JSON string containing the braces.
    Return a cleaned string that *might* be JSON.
    """
    t = (s or "").strip()

    # If it's a JSON string (quoted), decode once
    if (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        try:
            t = json.loads(t)
        except Exception:
            # keep as-is
            pass

    t = (t or "").strip()
    if t.startswith("{{") and t.endswith("}}"):
        t = t[1:-1].strip()
    return t



def parse_standard_json_bundle(raw: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    """Return (sources_dict, entry_relpath).

    - sources_dict: {relative_path: content} extracted from Standard JSON input or legacy multi-file dict.
    - entry_relpath: best-effort entry file path (from settings.compilationTarget) for Standard JSON.

    This fixes a major failure mode where we wrote a multi-file bundle but compiled an arbitrary .sol file.
    """
    t = unwrap_etherscan_source(raw)
    if not t.startswith("{"):
        return None, None

    try:
        j = json.loads(t)
    except Exception:
        return None, None

    entry: Optional[str] = None

    # Standard JSON: use compilationTarget as the entry file if present.
    settings = j.get("settings")
    if isinstance(settings, dict):
        ct = settings.get("compilationTarget")
        if isinstance(ct, dict) and ct:
            # keys are file paths; values are contract names; we only need the file
            entry = next(iter(ct.keys()), None)

    # Case 1: Standard JSON input
    srcs = j.get("sources")
    if isinstance(srcs, dict) and srcs:
        out: Dict[str, str] = {}
        for rel, v in srcs.items():
            if isinstance(v, dict) and "content" in v:
                out[rel] = v.get("content") or ""
            elif isinstance(v, str):
                out[rel] = v
        return (out if out else None), entry

    # Case 2: Legacy multi-file dict: {"A.sol":{"content":"..."}, ...} or {"A.sol":"..."}
    out2: Dict[str, str] = {}
    if isinstance(j, dict) and j:
        looks_like_files = any(str(k).lower().endswith(".sol") for k in j.keys())
        if looks_like_files:
            for rel, v in j.items():
                if isinstance(v, dict) and "content" in v:
                    out2[rel] = v.get("content") or ""
                elif isinstance(v, str):
                    out2[rel] = v
            return (out2 if out2 else None), None

    return None, None


# --------------------------------------------
# OpenZeppelin-style import rewrite helper
# --------------------------------------------
_OZ_IMPORT_REWRITES: List[Tuple[str, str]] = [
    # OpenZeppelin proxy (flattened files often keep relative paths that no longer match)
    ("../ERC1967/ERC1967Proxy.sol", "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol"),
    ("../ERC1967/ERC1967Upgrade.sol", "@openzeppelin/contracts/proxy/ERC1967/ERC1967Upgrade.sol"),
    ("../beacon/IBeacon.sol", "@openzeppelin/contracts/proxy/beacon/IBeacon.sol"),
    ("../Proxy.sol", "@openzeppelin/contracts/proxy/Proxy.sol"),
    ("./ProxyAdmin.sol", "@openzeppelin/contracts/proxy/transparent/ProxyAdmin.sol"),
    ("./TransparentUpgradeableProxy.sol", "@openzeppelin/contracts/proxy/transparent/TransparentUpgradeableProxy.sol"),

    # OpenZeppelin access/utils
    ("../../access/Ownable.sol", "@openzeppelin/contracts/access/Ownable.sol"),
    ("access/Ownable.sol", "@openzeppelin/contracts/access/Ownable.sol"),
    ("../utils/Context.sol", "@openzeppelin/contracts/utils/Context.sol"),
    ("utils/Context.sol", "@openzeppelin/contracts/utils/Context.sol"),
    ("../../utils/Address.sol", "@openzeppelin/contracts/utils/Address.sol"),
    ("utils/Address.sol", "@openzeppelin/contracts/utils/Address.sol"),
    ("../../utils/StorageSlot.sol", "@openzeppelin/contracts/utils/StorageSlot.sol"),
    ("utils/StorageSlot.sol", "@openzeppelin/contracts/utils/StorageSlot.sol"),

    # OpenZeppelin interfaces
    ("../../interfaces/IERC1967.sol", "@openzeppelin/contracts/interfaces/IERC1967.sol"),

    # OpenZeppelin math utils (seen in some flattened utils bundles)
    ("./math/Math.sol", "@openzeppelin/contracts/utils/math/Math.sol"),
    ("./math/SignedMath.sol", "@openzeppelin/contracts/utils/math/SignedMath.sol"),
]

def rewrite_known_imports(sol_source: str) -> str:
    """Rewrite a few high-frequency relative import paths into canonical package imports.

    This helps when Etherscan provides a *single* flattened file that still contains
    OpenZeppelin-style relative imports (e.g., ../ERC1967/...), which break because
    the file is saved as contract.sol at the workdir root.

    The rewrites are conservative: only exact string matches inside import directives
    are replaced.
    """
    if not sol_source:
        return sol_source

    out = sol_source
    for old, new in _OZ_IMPORT_REWRITES:
        # Replace only inside import quotes; simple and effective for these exact patterns.
        out = out.replace(f'"{old}"', f'"{new}"')
        out = out.replace(f"'{old}'", f"'{new}'")
    return out



def _resolve_oz_contracts_dir() -> Optional[Path]:
    """Return a path that contains OpenZeppelin sources like `access/Ownable.sol`.

    We support both vendor clones and npm installs (node_modules).
    """
    candidates: List[Path] = []

    # 1) npm install layout: node_modules/@openzeppelin/contracts
    candidates.append(NODE_MODULES_DIR / "@openzeppelin" / "contracts")

    # 2) Standard vendor clone: vendor/openzeppelin-contracts/contracts
    candidates.append(VENDOR / "openzeppelin-contracts" / "contracts")

    # 3) Other possible clone layouts
    candidates.append(VENDOR / "@openzeppelin" / "contracts" / "contracts")
    candidates.append(VENDOR / "@openzeppelin" / "contracts")

    for p in candidates:
        try:
            if p.exists() and p.is_dir() and (p / "access").exists():
                return p
        except Exception:
            continue

    return None


def _resolve_oz_upgradeable_dir() -> Optional[Path]:
    """Return a path that contains OpenZeppelin Upgradeable sources like `access/OwnableUpgradeable.sol`."""
    candidates: List[Path] = []

    # 1) npm install layout: node_modules/@openzeppelin/contracts-upgradeable
    candidates.append(NODE_MODULES_DIR / "@openzeppelin" / "contracts-upgradeable")

    # 2) Vendor clone layouts (optional)
    candidates.append(VENDOR / "openzeppelin-contracts-upgradeable" / "contracts")
    candidates.append(VENDOR / "@openzeppelin" / "contracts-upgradeable" / "contracts")
    candidates.append(VENDOR / "@openzeppelin" / "contracts-upgradeable")

    for p in candidates:
        try:
            if p.exists() and p.is_dir() and ((p / "access").exists() or (p / "proxy").exists()):
                return p
        except Exception:
            continue

    return None

def ensure_vendor_links(contract_dir: Path) -> None:
    if not VENDOR.exists():
        return

    candidates: List[Tuple[str, Path]] = [
        # solmate imports are like: solmate/tokens/ERC20.sol -> repo stores in src/
        ("solmate", VENDOR / "solmate" / "src"),
        # solady imports: solady/auth/Ownable.sol -> repo stores in src/
        ("solady", VENDOR / "solady" / "src"),
        # forge-std imports: forge-std/console.sol -> repo stores in src/
        ("forge-std", VENDOR / "forge-std" / "src"),
        # openzeppelin-contracts imports: openzeppelin-contracts/contracts/...
        ("openzeppelin-contracts", VENDOR / "openzeppelin-contracts"),
    ]

    for dest_name, src in candidates:
        if not src.exists():
            continue
        dest = contract_dir / dest_name
        if dest.exists():
            continue
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.symlink_to(src)
        except Exception:
            pass

    # Make sure @openzeppelin/contracts/... resolves (node_modules or vendor clone).
    try:
        oz_contracts = _resolve_oz_contracts_dir()
        if oz_contracts is not None:
            pkg_root = contract_dir / "@openzeppelin"
            pkg_root.mkdir(parents=True, exist_ok=True)
            dest = pkg_root / "contracts"
            if not dest.exists():
                dest.symlink_to(oz_contracts)
    except Exception:
        pass

    # Also link @openzeppelin/contracts-upgradeable/... when available.
    try:
        oz_upg = _resolve_oz_upgradeable_dir()
        if oz_upg is not None:
            pkg_root = contract_dir / "@openzeppelin"
            pkg_root.mkdir(parents=True, exist_ok=True)
            dest = pkg_root / "contracts-upgradeable"
            if not dest.exists():
                dest.symlink_to(oz_upg)
    except Exception:
        pass

    # Make common repo-root prefixes resolve even when the extracted bundle is flattened.
    # Many projects import like `contracts/...` or `src/...` expecting those folders at repo root.
    # A symlink to the workdir often fixes these without needing base-path flags (especially on older solc).
    for prefix in ["contracts", "src", "lib"]:
        p = contract_dir / prefix
        if not p.exists():
            try:
                p.symlink_to(contract_dir)
            except Exception:
                pass

    # If node_modules exists at the repo level, expose it inside the workdir too.
    # This helps some compiler setups that look for `node_modules/...` relative to CWD.
    try:
        nm = contract_dir / "node_modules"
        if not nm.exists() and NODE_MODULES_DIR.exists():
            nm.symlink_to(NODE_MODULES_DIR)
    except Exception:
        pass


def write_contract_workdir(contract_dir: Path, raw: str) -> Path:
    """Write either multi-file sources or single-file into contract_dir.

    Returns the path to pass to slither: either a directory (multi-file)
    or a single .sol file (single-file).
    """
    contract_dir.mkdir(parents=True, exist_ok=True)

    srcs, entry_rel = parse_standard_json_bundle(raw)
    if srcs is None:
        # single-file
        p = contract_dir / "contract.sol"
        fixed = rewrite_known_imports(raw)
        p.write_text(fixed, encoding="utf-8", errors="ignore")
        ensure_vendor_links(contract_dir)
        return p

    # multi-file
    wrote_any = False
    for rel, content in srcs.items():
        rel = str(rel).replace("\\", "/").lstrip("/")
        p = contract_dir / rel
        p.parent.mkdir(parents=True, exist_ok=True)

        # Write all files from the standard-json/multi-file bundle. Missing imports will break compilation.
        p.write_text(content or "", encoding="utf-8", errors="ignore")
        wrote_any = True

    if not wrote_any:
        # Fallback to single file if nothing .sol was extracted
        p = contract_dir / "contract.sol"
        fixed = rewrite_known_imports(raw)
        p.write_text(fixed, encoding="utf-8", errors="ignore")
        ensure_vendor_links(contract_dir)
        return p

    ensure_vendor_links(contract_dir)

    # Choose an entry file to compile/analyze.
    # 1) Prefer the Standard JSON compilationTarget (best signal of the intended main file).
    if entry_rel:
        entry_rel_norm = str(entry_rel).replace("\\", "/").lstrip("/")
        entry_path = contract_dir / entry_rel_norm
        if entry_path.exists() and entry_path.is_file():
            return entry_path

    # 2) Fallback heuristics: pick the shallowest path; tie-breaker = largest file (often the main contract)
    sol_files = [p for p in contract_dir.rglob("*.sol") if p.is_file()]
    if sol_files:
        sol_files = sorted(
            sol_files,
            key=lambda p: (
                len(p.relative_to(contract_dir).parts),
                -p.stat().st_size,
                str(p),
            ),
        )
        return sol_files[0]

    # If somehow no .sol exists, fallback to a single-file.
    p = contract_dir / "contract.sol"
    fixed = rewrite_known_imports(raw)
    p.write_text(fixed, encoding="utf-8", errors="ignore")
    return p


def iter_cache_files() -> List[Path]:
    return sorted(CACHE.glob("*/*.sol"))


# -------------------------
# solc-select integration
# -------------------------

_PRAGMA_RE = re.compile(r"^\s*pragma\s+solidity\s+([^;]+);", re.IGNORECASE)


# --- solc-select root discovery ---
def _solc_select_roots() -> List[Path]:
    """Return likely solc-select roots.

    solc-select can store artifacts either in the user's home (~/.solc-select)
    or inside a virtualenv (e.g., $VIRTUAL_ENV/.solc-select).
    """
    roots: List[Path] = []

    # Explicit override
    env_root = os.getenv("SOLC_SELECT_DIR")
    if env_root:
        roots.append(Path(env_root).expanduser())

    # Virtualenv-local (most important for your setup)
    venv = os.getenv("VIRTUAL_ENV")
    if venv:
        roots.append(Path(venv) / ".solc-select")

    # sys.prefix sometimes points at the venv
    roots.append(Path(sys.prefix) / ".solc-select")

    # Home directory default
    roots.append(Path.home() / ".solc-select")

    # De-dup while preserving order
    out: List[Path] = []
    seen = set()
    for r in roots:
        rp = str(r.resolve()) if r.exists() else str(r)
        if rp in seen:
            continue
        seen.add(rp)
        out.append(r)
    return out


def _get_installed_solc_versions() -> List[str]:
    """Return installed versions from `solc-select versions` (best-effort)."""
    try:
        p = subprocess.run(["solc-select", "versions"], capture_output=True, text=True)
        if p.returncode != 0:
            return []
        out = []
        for line in p.stdout.splitlines():
            # Lines can look like: "0.8.30 (current, set by ...)" -> take first token
            tok = line.strip().split()[0]
            if re.match(r"^\d+\.\d+\.\d+$", tok):
                out.append(tok)
        return sorted(out, key=lambda s: tuple(int(x) for x in s.split(".")))
    except Exception:
        return []


def _vtuple(v: str) -> Tuple[int, int, int]:
    a, b, c = v.split(".")
    return (int(a), int(b), int(c))


def _pick_solc_for_pragma(pragma_expr: str, installed: List[str], max_solc: Optional[str] = None) -> Optional[str]:
    """Very small constraint solver for common patterns."""
    expr = (pragma_expr or "").strip()
    if not expr or not installed:
        return None

    inst_all = sorted(installed, key=_vtuple)
    inst = inst_all

    # Cap selection to <= max_solc if provided (but only if it leaves at least one candidate)
    if max_solc and re.match(r"^\d+\.\d+\.\d+$", max_solc):
        try:
            mt = _vtuple(max_solc)
            capped = [v for v in inst_all if _vtuple(v) <= mt]
            if capped:
                inst = capped
        except Exception:
            pass

    # Exact: =0.7.6 or 0.8.9
    m = re.match(r"^=?\s*(\d+\.\d+\.\d+)$", expr)
    if m:
        v = m.group(1)
        return v if v in installed else None

    # Caret: ^0.5.16, ^0.8.0
    m = re.match(r"^\^\s*(\d+\.\d+\.\d+)$", expr)
    if m:
        base = _vtuple(m.group(1))
        major, minor, patch = base

        # caret semantics (SemVer):
        # ^1.2.3  => >=1.2.3 <2.0.0
        # ^0.8.1  => >=0.8.1 <0.9.0
        # ^0.5.0  => >=0.5.0 <0.6.0
        # ^0.0.4  => >=0.0.4 <0.0.5
        if major > 0:
            upper = (major + 1, 0, 0)
        elif minor > 0:
            upper = (0, minor + 1, 0)
        else:
            upper = (0, 0, patch + 1)

        cand = [v for v in inst if _vtuple(v) >= base and _vtuple(v) < upper]
        return cand[-1] if cand else None

    # Range: >=0.6.0 <0.8.0
    m = re.match(r"^>=\s*(\d+\.\d+\.\d+)\s*<\s*(\d+\.\d+\.\d+)$", expr)
    if m:
        lo = _vtuple(m.group(1))
        hi = _vtuple(m.group(2))
        cand = [v for v in inst if _vtuple(v) >= lo and _vtuple(v) < hi]
        return cand[-1] if cand else None

    # Very common: ^0.8.0, >=0.8.0
    m = re.match(r"^>=\s*(\d+\.\d+\.\d+)$", expr)
    if m:
        lo = _vtuple(m.group(1))
        cand = [v for v in inst if _vtuple(v) >= lo]
        return cand[-1] if cand else None

    return None


def _extract_first_pragma(target: Path) -> Optional[str]:
    """Scan up to a few .sol files for the first pragma expression."""
    files: List[Path] = []
    if target.is_dir():
        # If there is a "contract.sol" at root, prioritize it; otherwise scan a subset.
        preferred = target / "contract.sol"
        if preferred.exists():
            files = [preferred]
        else:
            files = list(target.rglob("*.sol"))[:30]
    else:
        files = [target]

    for p in files:
        try:
            for line in p.read_text(encoding="utf-8", errors="ignore").splitlines()[:80]:
                m = _PRAGMA_RE.match(line)
                if m:
                    return m.group(1).strip()
        except Exception:
            continue
    return None


def _solc_select_use(ver: str) -> bool:
    try:
        p = subprocess.run(
            ["solc-select", "use", ver],
            capture_output=True,
            text=True,
            timeout=SOLC_SELECT_TIMEOUT,
        )
        if p.returncode != 0:
            msg = (p.stderr or p.stdout or "").strip()
            if msg:
                print(f"⚠️ solc-select use {ver} failed: {msg[:200]}")
            else:
                print(f"⚠️ solc-select use {ver} failed (returncode={p.returncode})")
            return False
        return True
    except subprocess.TimeoutExpired:
        print(f"⚠️ solc-select use {ver} timed out after {SOLC_SELECT_TIMEOUT}s; continuing with current solc")
        return False
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted during solc-select use; continuing with current solc")
        return False
    except Exception as e:
        print(f"⚠️ solc-select use {ver} error: {e}")
        return False


def _solc_select_install(ver: str) -> bool:
    try:
        p = subprocess.run(
            ["solc-select", "install", ver],
            capture_output=True,
            text=True,
            timeout=SOLC_SELECT_TIMEOUT,
        )
        if p.returncode != 0:
            msg = (p.stderr or p.stdout or "").strip()
            if msg:
                print(f"⚠️ solc-select install {ver} failed: {msg[:200]}")
            else:
                print(f"⚠️ solc-select install {ver} failed (returncode={p.returncode})")
            return False
        return True
    except subprocess.TimeoutExpired:
        print(f"⚠️ solc-select install {ver} timed out after {SOLC_SELECT_TIMEOUT}s")
        return False
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted during solc-select install")
        return False
    except Exception as e:
        print(f"⚠️ solc-select install {ver} error: {e}")
        return False


def _find_solc_binary(ver: str) -> Optional[str]:
    """Best-effort: locate a version-specific solc binary installed by solc-select.

    IMPORTANT:
    - Do NOT fall back to a generic `solc` on PATH (e.g., ~/.local/bin/solc), because that
      can silently point to the wrong version and break exact pragmas like `=0.5.16`.
    - If we cannot find a version-specific binary, return None and let Slither use whatever
      `solc` is on PATH after `solc-select use <ver>`.
    """
    if not ver:
        return None

    for root in _solc_select_roots():
        try:
            if not root.exists():
                continue
        except Exception:
            continue

        candidates = [
            # Common solc-select layouts
            root / "artifacts" / f"solc-{ver}",
            root / "artifacts" / f"solc-v{ver}",
            root / f"solc-{ver}",
            root / f"solc-v{ver}",

            # Sometimes artifacts are directories containing a `solc` executable
            root / "artifacts" / f"solc-{ver}" / "solc",
            root / "artifacts" / f"solc-v{ver}" / "solc",
        ]

        for p in candidates:
            try:
                if p.exists() and p.is_file():
                    return str(p.resolve())
            except Exception:
                continue

    return None


def _is_below_min_solc(pragma_expr: str, min_ver: str) -> bool:
    """Best-effort: extract the first x.y.z seen in pragma and compare to min_ver."""
    if not min_ver:
        return False
    m = re.search(r"(\d+\.\d+\.\d+)", pragma_expr or "")
    if not m:
        return False
    try:
        return _vtuple(m.group(1)) < _vtuple(min_ver)
    except Exception:
        return False


# -------------------------
# Error classification (for empirical coverage reporting)
# -------------------------

def classify_err_tail(err_tail: str) -> str:
    """Map raw compiler/slither stderr tails into a small set of classes.

    This is used for descriptive statistics (what dominates scan failures).
    Keep this intentionally coarse and stable over time.
    """
    t = (err_tail or "").lower()
    if not t:
        return "no_output"

    # Successful runs are identified separately via ok==1, but keep a label anyway
    if "✅" in (err_tail or ""):
        return "ok"

    # OpenZeppelin / vendor patterns (keep separate from generic missing imports)
    if ("@openzeppelin" in t or "openzeppelin" in t) and (
        "file not found" in t or ("source" in t and "not found" in t) or ("not found" in t and "import" in t)
    ):
        return "openzeppelin_import"

    # Common Solidity compilation / dependency issues
    if ("file not found" in t) or ("not found" in t and "import" in t):
        return "missing_import"
    if "source" in t and "not found" in t:
        return "missing_import"
    if "parsererror" in t and "source" in t and "not found" in t:
        return "missing_import"

    # OpenZeppelin mentions (non-missing-import)
    if "@openzeppelin" in t or "openzeppelin" in t:
        return "openzeppelin_import"

    # solc-select / version mismatch
    if "pragma" in t and ("requires different compiler version" in t or "compiler version" in t):
        return "solc_version_mismatch"
    if "solc" in t and "version" in t and "not installed" in t:
        return "solc_missing"

    # crytic-compile / solc flags
    if "invalid option to --combined-json" in t and "compact-format" in t:
        return "solc_combined_json_flag"
    if "unknown file" in t and "contract.sol" in t:
        return "crytic_unknown_file"

    # Slither / parsing
    if "slither" in t and ("error" in t or "exception" in t):
        return "slither_error"

    # Fallback
    return "other"

# -------------------------
# Main
# -------------------------

def main():
    _require_pandas()

    # Simple lock to prevent concurrent runs from corrupting the progress CSV.
    lock_path = OUT / ".slither_run.lock"
    if lock_path.exists() and not ALLOW_CONCURRENT:
        try:
            msg = lock_path.read_text(encoding="utf-8", errors="ignore").strip()
        except Exception:
            msg = ""
        print(f"\n❌ Lockfile exists: {lock_path}")
        if msg:
            print(f"   lock info: {msg}")
        print("   Another run is likely active. Stop the other terminals or set ALLOW_CONCURRENT=1 (not recommended).\n")
        raise SystemExit(2)

    try:
        try:
            lock_path.write_text(f"pid={os.getpid()} time={time.time()} outdir={OUT}\n", encoding="utf-8")
        except Exception:
            pass

        print("\n--- run_slither_defi_from_cache config ---")
        print("OUTDIR=", OUT)
        print("CACHE_DIR=", CACHE)
        print("RETRY_FAILED=", RETRY_FAILED)
        print("AUTO_INSTALL_SOLC=", AUTO_INSTALL_SOLC)
        print("ALLOW_FALLBACK_EXACT=", ALLOW_FALLBACK_EXACT)
        print("ALLOW_CONCURRENT=", ALLOW_CONCURRENT)
        print("MIN_SOLC=", MIN_SOLC, "SKIP_BELOW_MIN_SOLC=", SKIP_BELOW_MIN_SOLC)
        print("MAX_SOLC=", MAX_SOLC)
        print("NODE_MODULES_DIR=", NODE_MODULES_DIR)
        print("SOLC_ARGS_EXTRA(set?)=", bool(SOLC_ARGS_EXTRA))
        print("SOLC_ARGS_EFFECTIVE=", SOLC_ARGS_EFFECTIVE[:80] + ("..." if len(SOLC_ARGS_EFFECTIVE) > 80 else ""))
        print("----------------------------------------\n")

        files = iter_cache_files()
        if MAX_FILES > 0:
            files = files[:MAX_FILES]

        installed_solc = _get_installed_solc_versions()

        done: set[str] = set()
        prev_ok: Dict[str, int] = {}
        prev_json: Dict[str, str] = {}
        rows: List[Dict] = []

        if PROGRESS.exists():
            prev = pd.read_csv(PROGRESS, low_memory=False)

            # Backfill err_class for older progress files
            if "err_class" not in prev.columns:
                prev["err_class"] = ""
            if "err_tail" in prev.columns:
                ok_series = prev.get("ok", 0)
                ok_series = ok_series.fillna(0).astype(int) if hasattr(ok_series, "fillna") else 0
                prev["err_class"] = prev["err_class"].fillna("").astype(str)
                mask_missing = prev["err_class"].str.strip().eq("")
                if mask_missing.any():
                    tails = prev.loc[mask_missing, "err_tail"].fillna("").astype(str)
                    prev.loc[mask_missing, "err_class"] = tails.map(classify_err_tail)
                # Ensure ok rows are labeled as ok
                if hasattr(ok_series, "eq"):
                    prev.loc[ok_series.eq(1), "err_class"] = "ok"

            # If we are retrying failures, keep only ok==1 rows in the existing progress
            # so we don't duplicate keys when we append re-run results.
            if RETRY_FAILED and "ok" in prev.columns:
                prev = prev[prev["ok"].fillna(0).astype(int) == 1].copy()

            rows = prev.to_dict("records")
            if {"chain", "address"}.issubset(prev.columns):
                keys = (prev["chain"].astype(str) + "|" + prev["address"].astype(str)).tolist()
                oks = prev.get("ok", pd.Series([0] * len(prev))).fillna(0).astype(int).tolist()
                jps = prev.get("json_path", pd.Series([""] * len(prev))).fillna("").astype(str).tolist()
                for k, o, jp in zip(keys, oks, jps):
                    prev_ok[k] = int(o)
                    prev_json[k] = str(jp)
                done = set(keys)

        processed_this_run = 0
        attempted_this_run = 0

        total_cache = len(files)

        for f in files:
            chain = f.parent.name.strip().lower()
            address = f.stem.strip().lower()
            key = f"{chain}|{address}"
            if key in done:
                continue

            attempted_this_run += 1
            raw = f.read_text(encoding="utf-8", errors="ignore")
            cid = f"{chain}_{address}"
            cdir = WORK / cid
            target = write_contract_workdir(cdir, raw)
            jout = JSONDIR / f"{cid}.json"

            # Pick solc version (best-effort)
            pragma = _extract_first_pragma(target)

            # Optional: skip very old pragmas
            if pragma and MIN_SOLC and SKIP_BELOW_MIN_SOLC and _is_below_min_solc(pragma, MIN_SOLC):
                rows.append({
                    "chain": chain,
                    "address": address,
                    "pragma": pragma or "",
                    "solc_picked": "",
                    "ok": 0,
                    "returncode": -2,
                    "elapsed_sec": 0.0,
                    "json_path": "",
                    "err_tail": f"SKIP: pragma<{MIN_SOLC}",
                    "err_class": "skip_old_pragma",
                })
                done.add(key)
                processed_this_run += 1
                continue

            picked = _pick_solc_for_pragma(pragma or "", installed_solc, MAX_SOLC or None) if pragma else None

            # If pragma asks for an exact/bare version and it's missing, optionally install it.
            if pragma and not picked:
                m_exact = re.match(r"^=?\s*(\d+\.\d+\.\d+)$", pragma.strip())
                if m_exact:
                    need = m_exact.group(1)
                    if AUTO_INSTALL_SOLC:
                        if _solc_select_install(need):
                            installed_solc = _get_installed_solc_versions()
                            picked = need if need in installed_solc else None

                    # Optional fallback for exact versions
                    if not picked and ALLOW_FALLBACK_EXACT:
                        try:
                            mm = ".".join(need.split(".")[:2])  # e.g., "0.5"
                            same_mm = [v for v in installed_solc if v.startswith(mm + ".")]
                            if same_mm:
                                picked = sorted(same_mm, key=_vtuple)[-1]
                        except Exception:
                            picked = None

                    if not picked:
                        rows.append({
                            "chain": chain,
                            "address": address,
                            "pragma": pragma or "",
                            "solc_picked": "",
                            "ok": 0,
                            "returncode": -3,
                            "elapsed_sec": 0.0,
                            "json_path": "",
                            "err_tail": (
                                f"SKIP: missing exact solc {need} (set AUTO_INSTALL_SOLC=1 to auto-install)"
                                + ("; fallback disabled" if not ALLOW_FALLBACK_EXACT else "")
                            ),
                            "err_class": "skip_missing_exact_solc",
                        })
                        done.add(key)
                        processed_this_run += 1
                        continue

            if picked:
                _solc_select_use(picked)

            t0 = time.time()
            try:
                # Remappings only (portable across many solc versions). We avoid --base-path/--include-path
                # because older solc builds error on these flags and it tanks the whole run.
                solc_remaps: List[str] = [
                    f"solmate/={str((cdir / 'solmate').resolve())}/",
                    f"solady/={str((cdir / 'solady').resolve())}/",
                    f"forge-std/={str((cdir / 'forge-std').resolve())}/",
                ]

                # OpenZeppelin: remap the *exact* prefix used in imports.
                oz_contracts = _resolve_oz_contracts_dir()
                if oz_contracts is not None:
                    ozp = str(oz_contracts.resolve())
                    solc_remaps.append(f"@openzeppelin/contracts/={ozp}/")
                    solc_remaps.append(f"contracts/external/openzeppelin/contracts/={ozp}/")
                    solc_remaps.append(f"openzeppelin-solidity/={ozp}/")
                    solc_remaps.append(f"openzeppelin-contracts/={ozp}/")

                # OpenZeppelin Upgradeable package
                oz_upg = _resolve_oz_upgradeable_dir()
                if oz_upg is not None:
                    ozup = str(oz_upg.resolve())
                    solc_remaps.append(f"@openzeppelin/contracts-upgradeable/={ozup}/")
                    solc_remaps.append(f"contracts/external/openzeppelin/contracts-upgradeable/={ozup}/")

                # openzeppelin-contracts style imports (repo-name prefix)
                oz_repo = VENDOR / "openzeppelin-contracts"
                if oz_repo.exists():
                    solc_remaps.append(f"openzeppelin-contracts/={str(oz_repo.resolve())}/")
                    if (oz_repo / "contracts").exists():
                        solc_remaps.append(f"openzeppelin-contracts/contracts/={str((oz_repo / 'contracts').resolve())}/")

                solc_remaps_str = " ".join(solc_remaps)

                # Always use absolute paths for target/output to avoid crytic-compile "Unknown file: contract.sol".
                target_abs = str(target.resolve())
                jout_abs = str(jout.resolve())

                solc_bin = _find_solc_binary(picked or "") if picked else None

                cmd = [
                    "slither",
                    target_abs,
                    "--json",
                    jout_abs,
                    "--disable-color",
                ]
                if solc_bin:
                    cmd += ["--solc", solc_bin]
                if solc_remaps_str.strip():
                    cmd += ["--solc-remaps", solc_remaps_str]

                # Optional: forward extra solc args (include paths, base-path, allow-paths, etc.).
                # Only apply for solc >= 0.6.0 to avoid breaking very old compilers.
                solc_args_to_use = SOLC_ARGS_EFFECTIVE
                try:
                    if picked and _vtuple(picked) < (0, 6, 0):
                        solc_args_to_use = ""
                except Exception:
                    pass
                if solc_args_to_use:
                    cmd += ["--solc-args", solc_args_to_use]

                env = os.environ.copy()

                # Make sure solc-select + its artifacts are visible (supports venv-local installs)
                extra_paths: List[str] = []
                for root in _solc_select_roots():
                    extra_paths.append(str(root))
                    extra_paths.append(str(root / "artifacts"))

                extra_paths.append(str(Path.home() / ".local" / "bin"))
                env["PATH"] = ":".join(extra_paths + [env.get("PATH", "")])

                proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(cdir))

                ok = 1 if (Path(jout_abs).exists() and Path(jout_abs).stat().st_size > 0) else 0
                elapsed = round(time.time() - t0, 3)

                err_t = proc_tail(proc.stdout, proc.stderr, proc.returncode)
                err_c = "ok" if ok == 1 else classify_err_tail(err_t)

                rows.append({
                    "chain": chain,
                    "address": address,
                    "pragma": pragma or "",
                    "solc_picked": picked or "",
                    "ok": ok,
                    "returncode": proc.returncode,
                    "elapsed_sec": elapsed,
                    "json_path": jout_abs if ok else "",
                    "err_tail": err_t,
                    "err_class": err_c,
                })

            except Exception as e:
                rows.append({
                    "chain": chain,
                    "address": address,
                    "pragma": pragma or "",
                    "solc_picked": picked or "",
                    "ok": 0,
                    "returncode": -999,
                    "elapsed_sec": round(time.time() - t0, 3),
                    "json_path": "",
                    "err_tail": tail(str(e)),
                    "err_class": "exception",
                })

            processed_this_run += 1
            done.add(key)

            # Periodic checkpoint
            if processed_this_run % BATCH_SIZE == 0:
                pd.DataFrame(rows).to_csv(PROGRESS, index=False)
                print(
                    f"processed {processed_this_run} (this run) | attempted {attempted_this_run} (this run) | "
                    f"total_cache={total_cache} | wrote ckpt {PROGRESS}"
                )

        # Final write
        pd.DataFrame(rows).to_csv(PROGRESS, index=False)
        print(
            "✅ done. wrote",
            PROGRESS,
            "rows=",
            len(rows),
            "| processed_this_run=",
            processed_this_run,
            "| attempted_this_run=",
            attempted_this_run,
        )

    finally:
        # Best-effort: remove lock
        if lock_path.exists():
            try:
                lock_path.unlink()
            except Exception:
                pass


if __name__ == "__main__":
    main()