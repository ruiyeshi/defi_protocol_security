#!/usr/bin/env python3
import os, sys
from pathlib import Path
from dotenv import load_dotenv

# --- Auto-detect project root ---
ROOT_DIR = Path(__file__).resolve()
while ROOT_DIR.name != "defi_protocol_security" and ROOT_DIR.parent != ROOT_DIR:
    ROOT_DIR = ROOT_DIR.parent

# --- Add root to Python path ---
sys.path.append(str(ROOT_DIR))

# --- Ensure .env is loaded from root ---
load_dotenv(dotenv_path=ROOT_DIR / ".env")

# --- Define folders ---
DATA_RAW = ROOT_DIR / "data_raw" / "contracts"
DATA_FINAL = ROOT_DIR / "data_final" / "contracts"
DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_FINAL.mkdir(parents=True, exist_ok=True)

# --- Check everything works ---
print("✅ ROOT_DIR:", ROOT_DIR)
print("✅ DATA_RAW exists:", DATA_RAW.exists())
print("✅ DATA_FINAL exists:", DATA_FINAL.exists())
print("✅ ETHERSCAN_API_KEY loaded:", os.getenv("ETHERSCAN_API_KEY")[:6] + "..." if os.getenv("ETHERSCAN_API_KEY") else "❌ Not found")