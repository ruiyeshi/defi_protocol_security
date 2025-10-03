import os
import requests
from dotenv import load_dotenv
from pathlib import Path

# Load your .env
load_dotenv(Path(__file__).resolve().parents[0] / ".env")

# Read key and base URL
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
BASE_URL = "https://api.etherscan.io/v2/api"

# Example: get ETH balance for a sample address on Ethereum mainnet
chainid = 1
address = "0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511"

params = {
    "chainid": chainid,
    "module": "account",
    "action": "balance",
    "address": address,
    "tag": "latest",
    "apikey": ETHERSCAN_API_KEY,
}

response = requests.get(BASE_URL, params=params)
print("✅ Request URL:", response.url)
print("✅ Status Code:", response.status_code)
print("✅ Response:", response.text[:300])