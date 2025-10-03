# config_loader.py
from dotenv import load_dotenv
import os

def load_api_key():
    load_dotenv()
    key = os.getenv("ETHERSCAN_API_KEY")
    if not key:
        raise ValueError("⚠️ Etherscan API key not found. Please check your .env file!")
    return key