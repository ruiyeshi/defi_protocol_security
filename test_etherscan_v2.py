import os, asyncio, aiohttp, json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent / ".env")

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
ETHERSCAN_V2_URL = os.getenv("ETHERSCAN_V2_URL")

async def test():
    params = {
        "chainid": 1,
        "module": "contract",
        "action": "getsourcecode",
        "address": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",  # UniswapV2Factory
        "apikey": ETHERSCAN_API_KEY
    }
    async with aiohttp.ClientSession() as s:
        async with s.get(ETHERSCAN_V2_URL, params=params) as r:
            print("Status:", r.status)
            print(json.dumps(await r.json(), indent=2)[:1000])

asyncio.run(test())