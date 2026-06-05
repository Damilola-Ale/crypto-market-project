import os
os.environ["BINANCE_API_KEY"] = "VwRsdWeAOEkAErLETXATW5yUkacV2G0DvQ9OVFzkqkHqpS2Fcw5tJpqK8SrofCwt"
os.environ["BINANCE_API_SECRET"] = "WiL3Syrb8Ce41skIG2penWreuCRcHb56igLs9R8S7ngK05QmkDjU9bYwhO3UfjLC"
os.environ["BINANCE_TESTNET"] = "1"

from execution.binance_client import get_account_balance, get_open_positions, _get_symbol_info

print("=== BALANCE ===")
print(get_account_balance())

print("\n=== OPEN POSITIONS ===")
print(get_open_positions())

print("\n=== SYMBOL INFO (ETHUSDT) ===")
info = _get_symbol_info("ETHUSDT")
print(f"status: {info.get('status')}")
for f in info.get("filters", []):
    if f["filterType"] in ("LOT_SIZE", "PRICE_FILTER", "MIN_NOTIONAL"):
        print(f)