import urllib.request, json

SYMBOLS = [
    "ICXUSDT", "RUNEUSDT", "LDOUSDT", "SUIUSDT", "ADAUSDT", "APTUSDT", "LSKUSDT",
    "AAVEUSDT", "SSVUSDT", "AVAXUSDT", "YGGUSDT", "UNIUSDT", "VETUSDT",
    "SANDUSDT", "TRBUSDT", "YFIUSDT", "IDUSDT", "ETHUSDT", "ORDIUSDT", "ZILUSDT",
    "XVGUSDT", "GMXUSDT", "ZECUSDT", "DEXEUSDT", "RPLUSDT", "IOSTUSDT", "NFPUSDT",
    "DOGEUSDT", "KSMUSDT", "KAVAUSDT", "EGLDUSDT", "ICPUSDT", "SOLUSDT", "GRTUSDT",
    "TRXUSDT", "PAXGUSDT", "CKBUSDT", "JUPUSDT", "ZENUSDT", "IOTXUSDT", "COTIUSDT",
    "TIAUSDT", "STORJUSDT", "RIFUSDT", "SLPUSDT", "CFXUSDT", "ARBUSDT", "CVXUSDT",
    "FETUSDT", "FILUSDT",
]

req = urllib.request.Request(
    "https://fapi.binance.com/fapi/v1/ticker/24hr",
    headers={"User-Agent": "Mozilla/5.0"}
)
try:
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.load(r)
    vol_by_symbol = {d["symbol"]: float(d["quoteVolume"]) for d in data}
    rows = []
    for s in SYMBOLS:
        v = vol_by_symbol.get(s)
        rows.append((s, v))
    rows.sort(key=lambda x: (x[1] is None, -(x[1] or 0)))
    print(f"{'SYMBOL':<12}{'24h Quote Volume (USDT)':>26}")
    for s, v in rows:
        flag = "  <-- THIN" if (v is None or v < 20_000_000) else ""
        print(f"{s:<12}{(f'{v:,.0f}' if v is not None else 'NOT FOUND'):>26}{flag}")
except Exception as e:
    print("FETCH FAILED:", e)