"""Write a synthetic 10-minute BTCUSDT recording as frames in JSON Lines, shaped like a
real one: a 5000-level snapshot, a depth update of about 35 levels every 100 ms, 170 book
tickers and 70 trades a second. Input for `marketrec bench` when no recording is at hand.

    python3 scripts/synthetic_day.py /tmp/day.jsonl
    mkdir -p /tmp/data/binance/BTCUSDT/2026-11-21/frames
    marketrec frames pack --input /tmp/day.jsonl --output /tmp/data/binance/BTCUSDT/2026-11-21/frames/10-00-00.frames
    marketrec bench --day /tmp/data/binance/BTCUSDT/2026-11-21
"""
import json, random, sys
random.seed(7)
T0 = 1795219200_000_000_000 + 10 * 3600 * 10**9
MID = 9_700_000  # cents
out = open(sys.argv[1], "w")
out.write(json.dumps({"header": {"symbol": "BTCUSDT", "price_step": "0.01000000", "qty_step": "0.00001000", "time_unit": "micro", "recorder": "synthetic"}}) + "\n")
def px(c): return f"{c // 100}.{c % 100:02d}000000"
def q(v): return f"{v // 100000}.{v % 100000:05d}000"
bids = {MID - 1 - i: random.randint(1, 200000) for i in range(5000)}
asks = {MID + i: random.randint(1, 200000) for i in range(5000)}
uid = 103047172278
def rec(kind, ts, aux, payload=None, raw=None):
    r = {"kind": kind, "recv_ts": ts, "aux": aux}
    if payload is not None: r["payload_json"] = payload
    if raw is not None: r["payload"] = raw
    out.write(json.dumps(r, separators=(",", ":")) + "\n")
rec("connected", T0, 1, raw="wss://stream.binance.com:9443/stream")
trade_id = 5_000_000_000
ts = T0 + 1_000_000
first = True
for step in range(6000):  # 100 ms each
    t = T0 + step * 100_000_000 + 1_000_000
    n = 38
    bch, ach = {}, {}
    for _ in range(n):
        if random.random() < 0.5:
            p = MID - 1 - int(random.expovariate(1 / 60))
            v = 0 if random.random() < 0.3 else random.randint(1, 200000)
            bch[p] = v
            if v: bids[p] = v
            else: bids.pop(p, None)
        else:
            p = MID + int(random.expovariate(1 / 60))
            v = 0 if random.random() < 0.3 else random.randint(1, 200000)
            ach[p] = v
            if v: asks[p] = v
            else: asks.pop(p, None)
    U = uid + 1; uid += random.randint(30, 90)
    data = {"e": "depthUpdate", "E": (t - 140_000_000) // 1000, "s": "BTCUSDT", "U": U, "u": uid,
            "b": [[px(p), q(v)] for p, v in bch.items()], "a": [[px(p), q(v)] for p, v in ach.items()]}
    rec("text", t, 1, payload={"stream": "btcusdt@depth@100ms", "data": data})
    if first:
        snap = {"lastUpdateId": uid, "bids": [[px(p), q(v)] for p, v in sorted(bids.items(), reverse=True)],
                "asks": [[px(p), q(v)] for p, v in sorted(asks.items())]}
        rec("snapshot", t + 2_000_000, t, payload=snap)
        first = False
    for k in range(17):  # book tickers
        tt = t + 3_000_000 + k * 5_500_000
        bb = max(bids); ba = min(asks)
        rec("text", tt, 1, payload={"stream": "btcusdt@bookTicker", "data": {"u": uid, "s": "BTCUSDT", "b": px(bb), "B": q(bids[bb]), "a": px(ba), "A": q(asks[ba])}})
    for k in range(7):  # trades
        tt = t + 4_000_000 + k * 13_000_000
        trade_id += 1
        rec("text", tt, 1, payload={"stream": "btcusdt@trade", "data": {"e": "trade", "E": (tt - 140_000_000) // 1000, "s": "BTCUSDT", "t": trade_id, "p": px(MID), "q": q(random.randint(1, 5000)), "T": (tt - 141_000_000) // 1000, "m": random.random() < 0.5, "M": True}})
rec("disconnected", T0 + 600 * 10**9 + 5_000_000, 1, raw="stopped")
