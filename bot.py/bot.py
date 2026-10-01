"""
SRL Trading Bot — Deriv R_75 H4
Runs on GitHub Actions every 5 minutes.
Safe first version: DRY_RUN=True logs signals without placing real orders.
"""
import asyncio
import json
import os
import math
from datetime import datetime, timezone
from pathlib import Path

import websockets

API_TOKEN = os.environ.get("DERIV_API_TOKEN", "")
APP_ID = "1089"
SYMBOL = "R_75"
HTF_SECONDS = 14400  # 4-hour candles

# ---- STRATEGY CONFIG ----
BUY_LEVEL = 30
SELL_LEVEL = -30
TP_POINTS = 1000
SL_POINTS = 500
ANCHOR_MODE = "Weighted"

# ---- SAFETY ----
# Set to False only when you are ready for real trades on demo money.
DRY_RUN = True

STATE_FILE = Path("state.json")
LOG_FILE = Path("bot_log.txt")


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "in_long": False,
        "in_short": False,
        "entry_price": 0.0,
        "last_signal_epoch": 0,
        "contract_id": None,
    }


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------- COMPOSITE MATH ----------------
EPS = 1e-15


def parkinson_vol(highs, lows, n):
    if len(highs) < n:
        return 0.01
    total = 0.0
    for i in range(-n, 0):
        h, l = highs[i], lows[i]
        if l > EPS:
            hl = math.log(h / l)
            total += hl * hl
    return math.sqrt(total / (n * 4.0 * math.log(2.0)))


def find_pivots(highs, lows, lb):
    if len(highs) < lb:
        return highs[-1], lows[-1], 0, 0
    window_h = highs[-lb:]
    window_l = lows[-lb:]
    ph = max(window_h)
    pl = min(window_l)
    ha = len(window_h) - 1 - window_h[::-1].index(ph)
    la = len(window_l) - 1 - window_l[::-1].index(pl)
    return ph, pl, ha, la


def vol_from_bars(volumes, n):
    return sum(volumes[-n:]) if len(volumes) >= n else sum(volumes)


def bvc_split(opens, highs, lows, closes, volumes, idx):
    sp = closes[idx] - opens[idx]
    isg = max((highs[idx] - lows[idx]) / 4.0, EPS)
    z = sp / isg
    phi = 1.0 / (1.0 + math.exp(-1.7 * z))
    return volumes[idx] * phi, volumes[idx] * (1.0 - phi)


def compute_dominance(opens, highs, lows, closes, volumes, lb):
    """Simplified dominance calc — matches Pine structure closely."""
    if len(closes) < lb + 2:
        return 0.5

    ph, pl, ha, la = find_pivots(highs, lows, lb)
    curr = closes[-1]

    # volume from pivot
    def vol_split_n(n):
        vb = vs = 0.0
        for k in range(len(closes) - n, len(closes)):
            b, s = bvc_split(opens, highs, lows, closes, volumes, k)
            vb += b
            vs += s
        return vb, vs

    look_la = max(1, la + 1)
    look_ha = max(1, ha + 1)

    vb_low, _ = vol_split_n(look_la)
    _, vs_high = vol_split_n(look_ha)

    sigma = parkinson_vol(highs, lows, lb) * curr
    k = 1.0

    dp_low = curr - pl
    dp_high = curr - ph

    q_bull = 0.0
    if vb_low > 0 and sigma > EPS:
        ir = abs(dp_low) / (sigma * k)
        q_bull = vb_low * ir * ir

    q_bear = 0.0
    if vs_high > 0 and sigma > EPS:
        ir = abs(dp_high) / (sigma * k)
        q_bear = vs_high * ir * ir

    total = q_bull + q_bear
    if total < EPS:
        return 0.5
    return q_bull / total


def compute_composite(opens, highs, lows, closes, volumes):
    """Returns composite in [-100, 100]."""
    if len(closes) < 60:
        return 0.0

    adomS = compute_dominance(opens, highs, lows, closes, volumes, 15)
    adomM = compute_dominance(opens, highs, lows, closes, volumes, 25)
    adomL = compute_dominance(opens, highs, lows, closes, volumes, 50)

    domS = (adomS - 0.5) * 2.0
    domM = (adomM - 0.5) * 2.0
    domL = (adomL - 0.5) * 2.0

    # momentum — use rolling dominance history approximations
    momS = 0.0
    momM = 0.0
    momL = 0.0
    momRaw = (momS * 10.0 + momM * 10.0 + momL * 10.0) / 3.0
    mom = max(-1.0, min(1.0, momRaw))

    # state score — simplified (0 for now)
    stateScore = 0.0

    c = (domS * 0.20 + domM * 0.30 + domL * 0.25 + mom * 0.15 + stateScore * 0.10) * 100.0
    return max(-100.0, min(100.0, c))


# ---------------- DERIV API ----------------
class DerivClient:
    def __init__(self):
        self.ws = None
        self.req_id = 1

    async def connect(self):
        url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
        self.ws = await websockets.connect(url, open_timeout=15, ping_interval=20)
        log("WebSocket connected")

    async def send(self, payload):
        self.req_id += 1
        payload["req_id"] = self.req_id
        await self.ws.send(json.dumps(payload))
        while True:
            raw = await asyncio.wait_for(self.ws.recv(), timeout=20)
            data = json.loads(raw)
            if data.get("req_id") == self.req_id:
                return data

    async def close(self):
        if self.ws:
            await self.ws.close()


async def fetch_candles(client, count=200):
    resp = await client.send({
        "ticks_history": SYMBOL,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "granularity": HTF_SECONDS,
        "style": "candles",
    })
    if "candles" not in resp:
        log(f"Error fetching candles: {resp.get('error', resp)}")
        return None
    return resp["candles"]


def get_candles_lists(candles):
    opens = [float(c["open"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    closes = [float(c["close"]) for c in candles]
    volumes = [float(c.get("volume", 1)) for c in candles]
    return opens, highs, lows, closes, volumes


async def place_order(client, direction, stake=1.0):
    """direction: 'long' or 'short'."""
    contract_type = "CALL" if direction == "long" else "PUT"
    resp = await client.send({
        "buy": "1",
        "price": stake,
        "parameters": {
            "amount": stake,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "duration": 4,
            "duration_unit": "h",
            "symbol": SYMBOL,
        },
    })
    return resp


async def run():
    log("=== SRL Bot run start ===")
    state = load_state()
    log(f"State: {state}")

    if not API_TOKEN:
        log("ERROR: DERIV_API_TOKEN not set")
        return

    client = DerivClient()
    try:
        await client.connect()

        # Authorize (optional for public candles, needed for buy)
        auth = await client.send({"authorize": API_TOKEN})
        if "error" in auth:
            log(f"Auth error: {auth['error']}")
            return
        log(f"Authorized on account {auth.get('authorize', {}).get('loginid', '?')}")

        # Fetch candles
        candles = await fetch_candles(client, count=200)
        if not candles:
            return

        opens, highs, lows, closes, volumes = get_candles_lists(candles)
        log(f"Fetched {len(candles)} H4 candles, latest close={closes[-1]}")

        # Compute composite for last 3 bars
        composite_now = compute_composite(opens, highs, lows, closes, volumes)

        # Previous bar composite
        comp_prev = compute_composite(
            opens[:-1], highs[:-1], lows[:-1], closes[:-1], volumes[:-1]
        )

        log(f"Composite prev={comp_prev:.2f} now={composite_now:.2f}")

        # --- ENTRY SIGNALS ---
        long_signal = comp_prev <= BUY_LEVEL and composite_now > BUY_LEVEL
        short_signal = comp_prev >= SELL_LEVEL and composite_now < SELL_LEVEL

        if long_signal or short_signal:
            direction = "long" if long_signal else "short"
            log(f"🔥 SIGNAL: {direction.upper()} (composite crossed {'+30' if long_signal else '-30'})")

            if state["in_long"] or state["in_short"]:
                log("  → Already in a trade, ignoring signal")
            else:
                state[f"in_{direction}"] = True
                state["entry_price"] = closes[-1]

                if DRY_RUN:
                    log(f"  → DRY_RUN: would place {direction} order at {closes[-1]}")
                else:
                    order = await place_order(client, direction)
                    if "buy" in order:
                        state["contract_id"] = order["buy"]["contract_id"]
                        log(f"  → ORDER PLACED: contract_id={state['contract_id']}")
                    else:
                        log(f"  → Order error: {order.get('error', order)}")
                        state[f"in_{direction}"] = False

        # --- EXIT CHECK (composite-0 flip) ---
        if state["in_long"]:
            exit_sig = comp_prev >= 0 and composite_now < 0
            if exit_sig:
                log(f"🔻 EXIT long: composite crossed below 0")
                if DRY_RUN:
                    log("  → DRY_RUN: would close long")
                else:
                    if state.get("contract_id"):
                        await client.send({"sell": state["contract_id"], "price": 0})
                    log("  → CLOSED long")
                state["in_long"] = False
                state["entry_price"] = 0.0
                state["contract_id"] = None

        if state["in_short"]:
            exit_sig = comp_prev <= 0 and composite_now > 0
            if exit_sig:
                log(f"🔺 EXIT short: composite crossed above 0")
                if DRY_RUN:
                    log("  → DRY_RUN: would close short")
                else:
                    if state.get("contract_id"):
                        await client.send({"sell": state["contract_id"], "price": 0})
                    log("  → CLOSED short")
                state["in_short"] = False
                state["entry_price"] = 0.0
                state["contract_id"] = None

        # Save state back
        state["last_signal_epoch"] = candles[-1]["epoch"]
        save_state(state)
        log("=== SRL Bot run end ===")

    except Exception as e:
        log(f"EXCEPTION: {type(e).__name__}: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(run())
