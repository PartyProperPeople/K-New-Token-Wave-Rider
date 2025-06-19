import os, json, time
import pandas as pd
from datetime import datetime, timedelta, UTC
from decimal import Decimal, getcontext
from binance.client import Client
from binance import ThreadedWebsocketManager
import keyring

# --- CONFIG ---
KEYRING_SERVICE = "ema-grid-bot"
API_KEY    = keyring.get_password(KEYRING_SERVICE, "API_KEY")
API_SECRET = keyring.get_password(KEYRING_SERVICE, "API_SECRET")
SYMBOL     = "SPKUSDT"
INTERVAL   = "15m"
CANDLE_FILE = "ohlc_15m.jsonl"
CANDLE_LIMIT_SEC = 8 * 60 * 60  # 48 hours
ORDER_QUANTITY = Decimal("10")
COOLDOWN_BARS = 4
getcontext().prec = 12

client = Client(API_KEY, API_SECRET)

def log(msg):
    print(f"{datetime.now(UTC):%Y-%m-%d %H:%M:%S}: {msg}")

def safe_dec(x):
    try:
        return Decimal(str(x))
    except:
        return Decimal("0")

def append_candle(candle):
    with open(CANDLE_FILE, "a") as f:
        f.write(json.dumps(candle) + "\n")

def load_recent_candles():
    if not os.path.exists(CANDLE_FILE):
        return []
    candles = []
    cutoff = (datetime.now(UTC) - timedelta(seconds=CANDLE_LIMIT_SEC)).timestamp() * 1000
    with open(CANDLE_FILE, "r") as f:
        for line in f:
            c = json.loads(line)
            if int(c["ts"]) >= cutoff:
                candles.append(c)
    return candles

def fill_history_if_needed():
    if os.path.exists(CANDLE_FILE) and os.path.getsize(CANDLE_FILE) > 0:
        return
    now = int(time.time() * 1000)
    start = now - CANDLE_LIMIT_SEC * 1000
    url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&startTime={start}&endTime={now}"
    import requests
    data = requests.get(url).json()
    with open(CANDLE_FILE, "w") as f:
        for d in data:
            candle = {
                "ts": d[0], "o": d[1], "h": d[2], "l": d[3], "c": d[4], "v": d[5]
            }
            f.write(json.dumps(candle) + "\n")

def get_lot_size(symbol):
    info = client.get_symbol_info(symbol)
    for f in info["filters"]:
        if f["filterType"] == "LOT_SIZE":
            return Decimal(f["stepSize"])
    return Decimal("0.00000001")

def round_down(val, step):
    return (val // step) * step

def compute_stoch(df, k_period):
    # Ensure columns are objects so pandas doesn't cast them to float
    df['c'] = df['c'].astype(object)
    df['h'] = df['h'].astype(object)
    df['l'] = df['l'].astype(object)

    lows = df['l'].rolling(k_period).min()
    highs = df['h'].rolling(k_period).max()

    k_vals = []
    for i in range(len(df)):
        if i < k_period - 1:
            k_vals.append(None)
        else:
            low = Decimal(str(lows.iloc[i]))
            high = Decimal(str(highs.iloc[i]))
            close = Decimal(str(df['c'].iloc[i]))
            if high == low:
                k_vals.append(Decimal("50"))
            else:
                k_val = ((close - low) / (high - low)) * Decimal("100")
                k_vals.append(k_val)
    df["K"] = k_vals
    df = df.dropna()
    return df

def check_signals(df, params):
    lower, mid, upper = params['LOWER'], params['MID'], params['UPPER']
    k_prev, k_now = df["K"].iloc[-2], df["K"].iloc[-1]
    signals = {"buy": False, "sell": False, "reason": None}
    if k_prev < lower and k_now > lower:
        signals["buy"] = True
        signals["reason"] = "K crosses up LOWER"
    elif k_prev < upper and k_now > upper:
        signals["sell"] = True
        signals["reason"] = "K crosses up UPPER"
    elif k_prev > mid and k_now < mid:
        signals["sell"] = True
        signals["reason"] = "K crosses down MID"
    elif k_prev > lower and k_now < lower:
        signals["sell"] = True
        signals["reason"] = "K crosses down LOWER"
    return signals

def place_market_order(symbol, side, quantity):
    try:
        order = client.create_order(
            symbol=symbol,
            side=side,
            type=Client.ORDER_TYPE_MARKET,
            quantity=float(quantity)
        )
        log(f"ORDER: {side} {quantity} {symbol} | Binance orderId: {order['orderId']}")
        return True
    except Exception as e:
        log(f"ORDER ERROR: {e}")
        return False

import random

def backtest_params(df, n_combos=10000, fine_tune=2000):
    best_score = -999
    best_params = None

    # --- Random coarse search ---
    for _ in range(n_combos):
        k_period = random.randint(8, 20)
        lower = random.randint(10, 30)
        upper = random.randint(70, 90)
        if upper <= lower + 10:
            continue
        mid = random.randint(lower + 2, upper - 2)
        df_stoch = compute_stoch(df, k_period)
        if len(df_stoch) < 3:
            continue
        trades = []
        in_position = False
        last_entry = -COOLDOWN_BARS
        for i in range(1, len(df_stoch)):
            k_prev = df_stoch["K"].iloc[i-1]
            k_now = df_stoch["K"].iloc[i]
            if not in_position and (k_prev < lower and k_now > lower) and (i - last_entry >= COOLDOWN_BARS):
                entry = df_stoch['c'].iloc[i]
                trades.append({'entry': entry, 'entry_idx': i})
                in_position = True
                last_entry = i
            elif in_position and (
                (k_prev < upper and k_now > upper) or
                (k_prev > mid and k_now < mid) or
                (k_prev > lower and k_now < lower)
            ):
                trades[-1]['exit'] = df_stoch['c'].iloc[i]
                in_position = False
        total = Decimal("1")
        for t in trades:
            if 'exit' in t:
                r = (t['exit'] - t['entry']) / t['entry'] + Decimal("1")
                total *= r
        pct = (total - Decimal("1")) * 100
        if pct > best_score:
            best_score = pct
            best_params = {'K_PERIOD': k_period, 'LOWER': Decimal(lower), 'MID': Decimal(mid), 'UPPER': Decimal(upper)}
    log(f"[Random Search] Best coarse params: {best_params} ({best_score:.2f}%)")

    # --- Fine-tune around best ---
    if best_params:
        best_k, best_l, best_m, best_u = int(best_params['K_PERIOD']), int(best_params['LOWER']), int(best_params['MID']), int(best_params['UPPER'])
        for _ in range(fine_tune):
            k_period = max(5, min(30, best_k + random.randint(-2, 2)))
            lower = max(5, min(45, best_l + random.randint(-2, 2)))
            upper = max(lower + 10, min(95, best_u + random.randint(-2, 2)))
            mid = max(lower + 1, min(upper - 1, best_m + random.randint(-2, 2)))
            df_stoch = compute_stoch(df, k_period)
            if len(df_stoch) < 3:
                continue
            trades = []
            in_position = False
            last_entry = -COOLDOWN_BARS
            for i in range(1, len(df_stoch)):
                k_prev = df_stoch["K"].iloc[i-1]
                k_now = df_stoch["K"].iloc[i]
                if not in_position and (k_prev < lower and k_now > lower) and (i - last_entry >= COOLDOWN_BARS):
                    entry = df_stoch['c'].iloc[i]
                    trades.append({'entry': entry, 'entry_idx': i})
                    in_position = True
                    last_entry = i
                elif in_position and (
                    (k_prev < upper and k_now > upper) or
                    (k_prev > mid and k_now < mid) or
                    (k_prev > lower and k_now < lower)
                ):
                    trades[-1]['exit'] = df_stoch['c'].iloc[i]
                    in_position = False
            total = Decimal("1")
            for t in trades:
                if 'exit' in t:
                    r = (t['exit'] - t['entry']) / t['entry'] + Decimal("1")
                    total *= r
            pct = (total - Decimal("1")) * 100
            if pct > best_score:
                best_score = pct
                best_params = {'K_PERIOD': k_period, 'LOWER': Decimal(lower), 'MID': Decimal(mid), 'UPPER': Decimal(upper)}
        log(f"[Fine Tune] Best fine-tuned params: {best_params} ({best_score:.2f}%)")
    return best_params, best_score


def get_trading_params():
    candles = load_recent_candles()
    df = pd.DataFrame(candles)
    for col in ["o","h","l","c","v"]:
        df[col] = df[col].astype(str).map(safe_dec)
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df.set_index('ts', inplace=True)
    params, perf = backtest_params(df)
    log(f"Optimal params after backtest: {params} (Return: {perf:.2f}%)")
    return params

def process_candle(candle, state, is_closed):
    # On close, store the candle in the file
    if is_closed:
        append_candle(candle)
    # On close, prune (optional: keep up to 48h in file)
    # On *every* update, recalc (with latest forming candle appended if not closed)
    candles = load_recent_candles()
    if not is_closed:
        # Use the live candle (not yet closed) for current calculation
        candles.append(candle)
    df = pd.DataFrame(candles)
    for col in ["o","h","l","c","v"]:
        df[col] = df[col].astype(str).map(safe_dec)
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df.set_index('ts', inplace=True)

    # No signal if not enough candles
    if len(df) < state['params']['K_PERIOD'] + 2:
        return state, state.get('cooldown', 0)

    df = compute_stoch(df, state['params']['K_PERIOD'])
    if len(df) < 2:
        return state, state.get('cooldown', 0)
    signal = check_signals(df, state['params'])
    price = df['c'].iloc[-1]
    step = get_lot_size(SYMBOL)
    qty = round_down(ORDER_QUANTITY / price, step)
    # For cooldown management per *candle* not per tick
    curr_ts = int(df.index[-1].timestamp())

    # State defaults
    if "last_action_ts" not in state:
        state["last_action_ts"] = 0
    if "cooldown" not in state:
        state["cooldown"] = 0

    # Decrement cooldown
    if state['cooldown'] > 0:
        state['cooldown'] -= 1
        return state, state['cooldown']

    # Only act once per candle, per side
    if not state['in_position'] and signal["buy"] and curr_ts != state["last_action_ts"]:
        trade_ok = place_market_order(SYMBOL, Client.SIDE_BUY, qty)
        if trade_ok:
            log(f"BUY: {qty} {SYMBOL} @ {price} ({signal['reason']}) [intra-candle]")
            state['in_position'] = True
            state['cooldown'] = COOLDOWN_BARS
            state["last_action_ts"] = curr_ts
    elif state['in_position'] and signal["sell"] and curr_ts != state["last_action_ts"]:
        trade_ok = place_market_order(SYMBOL, Client.SIDE_SELL, qty)
        if trade_ok:
            log(f"SELL: {qty} {SYMBOL} @ {price} ({signal['reason']}) [intra-candle]")
            state['in_position'] = False
            state['cooldown'] = COOLDOWN_BARS
            state["last_action_ts"] = curr_ts
            # Only re-optimize after sell
            state['params'] = get_trading_params()
            log(f"Parameters updated after sell.")
    else:
        # If you want, log live state here
        pass
    return state, state['cooldown']

def kline_callback(msg):
    k = msg['k']
    is_closed = k['x']
    candle = {
        "ts": int(k['t']),
        "o": k['o'], "h": k['h'],
        "l": k['l'], "c": k['c'], "v": k['v']
    }
    global bot_state
    bot_state, _ = process_candle(candle, bot_state, is_closed)

def main():
    fill_history_if_needed()
    global bot_state
    bot_state = {
        'in_position': False,
        'cooldown': 0,
        'params': get_trading_params(),
        'last_action_ts': 0
    }
    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start()
    twm.start_kline_socket(callback=kline_callback, symbol=SYMBOL, interval=INTERVAL)
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
