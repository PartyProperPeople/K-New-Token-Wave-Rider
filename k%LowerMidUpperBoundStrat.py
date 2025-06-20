import os, json, time, random
import pandas as pd
from datetime import datetime, timedelta, UTC
from decimal import Decimal, getcontext, ROUND_DOWN
from binance.client import Client
from binance import ThreadedWebsocketManager
import keyring

# --- CONFIG ---
KEYRING_SERVICE = "ema-grid-bot"
API_KEY    = keyring.get_password(KEYRING_SERVICE, "API_KEY")
API_SECRET = keyring.get_password(KEYRING_SERVICE, "API_SECRET")
SYMBOL     = "PEPEUSDT"
INTERVAL   = "5m"
CANDLE_FILE = "ohlc_5m.jsonl"
CANDLE_LIMIT_SEC = 4 * 60 * 60  # 48 hours
COOLDOWN_BARS = 4
N_COMBOS = 100000
FINE_TUNE = 2000
USDT_PCT = Decimal("0.9")
MIN_USDT_BAL = Decimal("5")
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

def get_lot_size_info(symbol):
    info = client.get_symbol_info(symbol)
    lot_size = {}
    for f in info["filters"]:
        if f["filterType"] == "LOT_SIZE":
            lot_size = {
                "stepSize": Decimal(f["stepSize"]),
                "minQty": Decimal(f["minQty"]),
                "maxQty": Decimal(f["maxQty"]),
                "decimals": abs(Decimal(f["stepSize"]).normalize().as_tuple().exponent)
            }
    return lot_size

def get_min_notional(symbol):
    info = client.get_symbol_info(symbol)
    for f in info["filters"]:
        if f["filterType"] == "MIN_NOTIONAL":
            return Decimal(f["minNotional"])
    return Decimal("0")

def adjust_qty(qty, symbol):
    lot = get_lot_size_info(symbol)
    step = lot["stepSize"]
    qty = (qty // step) * step
    decimals = lot["decimals"]
    qty = qty.quantize(Decimal('1e-{0}'.format(decimals)), rounding=ROUND_DOWN)
    if qty < lot["minQty"]:
        return Decimal("0")
    return qty

def get_base_qty_to_buy(price, symbol, usdt_pct=Decimal("0.9")):
    try:
        free = Decimal(client.get_asset_balance("USDT")["free"])
    except Exception as e:
        log(f"BALANCE ERROR: {e}")
        return Decimal("0")
    spend = (free * usdt_pct).quantize(Decimal("0.00000001"))
    qty = spend / price
    qty = adjust_qty(qty, symbol)
    min_notional = get_min_notional(symbol)
    if (qty * price) < min_notional or qty <= 0:
        return Decimal("0")
    return qty

def get_full_base_qty(symbol):
    base = symbol.replace("USDT", "")
    try:
        qty = Decimal(client.get_asset_balance(base)["free"])
        qty = adjust_qty(qty, symbol)
        min_notional = get_min_notional(symbol)
        price = Decimal(client.get_symbol_ticker(symbol=symbol)["price"])
        if (qty * price) < min_notional or qty <= 0:
            return Decimal("0")
        return qty
    except Exception as e:
        log(f"BASE BAL ERROR: {e}")
        return Decimal("0")

def compute_stoch(df, k_period):
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

def check_signals(df, params, usdt_balance):
    bounds = [
        params['LOWER'],
        params['MID_LOWER'],
        params['MID'],
        params['MID_UPPER'],
        params['UPPER'],
    ]
    names = ['LOWER', 'MID_LOWER', 'MID', 'MID_UPPER', 'UPPER']
    k_prev, k_now = df["K"].iloc[-2], df["K"].iloc[-1]
    signals = {"buy": False, "buy_level": None, "sell": False, "sell_level": None, "reason": None}
    # BUY: Cross up of LOWER or MID_LOWER (if enough USDT)
    if k_prev < bounds[0] and k_now > bounds[0] and usdt_balance > MIN_USDT_BAL:
        signals["buy"] = True
        signals["buy_level"] = names[0]
        signals["reason"] = f"K crosses up {names[0]}"
    elif k_prev < bounds[1] and k_now > bounds[1] and usdt_balance > MIN_USDT_BAL:
        signals["buy"] = True
        signals["buy_level"] = names[1]
        signals["reason"] = f"K crosses up {names[1]}"
    # SELL: Cross up of UPPER
    elif k_prev < bounds[4] and k_now > bounds[4]:
        signals["sell"] = True
        signals["sell_level"] = names[4]
        signals["reason"] = f"K crosses up {names[4]}"
    # SELL: Cross down of ANY bound
    else:
        for idx, b in enumerate(bounds):
            if k_prev > b and k_now < b:
                signals["sell"] = True
                signals["sell_level"] = names[idx]
                signals["reason"] = f"K crosses down {names[idx]}"
                break
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

def backtest_params(df, n_combos=N_COMBOS, fine_tune=FINE_TUNE):
    best_score = -999
    best_params = None
    for _ in range(n_combos):
        k_period = random.randint(8, 20)
        lower = random.randint(5, 20)
        mid_lower = random.randint(lower+1, lower+15)
        mid = random.randint(mid_lower+1, mid_lower+15)
        mid_upper = random.randint(mid+1, mid+15)
        upper = random.randint(mid_upper+1, 90)
        if not (lower < mid_lower < mid < mid_upper < upper):
            continue
        df_stoch = compute_stoch(df, k_period)
        if len(df_stoch) < 3:
            continue
        trades = []
        in_position = False
        last_entry = -COOLDOWN_BARS
        usdt_balance = Decimal("100")  # Simulated for backtest
        for i in range(1, len(df_stoch)):
            k_prev = df_stoch["K"].iloc[i-1]
            k_now = df_stoch["K"].iloc[i]
            signals = {"buy": False, "sell": False}
            if k_prev < lower and k_now > lower:
                signals["buy"] = True
            elif k_prev < mid_lower and k_now > mid_lower:
                signals["buy"] = True
            elif k_prev < upper and k_now > upper:
                signals["sell"] = True
            elif (k_prev > lower and k_now < lower) or (k_prev > mid_lower and k_now < mid_lower) or (k_prev > mid and k_now < mid) or (k_prev > mid_upper and k_now < mid_upper) or (k_prev > upper and k_now < upper):
                signals["sell"] = True
            if not in_position and signals["buy"] and (i - last_entry >= COOLDOWN_BARS):
                entry = df_stoch['c'].iloc[i]
                trades.append({'entry': entry, 'entry_idx': i})
                in_position = True
                last_entry = i
            elif in_position and signals["sell"]:
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
            best_params = {
                'K_PERIOD': k_period,
                'LOWER': Decimal(lower),
                'MID_LOWER': Decimal(mid_lower),
                'MID': Decimal(mid),
                'MID_UPPER': Decimal(mid_upper),
                'UPPER': Decimal(upper)
            }
    log(f"[Random Search] Best coarse params: {best_params} ({best_score:.2f}%)")
    if best_params:
        best_k = int(best_params['K_PERIOD'])
        best_b = [
            int(best_params['LOWER']),
            int(best_params['MID_LOWER']),
            int(best_params['MID']),
            int(best_params['MID_UPPER']),
            int(best_params['UPPER'])
        ]
        for _ in range(fine_tune):
            k_period = max(5, min(30, best_k + random.randint(-2, 2)))
            lower = max(1, best_b[0] + random.randint(-2, 2))
            mid_lower = max(lower+1, best_b[1] + random.randint(-2, 2))
            mid = max(mid_lower+1, best_b[2] + random.randint(-2, 2))
            mid_upper = max(mid+1, best_b[3] + random.randint(-2, 2))
            upper = max(mid_upper+1, best_b[4] + random.randint(-2, 2))
            if not (lower < mid_lower < mid < mid_upper < upper):
                continue
            df_stoch = compute_stoch(df, k_period)
            if len(df_stoch) < 3:
                continue
            trades = []
            in_position = False
            last_entry = -COOLDOWN_BARS
            usdt_balance = Decimal("100")
            for i in range(1, len(df_stoch)):
                k_prev = df_stoch["K"].iloc[i-1]
                k_now = df_stoch["K"].iloc[i]
                signals = {"buy": False, "sell": False}
                if k_prev < lower and k_now > lower:
                    signals["buy"] = True
                elif k_prev < mid_lower and k_now > mid_lower:
                    signals["buy"] = True
                elif k_prev < upper and k_now > upper:
                    signals["sell"] = True
                elif (k_prev > lower and k_now < lower) or (k_prev > mid_lower and k_now < mid_lower) or (k_prev > mid and k_now < mid) or (k_prev > mid_upper and k_now < mid_upper) or (k_prev > upper and k_now < upper):
                    signals["sell"] = True
                if not in_position and signals["buy"] and (i - last_entry >= COOLDOWN_BARS):
                    entry = df_stoch['c'].iloc[i]
                    trades.append({'entry': entry, 'entry_idx': i})
                    in_position = True
                    last_entry = i
                elif in_position and signals["sell"]:
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
                best_params = {
                    'K_PERIOD': k_period,
                    'LOWER': Decimal(lower),
                    'MID_LOWER': Decimal(mid_lower),
                    'MID': Decimal(mid),
                    'MID_UPPER': Decimal(mid_upper),
                    'UPPER': Decimal(upper)
                }
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
    if is_closed:
        append_candle(candle)
    candles = load_recent_candles()
    if not is_closed:
        candles.append(candle)
    df = pd.DataFrame(candles)
    for col in ["o","h","l","c","v"]:
        df[col] = df[col].astype(str).map(safe_dec)
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df.set_index('ts', inplace=True)
    if len(df) < state['params']['K_PERIOD'] + 2:
        return state, state.get('cooldown', 0)
    df = compute_stoch(df, state['params']['K_PERIOD'])
    if len(df) < 2:
        return state, state.get('cooldown', 0)
    try:
        usdt_balance = Decimal(client.get_asset_balance("USDT")["free"])
    except Exception as e:
        log(f"BALANCE ERROR: {e}")
        usdt_balance = Decimal("0")
    signal = check_signals(df, state['params'], usdt_balance)
    price = df['c'].iloc[-1]
    curr_ts = int(df.index[-1].timestamp())
    if "last_action_ts" not in state:
        state["last_action_ts"] = 0
    if "cooldown" not in state:
        state["cooldown"] = 0
    if state['cooldown'] > 0:
        state['cooldown'] -= 1
        return state, state['cooldown']
    if not state['in_position'] and signal["buy"] and curr_ts != state["last_action_ts"]:
        qty = get_base_qty_to_buy(price, SYMBOL, USDT_PCT)
        if qty <= 0:
            log("Not enough USDT to trade or qty below minQty.")
            return state, state['cooldown']
        trade_ok = place_market_order(SYMBOL, Client.SIDE_BUY, qty)
        if trade_ok:
            log(f"BUY: {qty} {SYMBOL} @ {price} ({signal['reason']}) [intra-candle]")
            state['in_position'] = True
            state['cooldown'] = COOLDOWN_BARS
            state["last_action_ts"] = curr_ts
    elif state['in_position'] and signal["sell"] and curr_ts != state["last_action_ts"]:
        qty = get_full_base_qty(SYMBOL)
        if qty <= 0:
            log("No base asset to sell or qty below minQty.")
            return state, state['cooldown']
        trade_ok = place_market_order(SYMBOL, Client.SIDE_SELL, qty)
        if trade_ok:
            log(f"SELL: {qty} {SYMBOL} @ {price} ({signal['reason']}) [intra-candle]")
            state['in_position'] = False
            state['cooldown'] = COOLDOWN_BARS
            state["last_action_ts"] = curr_ts
            state['params'] = get_trading_params()
            log(f"Parameters updated after sell.")
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
