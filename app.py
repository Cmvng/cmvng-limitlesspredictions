# ═══════════════════════════════════════════════════════════════════════════════
# CMVNG BOT v2 — CONFIRMATION TRADING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
# Philosophy: NOT prediction trading. CONFIRMATION trading.
# Wait for candle to form. Confirm direction won't reverse. Enter late at high odds.
# MECHANICAL — pure coded rules. Zero API cost. Instant execution.
# ═══════════════════════════════════════════════════════════════════════════════

from flask import Flask, request, jsonify, render_template_string
import pg8000.native
import os
import re
import threading
import time
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════
# ENVIRONMENT VARIABLES
# ═══════════════════════════════════════════════════════════

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")

# Polymarket CLOB API credentials
POLY_API_KEY       = os.environ.get("POLY_API_KEY", "")
POLY_API_SECRET    = os.environ.get("POLY_API_SECRET", "")
POLY_API_PASSPHRASE = os.environ.get("POLY_API_PASSPHRASE", "")
POLY_FUNDER_ADDRESS = os.environ.get("POLY_FUNDER_ADDRESS", "")
POLY_PROXY_URL     = os.environ.get("POLY_PROXY_URL", "")
LIMITLESS_PRIV_KEY = os.environ.get("LIMITLESS_PRIVATE_KEY", "")

# ═══════════════════════════════════════════════════════════
# PROXY PATCH — must happen before ClobClient import
# ═══════════════════════════════════════════════════════════

_POLY_PROXY_PATCHED = False
if POLY_PROXY_URL:
    print("[STARTUP] Patching py_clob_client_v2 with proxy: {}...".format(POLY_PROXY_URL[:30]))
    try:
        import httpx as _early_httpx
        try:
            from py_clob_client_v2.http_helpers import helpers as _early_v2h
            _early_v2h._http_client = _early_httpx.Client(
                http2=True, proxy=POLY_PROXY_URL, timeout=30.0,
            )
            _POLY_PROXY_PATCHED = True
            print("[STARTUP] ✓ proxy patched")
        except ImportError as _ie:
            print("[STARTUP] ✗ py_clob_client_v2 not available — {}".format(_ie))
    except Exception as _pe:
        print("[STARTUP] ✗ Proxy pre-patch failed: {}".format(_pe))
else:
    print("[STARTUP] No POLY_PROXY_URL — direct connection")

# ═══════════════════════════════════════════════════════════
# CONSTANTS & CACHES
# ═══════════════════════════════════════════════════════════

LAGOS_TZ = timezone(timedelta(hours=1))
LIMITLESS_API = "https://api.limitless.exchange"
POLY_GAMMA_API = "https://gamma-api.polymarket.com"

BINANCE_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT",
    "XRP": "XRPUSDT", "DOGE": "DOGEUSDT", "ADA": "ADAUSDT",
    "BNB": "BNBUSDT", "AVAX": "AVAXUSDT", "LINK": "LINKUSDT",
    "DOT": "DOTUSDT", "LTC": "LTCUSDT", "BCH": "BCHUSDT",
    "XLM": "XLMUSDT", "UNI": "UNIUSDT", "ATOM": "ATOMUSDT",
    "NEAR": "NEARUSDT", "OP": "OPUSDT", "ARB": "ARBUSDT",
    "TRX": "TRXUSDT", "TON": "TONUSDT", "ONDO": "ONDOUSDT",
    "XMR": "XMRUSDT", "ZEC": "ZECUSDT", "APT": "APTUSDT",
    "HYPE": "HYPEUSDT", "MNT": "MNTUSDT",
}

YAHOO_MAP = {
    "BTC":"BTC-USD", "ETH":"ETH-USD", "SOL":"SOL-USD",
    "ADA":"ADA-USD", "BNB":"BNB-USD", "DOGE":"DOGE-USD",
    "XRP":"XRP-USD", "AVAX":"AVAX-USD","LINK":"LINK-USD",
    "LTC":"LTC-USD", "BCH":"BCH-USD", "XLM":"XLM-USD",
    "ZEC":"ZEC-USD", "ONDO":"ONDO-USD",
    "DOT":"DOT-USD", "UNI":"UNI-USD", "ATOM":"ATOM-USD",
    "TRX":"TRX-USD", "APT":"APT-USD", "ARB":"ARB-USD",
    "OP":"OP-USD", "NEAR":"NEAR-USD","TON":"TON-USD",
}

# Chainlink RTDS caches
_chainlink_prices = {}   # {"BTC": 78900.50, ...}
_chainlink_ptb = {}      # {"BTC_15M": (end_ts, price), ...}
_chainlink_connected = False


# ═══════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════

def get_db():
    import urllib.parse
    db_url = DATABASE_URL.replace('postgres://', 'postgresql://')
    url = urllib.parse.urlparse(db_url)
    return pg8000.native.Connection(
        host=url.hostname, port=url.port or 5432,
        database=url.path.lstrip('/'),
        user=url.username, password=url.password,
        ssl_context=True
    )

def init_db():
    conn = get_db()
    # v2 paper trades table — confirmation trading
    conn.run("""
        CREATE TABLE IF NOT EXISTS v2_paper_trades (
            id              SERIAL PRIMARY KEY,
            platform        TEXT NOT NULL,
            timeframe       TEXT NOT NULL,
            asset           TEXT NOT NULL,
            direction       TEXT NOT NULL,
            ptb             REAL,
            entry_odds      REAL,
            entry_price     REAL,
            stake           REAL DEFAULT 2.50,
            entry_note      TEXT,
            hh_count        INTEGER,
            hl_count        INTEGER,
            ll_count        INTEGER,
            lh_count        INTEGER,
            grind_rate      TEXT,
            ptb_distance    REAL,
            session_label   TEXT,
            volatility      TEXT,
            prev_candle     TEXT,
            hedged          BOOLEAN DEFAULT FALSE,
            hedge_odds      REAL,
            hedge_direction TEXT,
            hedge_note      TEXT,
            hedge_pnl       REAL,
            market_id       TEXT,
            slug            TEXT,
            condition_id    TEXT,
            up_token        TEXT,
            down_token      TEXT,
            open_price      REAL,
            close_price     REAL,
            actual_result   TEXT,
            outcome         TEXT,
            pnl             REAL,
            balance_after   REAL,
            status          TEXT DEFAULT 'OPEN',
            fired_at        TIMESTAMPTZ DEFAULT NOW(),
            resolved_at     TIMESTAMPTZ,
            confidence      TEXT,
            market_url      TEXT,
            limit_price     REAL,
            book_ask        REAL,
            filled_at       TIMESTAMPTZ,
            order_status    TEXT DEFAULT 'FILLED'
        )
    """)
    # Migrations for existing DBs
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS market_url TEXT")
    except:
        pass
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS limit_price REAL")
    except:
        pass
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS book_ask REAL")
    except:
        pass
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS filled_at TIMESTAMPTZ")
    except:
        pass
    try:
        conn.run("ALTER TABLE v2_paper_trades ADD COLUMN IF NOT EXISTS order_status TEXT DEFAULT 'FILLED'")
    except:
        pass
    # v2 balance tracking
    conn.run("""
        CREATE TABLE IF NOT EXISTS v2_balances (
            id              SERIAL PRIMARY KEY,
            platform        TEXT NOT NULL,
            balance         REAL DEFAULT 100.0,
            peak_balance    REAL DEFAULT 100.0,
            wins            INTEGER DEFAULT 0,
            losses          INTEGER DEFAULT 0,
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Insert default balances if not exist
    for platform in ["polymarket", "limitless"]:
        try:
            existing = conn.run(
                "SELECT id FROM v2_balances WHERE platform = :p", p=platform)
            if not list(existing):
                conn.run(
                    "INSERT INTO v2_balances (platform, balance, peak_balance) VALUES (:p, 50.0, 50.0)",
                    p=platform)
        except:
            pass
    conn.close()
    print("[V2] Database initialized")


def reset_db():
    """Reset all paper trades and balances for a fresh start."""
    try:
        conn = get_db()
        # Delete all existing trades
        conn.run("DELETE FROM v2_paper_trades")
        # Reset balances to $50 each
        conn.run("UPDATE v2_balances SET balance = 50.0, peak_balance = 50.0, wins = 0, losses = 0, updated_at = NOW()")
        conn.close()
        # Reset in-memory balances
        _v2_balances["polymarket"] = {"balance": 50.0, "peak_balance": 50.0, "wins": 0, "losses": 0}
        _v2_balances["limitless"] = {"balance": 50.0, "peak_balance": 50.0, "wins": 0, "losses": 0}
        print("[V2] *** DATABASE RESET — all trades cleared, balances reset to $50 ***")
    except Exception as e:
        print("[V2] Reset error: {}".format(e))


# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    def _send():
        try:
            import requests
            requests.post(
                "https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_TOKEN),
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
                timeout=10
            )
        except Exception as e:
            print("Telegram error: {}".format(e))
    threading.Thread(target=_send, daemon=True).start()


# ═══════════════════════════════════════════════════════════
# BINANCE DATA
# ═══════════════════════════════════════════════════════════

def _fetch_binance_candles(asset, interval="15m", limit=100):
    """Fetch OHLCV candles from Binance. Returns list of dicts with o,h,l,c,v,t."""
    import requests as req
    symbol = BINANCE_MAP.get(asset.upper())
    if not symbol:
        return None
    try:
        r = req.get("https://api.binance.com/api/v3/klines",
                     params={"symbol": symbol, "interval": interval, "limit": limit},
                     timeout=3)
        if r.status_code != 200:
            return None
        klines = r.json()
        if not klines or len(klines) < 5:
            return None
        candles = []
        for k in klines:
            candles.append({
                "o": float(k[1]), "h": float(k[2]),
                "l": float(k[3]), "c": float(k[4]),
                "v": float(k[5]), "t": int(k[0]),
            })
        return candles
    except Exception as e:
        print("Binance candle error {} {}: {}".format(asset, interval, e))
        return None

def _get_binance_price(asset):
    """Get current price from Binance — instant."""
    import requests as req
    symbol = BINANCE_MAP.get(asset.upper())
    if not symbol:
        return None
    try:
        r = req.get("https://api.binance.com/api/v3/ticker/price",
                     params={"symbol": symbol}, timeout=2)
        if r.status_code == 200:
            return float(r.json().get("price", 0))
    except:
        pass
    return None

def get_price(asset):
    """Get current price — Binance first, yfinance fallback."""
    bp = _get_binance_price(asset)
    if bp and bp > 0:
        return bp
    try:
        import yfinance as yf
        symbol = YAHOO_MAP.get(asset.upper())
        if not symbol:
            return None
        ticker = yf.Ticker(symbol)
        try:
            price = ticker.fast_info.last_price
            if price and price > 0:
                return float(price)
        except:
            pass
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        print("yfinance error {}: {}".format(asset, e))
    return None


# ═══════════════════════════════════════════════════════════
# POLYMARKET CLOB CLIENT (singleton)
# ═══════════════════════════════════════════════════════════

_poly_clob_client = None

def _poly_has_creds():
    return bool(POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE
                and LIMITLESS_PRIV_KEY and POLY_FUNDER_ADDRESS)

def _get_poly_client():
    """Get or create Polymarket CLOB client."""
    global _poly_clob_client
    if _poly_clob_client is not None:
        return _poly_clob_client
    if not _poly_has_creds():
        return None
    try:
        try:
            from py_clob_client_v2 import ClobClient, ApiCreds
            client = ClobClient(
                host="https://clob.polymarket.com",
                chain_id=137,
                key=LIMITLESS_PRIV_KEY,
                signature_type=2,
                funder=POLY_FUNDER_ADDRESS,
                creds=ApiCreds(
                    api_key=POLY_API_KEY,
                    api_secret=POLY_API_SECRET,
                    api_passphrase=POLY_API_PASSPHRASE,
                ),
            )
            _poly_clob_client = client
            print("Polymarket CLOB V2 client initialized")
        except ImportError:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
            creds = ApiCreds(
                api_key=POLY_API_KEY,
                api_secret=POLY_API_SECRET,
                api_passphrase=POLY_API_PASSPHRASE
            )
            client = ClobClient(
                "https://clob.polymarket.com",
                key=LIMITLESS_PRIV_KEY,
                chain_id=137, signature_type=2,
                funder=POLY_FUNDER_ADDRESS,
            )
            client.set_api_creds(creds)
            _poly_clob_client = client
            print("Polymarket CLOB V1 client initialized")
        # Proxy injection
        if POLY_PROXY_URL and not _POLY_PROXY_PATCHED:
            try:
                import httpx as _httpx
                from py_clob_client_v2.http_helpers import helpers as _v2h
                _v2h._http_client = _httpx.Client(
                    http2=True, proxy=POLY_PROXY_URL, timeout=30.0,
                )
                print("Polymarket proxy injected at client init")
            except:
                pass
        return _poly_clob_client
    except Exception as e:
        print("CLOB init error: {}".format(e))
        return None


# ═══════════════════════════════════════════════════════════
# CHAINLINK RTDS WEBSOCKET
# ═══════════════════════════════════════════════════════════

POLY_RTDS_URL = "wss://ws-live-data.polymarket.com"

def _rtds_price_to_beat(asset, timeframe, end_ts):
    """Get the Price to Beat from Chainlink boundary capture."""
    key = "{}_{}".format(asset, timeframe)
    entry = _chainlink_ptb.get(key)
    if entry:
        stored_ts, stored_price = entry
        tf_sec = {"5M": 300, "15M": 900, "1H": 3600, "DAILY": 86400}.get(timeframe, 300)
        if abs(stored_ts - end_ts) <= tf_sec * 2:
            return stored_price
    return None

def _rtds_current_price(asset):
    return _chainlink_prices.get(asset)

def _rtds_loop():
    """Background thread: Chainlink RTDS WebSocket for real-time prices."""
    global _chainlink_connected
    import websocket

    pair_map = {
        "btc/usd": "BTC", "eth/usd": "ETH", "sol/usd": "SOL",
        "xrp/usd": "XRP", "doge/usd": "DOGE", "bnb/usd": "BNB",
        "hype/usd": "HYPE", "ada/usd": "ADA", "avax/usd": "AVAX",
        "link/usd": "LINK", "dot/usd": "DOT", "ltc/usd": "LTC",
    }
    _msg_count = [0]

    def _store_ptb(asset, price, ts_sec):
        for tf_label, tf_sec, max_delay in [("5M", 300, 5), ("15M", 900, 10), ("1H", 3600, 10)]:
            window_start = (ts_sec // tf_sec) * tf_sec
            window_end = window_start + tf_sec
            key = "{}_{}".format(asset, tf_label)
            existing = _chainlink_ptb.get(key)
            if ts_sec - window_start <= max_delay:
                if not existing or existing[0] != window_end:
                    _chainlink_ptb[key] = (window_end, price)

    def on_message(ws, message):
        global _chainlink_connected
        _chainlink_connected = True
        try:
            if message == "PONG":
                return
            _msg_count[0] += 1
            if message.startswith("{") or message.startswith("["):
                data = json.loads(message)
                if isinstance(data, dict):
                    payload = data.get("payload")
                    if payload and isinstance(payload, dict):
                        symbol = (payload.get("symbol") or "").lower()
                        value = payload.get("value") or payload.get("price")
                        ts = payload.get("timestamp") or data.get("timestamp") or 0
                        if symbol and value:
                            price = float(value)
                            asset = pair_map.get(symbol)
                            if asset:
                                _chainlink_prices[asset] = price
                                ts_sec = int(ts) // 1000 if ts > 1e12 else int(ts) if isinstance(ts, (int, float)) else int(time.time())
                                _store_ptb(asset, price, ts_sec)
                return
            parts = message.split(",")
            if len(parts) >= 4:
                ts_ms = int(parts[0])
                pair = parts[2].strip()
                price = float(parts[3].strip())
                asset = pair_map.get(pair)
                if asset:
                    _chainlink_prices[asset] = price
                    _store_ptb(asset, price, ts_ms // 1000)
        except Exception as e:
            if _msg_count[0] <= 5:
                print("RTDS parse error: {}".format(e))

    def on_error(ws, error):
        global _chainlink_connected
        _chainlink_connected = False
        print("RTDS error: {}".format(error))

    def on_close(ws, close_status, close_msg):
        global _chainlink_connected
        _chainlink_connected = False

    def on_open(ws):
        global _chainlink_connected
        _chainlink_connected = True
        sub = json.dumps({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "filters": {}
            }]
        })
        ws.send(sub)
        print("RTDS connected + subscribed")

    while True:
        try:
            ws = websocket.WebSocketApp(
                POLY_RTDS_URL,
                on_message=on_message, on_error=on_error,
                on_close=on_close, on_open=on_open
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print("RTDS loop error: {}".format(e))
        time.sleep(5)


# ═══════════════════════════════════════════════════════════
# POLYMARKET MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════

def _poly_parse_market(market, timeframe_hint=None):
    """Parse a Polymarket crypto Up/Down market from Gamma API data."""
    try:
        question = market.get("question") or market.get("title") or ""
        slug = market.get("slug") or ""
        condition_id = market.get("conditionId") or market.get("condition_id") or ""
        slug_lower = slug.lower()
        q_lower = question.lower()

        # Detect asset
        asset = None
        asset_patterns = [
            (["btc-", "bitcoin-"], "BTC"), (["eth-", "ethereum-"], "ETH"),
            (["sol-", "solana-"], "SOL"), (["xrp-"], "XRP"),
            (["doge-", "dogecoin-"], "DOGE"), (["hype-", "hyperliquid-"], "HYPE"),
            (["bnb-"], "BNB"), (["ada-", "cardano-"], "ADA"),
            (["avax-", "avalanche-"], "AVAX"), (["link-", "chainlink-"], "LINK"),
        ]
        for prefixes, sym in asset_patterns:
            if any(p in slug_lower for p in prefixes):
                asset = sym
                break
        if not asset:
            for word, sym in [("bitcoin", "BTC"), ("ethereum", "ETH"), ("solana", "SOL"),
                              ("xrp", "XRP"), ("dogecoin", "DOGE"), ("hyperliquid", "HYPE")]:
                if word in q_lower:
                    asset = sym
                    break
        if not asset:
            return None
        if "up or down" not in q_lower and "updown" not in slug_lower:
            return None

        # Expiry
        now = datetime.now(timezone.utc)
        end_date = market.get("endDate") or market.get("end_date_iso") or ""
        exp_ts = market.get("expirationTimestamp") or market.get("expiration_timestamp")
        expiry_dt = None
        if exp_ts:
            try:
                if isinstance(exp_ts, str): exp_ts = int(exp_ts)
                if exp_ts > 1e12: exp_ts = exp_ts / 1000
                expiry_dt = datetime.fromtimestamp(exp_ts, tz=timezone.utc)
            except:
                exp_ts = None
        if not expiry_dt and end_date:
            try:
                expiry_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                exp_ts = int(expiry_dt.timestamp())
            except:
                return None
        if not expiry_dt:
            return None
        mins_left = (expiry_dt - now).total_seconds() / 60
        if mins_left <= 0:
            return None

        # Timeframe
        timeframe = None
        if "-15m-" in slug_lower or "-15m" in slug_lower:
            timeframe = "15M"
        elif "-5m-" in slug_lower or "-5m" in slug_lower:
            timeframe = "5M"
        elif "-1h-" in slug_lower or "-1h" in slug_lower or "hourly" in slug_lower:
            timeframe = "1H"
        # Detect 1H from "up-or-down-{month}-{day}" format (no -5m/-15m suffix)
        elif "up-or-down-" in slug_lower and "-updown-" not in slug_lower and "up-or-down-on-" not in slug_lower:
            timeframe = "1H"
        # Daily: "bitcoin-up-or-down-on-may-27"
        elif "up-or-down-on-" in slug_lower:
            timeframe = "DAILY"
        if not timeframe:
            created = market.get("createdAt") or ""
            if created and exp_ts:
                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    dur = (expiry_dt - created_dt).total_seconds() / 60
                    if 55 <= dur <= 65: timeframe = "1H"
                    elif 13 <= dur <= 17: timeframe = "15M"
                    elif 4 <= dur <= 6: timeframe = "5M"
                    elif dur > 600: timeframe = "DAILY"
                except:
                    pass
        if not timeframe and timeframe_hint:
            timeframe = timeframe_hint
        if not timeframe:
            return None

        # Odds
        outcome_prices = market.get("outcomePrices") or market.get("outcome_prices")
        up_odds = 50.0
        if outcome_prices:
            if isinstance(outcome_prices, str):
                try: outcome_prices = json.loads(outcome_prices)
                except: outcome_prices = None
            if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                up_raw = float(outcome_prices[0])
                up_odds = up_raw * 100 if up_raw <= 1.0 else up_raw

        # Token IDs + outcome ordering
        clob_tokens = market.get("clobTokenIds")
        if isinstance(clob_tokens, str):
            try: clob_tokens = json.loads(clob_tokens)
            except: clob_tokens = []
        outcomes_raw = market.get("outcomes")
        if isinstance(outcomes_raw, str):
            try: outcomes_raw = json.loads(outcomes_raw)
            except: outcomes_raw = None
        up_index, down_index = 0, 1
        if isinstance(outcomes_raw, list) and len(outcomes_raw) >= 2:
            o0 = str(outcomes_raw[0]).lower().strip()
            if o0 in ("no", "down", "below"):
                up_index, down_index = 1, 0
                if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                    up_raw = float(outcome_prices[1])
                    up_odds = up_raw * 100 if up_raw <= 1.0 else up_raw

        # Baseline from market text
        baseline = None
        for field in ["question", "description", "resolutionSource", "rules", "title"]:
            text = str(market.get(field) or "")
            if "$" in text:
                all_prices = re.findall(r'\$([0-9,]+\.?\d*)', text)
                for p in all_prices:
                    try:
                        val = float(p.replace(",", ""))
                        if asset == "BTC" and 10000 < val < 200000: baseline = val; break
                        elif asset == "ETH" and 500 < val < 10000: baseline = val; break
                        elif asset == "SOL" and 5 < val < 500: baseline = val; break
                        elif asset == "XRP" and 0.1 < val < 10: baseline = val; break
                        elif asset == "DOGE" and 0.01 < val < 2: baseline = val; break
                    except:
                        pass
                if baseline: break

        market_id = str(market.get("id") or condition_id or slug)
        up_token = str(clob_tokens[up_index]) if clob_tokens and len(clob_tokens) > up_index else ""
        down_token = str(clob_tokens[down_index]) if clob_tokens and len(clob_tokens) > down_index else ""

        return {
            "market_id": market_id, "title": question, "asset": asset,
            "baseline": baseline, "expiry_dt": expiry_dt,
            "mins_left": mins_left, "hours_left": mins_left / 60,
            "yes_odds": up_odds, "slug": slug,
            "condition_id": condition_id, "timeframe": timeframe,
            "clob_tokens": clob_tokens or [],
            "up_token": up_token, "down_token": down_token,
            "up_token_index": up_index, "down_token_index": down_index,
        }
    except Exception as e:
        print("POLY PARSE ERR: {}".format(e))
        return None


def _poly_fetch_markets():
    """Fetch active crypto Up/Down markets from Polymarket."""
    import requests as req
    now = datetime.now(timezone.utc)
    markets = []
    current_ts = int(now.timestamp())

    # Strategy 1: Public search
    try:
        r = req.get("{}/public-search".format(POLY_GAMMA_API),
                    params={"query": "up or down", "limit": 50}, timeout=12)
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get("events", data.get("markets", data.get("data", []))) if isinstance(data, dict) else []
            for item in items:
                item_markets = []
                if isinstance(item, dict):
                    if item.get("markets"):
                        item_markets = item["markets"]
                    elif item.get("clobTokenIds") or item.get("conditionId"):
                        item_markets = [item]
                for m in item_markets:
                    q = (m.get("question") or m.get("title") or "").lower()
                    if ("up" in q and "down" in q) or "updown" in q:
                        parsed = _poly_parse_market(m)
                        if parsed:
                            markets.append(parsed)
            if markets:
                return markets
    except Exception as e:
        print("Poly search error: {}".format(e))

    # Strategy 2: Slug lookup
    assets = [("btc", "BTC"), ("eth", "ETH"), ("sol", "SOL"), ("xrp", "XRP")]
    _1h_full_names = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "xrp"}
    try:
        from zoneinfo import ZoneInfo
        _ET = ZoneInfo("America/New_York")
    except:
        _ET = timezone(timedelta(hours=-4))
    now_et = now.astimezone(_ET)
    _1h_start = now_et.replace(minute=0, second=0, microsecond=0)
    _1h_h12 = _1h_start.hour % 12 or 12
    _1h_ap = "am" if _1h_start.hour < 12 else "pm"
    _1h_mo = _1h_start.strftime("%B").lower()
    _1h_dy = _1h_start.day
    _1h_yr = _1h_start.year

    for asset_slug, _ in assets:
        slugs = []
        for tf_slug, tf_sec in [("5m", 300), ("15m", 900)]:
            ws = (current_ts // tf_sec) * tf_sec
            slugs.append("{}-updown-{}-{}".format(asset_slug, tf_slug, ws))
        _1h_name = _1h_full_names.get(asset_slug, asset_slug)
        # 1H slug: "bitcoin-up-or-down-may-27-10pm-et" (no year)
        slugs.append("{}-up-or-down-{}-{}-{}{}-et".format(
            _1h_name, _1h_mo, _1h_dy, _1h_h12, _1h_ap))
        # 1H with year fallback
        slugs.append("{}-up-or-down-{}-{}-{}-{}{}-et".format(
            _1h_name, _1h_mo, _1h_dy, _1h_yr, _1h_h12, _1h_ap))
        # Daily slug: "bitcoin-up-or-down-on-may-27" (no year)
        slugs.append("{}-up-or-down-on-{}-{}".format(
            _1h_name, _1h_mo, _1h_dy))
        # Daily with year fallback
        slugs.append("{}-up-or-down-on-{}-{}-{}".format(
            _1h_name, _1h_mo, _1h_dy, _1h_yr))

        for s in slugs:
            _is_daily = "-up-or-down-on-" in s
            _is_1h = "-up-or-down-" in s and not _is_daily
            tf_hint = "DAILY" if _is_daily else "1H" if _is_1h else None
            for url in ["{}/events/slug/{}".format(POLY_GAMMA_API, s),
                        "{}/events".format(POLY_GAMMA_API)]:
                try:
                    params = {"slug": s} if "/slug/" not in url else {}
                    r = req.get(url, params=params, timeout=8)
                    if r.status_code == 200:
                        data = r.json()
                        em = []
                        if isinstance(data, list) and data:
                            em = data[0].get("markets", []) if isinstance(data[0], dict) else []
                        elif isinstance(data, dict):
                            em = data.get("markets", [])
                        for m in em:
                            parsed = _poly_parse_market(m, timeframe_hint=tf_hint)
                            if parsed:
                                markets.append(parsed)
                        if em: break
                except:
                    pass

    if markets:
        return markets

    # Strategy 3: Broad scan
    try:
        r = req.get("{}/markets".format(POLY_GAMMA_API),
                    params={"active": "true", "closed": "false", "limit": 100,
                            "order": "volume24hr", "ascending": "false"}, timeout=15)
        if r.status_code == 200:
            batch = r.json() if isinstance(r.json(), list) else []
            for m in batch:
                q = (m.get("question") or "").lower()
                if "up or down" in q or "updown" in q:
                    parsed = _poly_parse_market(m)
                    if parsed:
                        markets.append(parsed)
    except Exception as e:
        print("Poly broad error: {}".format(e))

    return markets


def _poly_get_baseline(parsed, price=None):
    """Get PTB: (1) market title, (2) Chainlink boundary, (3) stream."""
    asset = parsed.get("asset", "")
    tf = parsed.get("timeframe", "")
    if parsed.get("baseline") and parsed["baseline"] > 0:
        return parsed["baseline"]
    key = "{}_{}".format(asset, tf)
    entry = _chainlink_ptb.get(key)
    if entry:
        return entry[1]
    chainlink = _chainlink_prices.get(asset)
    if chainlink:
        return chainlink
    return price



# ═══════════════════════════════════════════════════════════
# V2 CONFIRMATION ENGINE — CORE ANALYSIS
# ═══════════════════════════════════════════════════════════

def _v2_session_filter(utc_hour):
    """AVOID London/US cross + Peak US + Peak Asia.
    PREFER: Late US/early Asian + Early morning."""
    if 4 <= utc_hour <= 11:
        return "EARLY_MORNING", True
    elif 17 <= utc_hour <= 22:
        return "LATE_US_ASIA", True
    elif 11 <= utc_hour < 12:
        return "PRE_LONDON", True
    elif 12 <= utc_hour <= 17:
        return "US_SESSION", False
    elif 23 <= utc_hour or utc_hour < 4:
        return "PEAK_ASIA", False
    else:
        return "TRANSITION", True


def _v2_analyze_structure(candles):
    """Analyze HH/HL structure from intra-period candles.
    Candle intervals per timeframe (set by caller):
    - Hourly watcher: 15M candles (3 completed at T+45)
    - 15M watcher: 5M candles (2 completed at T+10)
    - Daily watcher: 4H candles (4-5 by quiet hours)

    With 2-5 candles, compare each consecutively for HH/HL/LH/LL.
    Spike = one candle did >50% of the total range."""

    if not candles or len(candles) < 2:
        return None

    # Count HH, HL, LH, LL by comparing consecutive candle highs and lows
    hh_count = 0
    lh_count = 0
    for i in range(1, len(candles)):
        if candles[i]["h"] > candles[i-1]["h"]:
            hh_count += 1
        elif candles[i]["h"] < candles[i-1]["h"]:
            lh_count += 1

    hl_count = 0
    ll_count = 0
    for i in range(1, len(candles)):
        if candles[i]["l"] > candles[i-1]["l"]:
            hl_count += 1
        elif candles[i]["l"] < candles[i-1]["l"]:
            ll_count += 1

    # Spike detection — did one candle do all the work?
    # Measure by BODY MOVE (close - open), not range (high - low)
    # In a steady grind, each candle contributes similar body moves
    # In a spike, one candle has a huge body while others are small/flat
    body_moves = [abs(c["c"] - c["o"]) for c in candles]
    total_body = sum(body_moves)
    max_body = max(body_moves) if body_moves else 0

    if total_body > 0:
        body_concentration = max_body / total_body
    else:
        body_concentration = 0

    # Also check: did the total period move happen gradually?
    total_range = max(c["h"] for c in candles) - min(c["l"] for c in candles)

    # Spike = one candle's body is more than 70% of the total body movement
    # This means one candle did most of the work
    if body_concentration > 0.70:
        grind_type = "spike"
    elif body_concentration < 0.45:
        grind_type = "steady"
    else:
        grind_type = "normal"

    # Direction — HH>=2 AND HL>=2 = clean trend, else FLAT
    if hh_count >= 2 and hl_count >= 2 and hh_count > lh_count:
        direction = "UP"
    elif ll_count >= 2 and lh_count >= 2 and ll_count > hh_count:
        direction = "DOWN"
    elif hh_count >= 2 and hl_count >= 2 and lh_count == 0:
        direction = "UP"
    elif ll_count >= 2 and lh_count >= 2 and hh_count == 0:
        direction = "DOWN"
    else:
        direction = "FLAT"

    return {
        "hh_count": hh_count, "hl_count": hl_count,
        "lh_count": lh_count, "ll_count": ll_count,
        "grind_type": grind_type, "direction": direction,
        "concentration": round(body_concentration, 3),
    }


def _v2_analyze_prev_candle(candle):
    """Analyze previous period's candle for strength/direction."""
    if not candle:
        return None
    o, h, l, c = candle["o"], candle["h"], candle["l"], candle["c"]
    rng = max(h - l, 0.0001)
    body = abs(c - o)
    body_pct = body / rng
    close_pos = (c - l) / rng
    green = c > o
    upper_wick = (h - max(o, c)) / rng
    lower_wick = (min(o, c) - l) / rng

    if body_pct > 0.6 and close_pos > 0.7 and green:
        strength = "STRONG_BULL"
    elif body_pct > 0.6 and close_pos < 0.3 and not green:
        strength = "STRONG_BEAR"
    elif body_pct < 0.15:
        strength = "DOJI"
    elif green:
        strength = "MILD_BULL"
    else:
        strength = "MILD_BEAR"

    return {
        "green": green, "body_pct": round(body_pct, 3),
        "close_pos": round(close_pos, 3), "strength": strength,
        "upper_wick": round(upper_wick, 3), "lower_wick": round(lower_wick, 3),
        "range": rng,
    }


def _v2_volatility_check(candles, current_range=None):
    """ATR-based volatility check."""
    if not candles or len(candles) < 3:
        return "unknown", True
    ranges = [c["h"] - c["l"] for c in candles[-10:]]
    atr = sum(ranges) / len(ranges) if ranges else 0
    if current_range is None:
        current_range = candles[-1]["h"] - candles[-1]["l"]
    if atr <= 0:
        return "unknown", True
    ratio = current_range / atr
    if ratio > 2.5:
        return "extreme", False
    elif ratio > 1.8:
        return "high", False
    else:
        return "normal", True


def _v2_should_enter(price, ptb, asset, structure, prev_candle,
                     vol_safe, session_safe, timeframe, secs_remaining):
    """Master entry decision — CONFIRMATION, not prediction.

    The question: "Will this close above/below the PTB?"

    1. Where is price relative to PTB? (must be meaningfully on one side)
    2. How did it get there? (steady grind = safe, spike = dangerous)
    3. Previous candle supports the bias?
    4. Structure confirms the path? (no reversal signs)
    5. Session quiet? Volatility normal?
    6. Given the distance and time left, is it unlikely to reverse back?
    """

    # Hard filters
    if not session_safe and timeframe != "DAILY":
        return False, None, 0, "Volatile session — skip"

    if not vol_safe:
        return False, None, 0, "Volatility too high — skip"

    if not price or not ptb or ptb <= 0:
        return False, None, 0, "No price or PTB data"

    if not structure:
        return False, None, 0, "No structure data"

    if not prev_candle:
        return False, None, 0, "No previous candle data"

    # 1. Distance from PTB
    distance_pct = ((price - ptb) / ptb) * 100
    abs_dist = abs(distance_pct)

    min_dist = {
        "BTC": 0.05, "ETH": 0.08, "SOL": 0.10,
        "XRP": 0.15, "DOGE": 0.20, "BNB": 0.10,
    }.get(asset, 0.10)

    if abs_dist < min_dist:
        return False, None, 0, "Too close to PTB ({:+.3f}%) — coin flip".format(distance_pct)

    direction = "UP" if distance_pct > 0 else "DOWN"

    # 2. How did price get here?
    grind = structure.get("grind_type", "normal")
    if grind == "spike":
        return False, None, 0, "Spike — one candle did {:.0f}% of the body move".format(
            structure.get("concentration", 0) * 100)

    # 3. Previous candle must align with direction
    if direction == "UP" and prev_candle["strength"] in ("STRONG_BEAR", "MILD_BEAR"):
        return False, None, 0, "Prev candle RED — no bullish momentum behind this"

    if direction == "DOWN" and prev_candle["strength"] in ("STRONG_BULL", "MILD_BULL"):
        return False, None, 0, "Prev candle GREEN — no bearish momentum behind this"

    # 4. Structure must not contradict
    struct_dir = structure.get("direction", "FLAT")
    hh = structure.get("hh_count", 0)
    hl = structure.get("hl_count", 0)
    lh = structure.get("lh_count", 0)
    ll = structure.get("ll_count", 0)

    # If structure shows clear opposite direction, skip
    if direction == "UP" and struct_dir == "DOWN":
        return False, None, 0, "Price above PTB but structure DOWN — conflicting"

    if direction == "DOWN" and struct_dir == "UP":
        return False, None, 0, "Price below PTB but structure UP — conflicting"

    # Any reversal signs = skip
    if direction == "UP" and lh >= 1:
        return False, None, 0, "LH={} — momentum fading, may drop back to PTB".format(lh)

    if direction == "DOWN" and hl >= 1:
        return False, None, 0, "HL={} — momentum fading, may rise back to PTB".format(hl)

    # 5. Build confidence
    confidence = 60

    # Distance bonus — further from PTB = safer
    if abs_dist > 0.30:
        confidence += 15
    elif abs_dist > 0.15:
        confidence += 10
    elif abs_dist > min_dist:
        confidence += 5

    # Previous candle strength
    if direction == "UP":
        if prev_candle["strength"] == "STRONG_BULL":
            confidence += 15
        elif prev_candle["strength"] == "MILD_BULL":
            confidence += 10
    else:
        if prev_candle["strength"] == "STRONG_BEAR":
            confidence += 15
        elif prev_candle["strength"] == "MILD_BEAR":
            confidence += 10

    # Doji prev = no bonus but not a skip (distance and structure carry it)
    # Structure HH/HL bonus
    if direction == "UP":
        confidence += min(hh * 3, 10)
        confidence += min(hl * 3, 10)
    else:
        confidence += min(ll * 3, 10)
        confidence += min(lh * 3, 10)

    # Steady grind bonus
    if grind == "steady":
        confidence += 5

    confidence = min(confidence, 99)

    # Build reason
    reason = "{} {:+.3f}% from PTB | {} | HH={} HL={} LH={} LL={} | {} | {}min left".format(
        direction, distance_pct, prev_candle["strength"],
        hh, hl, lh, ll, grind, int(secs_remaining / 60))

    # Minimum confidence per timeframe
    min_conf = {"1H": 70, "15M": 70, "DAILY": 75}.get(timeframe, 70)
    if confidence < min_conf:
        return False, None, confidence, "Conf {} < {} — {}".format(confidence, min_conf, reason)

    return True, direction, confidence, reason


def _v2_build_entry_note(asset, timeframe, direction, prev_candle, structure,
                         ptb, price, session_label, vol_label, confidence,
                         secs_remaining=0):
    """Build human-readable entry note."""
    prev_str = ""
    if prev_candle:
        color = "green" if prev_candle["green"] else "red"
        prev_str = "Prev: {} {}, body={:.0f}%, close@{:.0f}%".format(
            prev_candle["strength"], color,
            prev_candle["body_pct"] * 100, prev_candle["close_pos"] * 100)

    struct_str = ""
    if structure:
        struct_str = "HH={} HL={} LH={} LL={} | {}".format(
            structure["hh_count"], structure["hl_count"],
            structure["lh_count"], structure["ll_count"],
            structure["grind_type"])

    ptb_str = ""
    if ptb and price:
        dist = ((price - ptb) / ptb) * 100
        ptb_str = "PTB dist: {:+.3f}%".format(dist)

    return "{} {} {} | {} | {} | {} | Session: {} | Vol: {} | Conf: {} | {}min left".format(
        timeframe, asset, direction,
        prev_str, struct_str, ptb_str,
        session_label, vol_label, confidence,
        int(secs_remaining / 60) if secs_remaining else "?")


def _v2_market_url(platform, market_data=None, asset=None, timeframe=None):
    """Build clickable URL to the market on Polymarket/Limitless."""
    if platform == "polymarket":
        slug = market_data.get("slug", "") if market_data else ""
        condition_id = market_data.get("condition_id", "") if market_data else ""
        if slug:
            return "https://polymarket.com/event/{}".format(slug)
        elif condition_id:
            return "https://polymarket.com/market/{}".format(condition_id)
        return "https://polymarket.com"
    elif platform == "limitless":
        slug = market_data.get("slug", "") if market_data else ""
        if slug:
            return "https://limitless.exchange/markets/{}".format(slug)
        return "https://limitless.exchange"
    return ""

# V2 HEDGE LOGIC
# ═══════════════════════════════════════════════════════════

def _v2_check_hedge(trade, current_structure, candles=None, ptb=None):
    """Check if an open trade should be hedged.
    HEDGE ONLY when there's strong evidence of reversal — not noise.
    
    Requirements for hedge:
    1. Structure must show DOMINANT opposing signals (LH >= 3 AND LL >= 2 for UP trades)
    2. Price must have crossed back through PTB against the trade direction
    3. The grind type must NOT be choppy (choppy = no real trend either way)
    """
    if not trade or not current_structure:
        return False, None

    direction = trade.get("direction")
    hh = current_structure.get("hh_count", 0)
    hl = current_structure.get("hl_count", 0)
    lh = current_structure.get("lh_count", 0)
    ll = current_structure.get("ll_count", 0)
    grind = current_structure.get("grind_type", "")

    # Don't hedge in choppy markets — no clear reversal, just noise
    if grind == "choppy":
        return False, None

    if direction == "UP":
        # Need STRONG reversal: multiple lower highs AND lower lows
        # AND the opposing signals must dominate (more LH/LL than HH/HL)
        if lh >= 3 and ll >= 2 and lh > hh and ll > hl:
            reason = "Strong reversal: LH={} LL={} dominate HH={} HL={} | {}".format(lh, ll, hh, hl, grind)
            
            # Extra confirmation: price crossed back below PTB
            if candles and ptb and ptb > 0:
                current_price = candles[-1]["c"]
                if current_price < ptb:
                    reason += " | Price below PTB"
                    return True, reason
                else:
                    # Structure says reversal but price still above PTB — not confirmed yet
                    return False, None
            
            # No PTB data — rely on structure alone but be strict
            if lh >= 4 and ll >= 3:
                return True, reason
            return False, None

    elif direction == "DOWN":
        if hh >= 3 and hl >= 2 and hh > lh and hl > ll:
            reason = "Strong reversal: HH={} HL={} dominate LH={} LL={} | {}".format(hh, hl, lh, ll, grind)
            
            if candles and ptb and ptb > 0:
                current_price = candles[-1]["c"]
                if current_price > ptb:
                    reason += " | Price above PTB"
                    return True, reason
                else:
                    return False, None
            
            if hh >= 4 and hl >= 3:
                return True, reason
            return False, None

    return False, None


# ═══════════════════════════════════════════════════════════
# V2 PAPER TRADING — BALANCE & DB
# ═══════════════════════════════════════════════════════════

_v2_balances = {
    "polymarket": {"balance": 50.0, "peak_balance": 50.0, "wins": 0, "losses": 0},
    "limitless": {"balance": 50.0, "peak_balance": 50.0, "wins": 0, "losses": 0},
}

def _v2_load_balances():
    """Load balances from DB."""
    try:
        conn = get_db()
        rows = conn.run("SELECT platform, balance, peak_balance, wins, losses FROM v2_balances")
        for r in rows:
            _v2_balances[r[0]] = {
                "balance": float(r[1]), "peak_balance": float(r[2]),
                "wins": int(r[3]), "losses": int(r[4]),
            }
        conn.close()
    except Exception as e:
        print("[V2] Load balances error: {}".format(e))

def _v2_save_balance(platform):
    """Save balance to DB."""
    try:
        bal = _v2_balances.get(platform, {})
        conn = get_db()
        conn.run("""
            UPDATE v2_balances SET balance = :b, peak_balance = :p,
            wins = :w, losses = :l, updated_at = NOW()
            WHERE platform = :plat
        """, b=bal.get("balance", 100), p=bal.get("peak_balance", 100),
            w=bal.get("wins", 0), l=bal.get("losses", 0), plat=platform)
        conn.close()
    except Exception as e:
        print("[V2] Save balance error: {}".format(e))


def _v2_record_paper_trade(platform, timeframe, asset, direction, ptb,
                           entry_odds, stake, entry_note, structure,
                           session_label, volatility_label, prev_candle_str,
                           market_data=None, confidence=None):
    """Record a new paper trade in the database."""
    market_url = _v2_market_url(platform, market_data, asset, timeframe)
    try:
        conn = get_db()
        conn.run("""
            INSERT INTO v2_paper_trades (
                platform, timeframe, asset, direction, ptb, entry_odds,
                entry_price, stake, entry_note, hh_count, hl_count, ll_count, lh_count,
                grind_rate, ptb_distance, session_label, volatility,
                prev_candle, market_id, slug, condition_id,
                up_token, down_token, confidence, market_url, status
            ) VALUES (
                :plat, :tf, :asset, :dir, :ptb, :odds,
                :price, :stake, :note, :hh, :hl, :ll, :lh,
                :grind, :ptb_dist, :sess, :vol,
                :prev, :mid, :slug, :cid,
                :up_tok, :dn_tok, :conf, :murl, 'OPEN'
            )
        """,
            plat=platform, tf=timeframe, asset=asset, dir=direction,
            ptb=ptb, odds=entry_odds, price=_get_binance_price(asset),
            stake=stake, note=entry_note,
            hh=structure.get("hh_count", 0) if structure else 0,
            hl=structure.get("hl_count", 0) if structure else 0,
            ll=structure.get("ll_count", 0) if structure else 0,
            lh=structure.get("lh_count", 0) if structure else 0,
            grind=structure.get("grind_type", "") if structure else "",
            ptb_dist=0, sess=session_label, vol=volatility_label,
            prev=prev_candle_str or "",
            mid=market_data.get("market_id", "") if market_data else "",
            slug=market_data.get("slug", "") if market_data else "",
            cid=market_data.get("condition_id", "") if market_data else "",
            up_tok=market_data.get("up_token", "") if market_data else "",
            dn_tok=market_data.get("down_token", "") if market_data else "",
            conf=str(confidence) if confidence else "",
            murl=market_url,
        )
        conn.close()
        print("[V2] Paper trade: {} {} {} @ {:.0f}c".format(
            platform, asset, direction, (entry_odds or 50)))
    except Exception as e:
        print("[V2] Record trade error: {}".format(e))


# ═══════════════════════════════════════════════════════════
# V2 ORDER BOOK READING (for paper odds accuracy)
# ═══════════════════════════════════════════════════════════

def _v2_get_live_odds(market_data, direction):
    """Read live price from Polymarket CLOB using get_price().
    NOTE: get_order_book() is BROKEN (GitHub Issue #180). Use get_price() instead.
    Single call — fast fail on 404 (expired market) or timeout."""
    client = _get_poly_client()
    if not client or not market_data:
        return None

    try:
        token = market_data.get("up_token") if direction == "UP" else market_data.get("down_token")
        if not token:
            return None

        # Single call — BUY side = best ask (what we'd pay to buy)
        buy_price = None
        try:
            buy_result = client.get_price(str(token), side="BUY")
            if buy_result:
                if isinstance(buy_result, dict):
                    buy_price = float(buy_result.get("price", 0))
                elif isinstance(buy_result, (int, float, str)):
                    buy_price = float(buy_result)
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "No orderbook" in err_str:
                # Market expired — don't log spam, just return None
                return None
            if "timed out" in err_str.lower() or "timeout" in err_str.lower():
                return None
            print("[V2] POLY price error: {}".format(err_str[:80]))
            return None

        if buy_price and 0.01 <= buy_price <= 0.99:
            asset = market_data.get("asset", "?")
            tf = market_data.get("timeframe", "?")
            print("[V2] POLY PRICE {} {} {} = {:.0f}c".format(asset, tf, direction, buy_price * 100))
            return round(buy_price * 100, 1)

    except Exception as e:
        print("[V2] Poly price error: {}".format(e))
    return None


# ═══════════════════════════════════════════════════════════
# LIMITLESS MARKET DISCOVERY + ORDERBOOK
# ═══════════════════════════════════════════════════════════

def _limitless_fetch_markets():
    """Fetch active crypto Up/Down markets from Limitless Exchange.
    Uses GET /markets/active/slugs for lightweight discovery,
    then GET /markets/{slug} for full details."""
    import requests as req
    markets = []
    now = datetime.now(timezone.utc)

    try:
        # Get all active market slugs with metadata
        r = req.get("{}/markets/active/slugs".format(LIMITLESS_API), timeout=12)
        if r.status_code != 200:
            print("[LMTS] Active slugs status: {}".format(r.status_code))
            return markets

        slugs_data = r.json()
        if not isinstance(slugs_data, list):
            return markets

        for entry in slugs_data:
            slug = entry.get("slug", "")
            ticker = entry.get("ticker", "")
            strike = entry.get("strikePrice")
            deadline = entry.get("deadline")

            # Filter: crypto Up/Down markets only
            slug_lower = slug.lower()
            if not ticker:
                continue

            # Detect asset from ticker
            asset = ticker.upper() if ticker.upper() in BINANCE_MAP else None
            if not asset:
                continue

            # Must be an above/below or up/down market
            is_updown = any(kw in slug_lower for kw in ["above", "below", "up-or-down", "updown"])
            if not is_updown:
                continue

            # Parse deadline for timeframe detection
            if not deadline:
                continue
            try:
                deadline_dt = datetime.fromisoformat(deadline.replace("Z", "+00:00"))
            except:
                continue
            mins_left = (deadline_dt - now).total_seconds() / 60
            if mins_left <= 0:
                continue

            # Parse strike price as PTB
            baseline = None
            if strike:
                try:
                    baseline = float(strike)
                except:
                    pass

            # Detect timeframe from slug patterns or deadline proximity
            timeframe = None
            if "hourly" in slug_lower or "-1h-" in slug_lower:
                timeframe = "1H"
            elif "daily" in slug_lower or "on-" in slug_lower:
                timeframe = "DAILY"
            elif "-15m" in slug_lower:
                timeframe = "15M"

            # Fallback: estimate from time remaining
            if not timeframe:
                if 3 <= mins_left <= 65:
                    timeframe = "1H"
                elif 65 < mins_left <= 1500:
                    timeframe = "DAILY"
                elif mins_left <= 3:
                    continue  # Too close to expiry

            if not timeframe:
                continue

            # Handle group markets (nested)
            nested = entry.get("markets")
            if nested and isinstance(nested, list):
                for nm in nested:
                    ns = nm.get("slug", "")
                    if ns:
                        markets.append({
                            "slug": ns, "asset": asset, "timeframe": timeframe,
                            "baseline": baseline, "expiry_dt": deadline_dt,
                            "mins_left": mins_left, "market_id": ns,
                            "platform": "limitless",
                        })
            else:
                markets.append({
                    "slug": slug, "asset": asset, "timeframe": timeframe,
                    "baseline": baseline, "expiry_dt": deadline_dt,
                    "mins_left": mins_left, "market_id": slug,
                    "platform": "limitless",
                })

    except Exception as e:
        print("[LMTS] Fetch markets error: {}".format(e))

    if markets:
        print("[LMTS] Found {} crypto markets".format(len(markets)))
    return markets


def _limitless_get_orderbook_odds(slug, direction):
    """Read Limitless order book for a market.
    GET /markets/{slug}/orderbook → {bids, asks, adjustedMidpoint, lastTradePrice}
    Returns odds as percentage (e.g. 72.0 for 72c) or None."""
    import requests as req
    try:
        r = req.get("{}/markets/{}/orderbook".format(LIMITLESS_API, slug), timeout=8)
        if r.status_code != 200:
            return None
        book = r.json()
        if not book:
            return None

        asks = book.get("asks", [])
        bids = book.get("bids", [])
        mid = book.get("adjustedMidpoint")
        ltp = book.get("lastTradePrice")

        best_ask = float(asks[0].get("price", 0)) if asks else None
        best_bid = float(bids[0].get("price", 0)) if bids else None

        print("[LMTS] BOOK {} {} | ask={} bid={} mid={} ltp={} depth={}a/{}b".format(
            slug[:30], direction,
            "{:.4f}".format(best_ask) if best_ask else "None",
            "{:.4f}".format(best_bid) if best_bid else "None",
            "{:.4f}".format(float(mid)) if mid else "None",
            "{:.4f}".format(float(ltp)) if ltp else "None",
            len(asks), len(bids)))

        # For UP/YES direction, read the asks (price to buy YES shares)
        # For DOWN/NO direction, we buy NO shares
        # Limitless: asks = sell orders for YES, bids = buy orders for YES
        # To buy YES: we take the best ask
        # To buy NO: equivalent to selling YES at best bid, OR 1 - best_ask for NO
        if direction == "UP":
            if best_ask and 0.01 <= best_ask <= 0.99:
                return round(best_ask * 100, 1)
        else:
            # DOWN = buy NO shares. Price of NO = 1 - price of YES
            if best_ask and 0.01 <= best_ask <= 0.99:
                no_price = 1.0 - best_ask
                if 0.01 <= no_price <= 0.99:
                    return round(no_price * 100, 1)

        # Fallback to midpoint
        if mid:
            mid_f = float(mid)
            if direction == "UP":
                return round(mid_f * 100, 1)
            else:
                return round((1.0 - mid_f) * 100, 1)

        # Last trade price as final fallback
        if ltp:
            ltp_f = float(ltp)
            if direction == "UP":
                return round(ltp_f * 100, 1)
            else:
                return round((1.0 - ltp_f) * 100, 1)

    except Exception as e:
        print("[LMTS] Orderbook error {}: {}".format(slug, e))
    return None


def _v2_get_odds(platform, market_data, direction):
    """Unified odds reading — routes to the right order book per platform."""
    if platform == "polymarket":
        return _v2_get_live_odds(market_data, direction)
    elif platform == "limitless":
        slug = market_data.get("slug", "") if market_data else ""
        if slug:
            return _limitless_get_orderbook_odds(slug, direction)
    return None


def _v2_calc_limit_price(book_ask, confidence):
    """Calculate limit order price. Per spec: typical entry 70-90c.
    Place limit slightly below ask to get filled on any dip.
    Minimum limit: 65c — below that there's not enough confirmation."""

    if not book_ask or book_ask <= 0:
        return None, False

    # Below 65c means the market isn't confirming this direction — skip
    if book_ask < 65:
        return None, False

    # Place limit 0.5-2c below ask depending on confidence
    if book_ask >= 90:
        # Very high odds — limit just barely below
        limit = book_ask - 0.5 if confidence >= 85 else book_ask - 1.0
    elif book_ask >= 75:
        # Good range — small undercut
        limit = book_ask - 1.0 if confidence >= 85 else book_ask - 2.0
    else:
        # 65-75c range — slightly more undercut
        limit = book_ask - 2.0 if confidence >= 85 else book_ask - 3.0

    # Floor at 65c
    limit = max(65, limit)

    return round(limit, 1), True


# ═══════════════════════════════════════════════════════════
# V2 RESOLUTION — Check outcomes of paper trades
# ═══════════════════════════════════════════════════════════

def _v2_resolve_trades():
    """Resolve paper trades by checking the ACTUAL platform outcome.
    Polymarket: Gamma API outcomePrices → [1.0, 0.0] = UP won, [0.0, 1.0] = DOWN won
    Limitless: GET /markets/{slug} → check resolution status
    Falls back to Binance price vs PTB if platform check fails."""
    import requests as req
    try:
        conn = get_db()
        rows = conn.run("""
            SELECT id, platform, timeframe, asset, direction, ptb, entry_odds,
                   stake, market_id, slug, fired_at, hedged, hedge_odds, hedge_direction
            FROM v2_paper_trades WHERE status = 'OPEN'
            AND (order_status = 'FILLED' OR order_status IS NULL)
        """)
        cols = ["id", "platform", "timeframe", "asset", "direction", "ptb",
                "entry_odds", "stake", "market_id", "slug", "fired_at",
                "hedged", "hedge_odds", "hedge_direction"]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()

        if not trades:
            return 0

        resolved = 0
        for t in trades:
            # Skip if trade is less than the timeframe duration old
            if t.get("fired_at"):
                fired = t["fired_at"]
                if isinstance(fired, str):
                    try:
                        fired = datetime.fromisoformat(fired.replace("Z", "+00:00"))
                    except:
                        continue
                if not fired.tzinfo:
                    fired = fired.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                tf = t["timeframe"]
                min_age = {"15M": 16, "1H": 61, "DAILY": 1441}.get(tf, 61)
                if (now - fired).total_seconds() / 60 < min_age:
                    continue

            asset = t["asset"]
            slug = t.get("slug", "")
            platform = t["platform"]
            actual = None

            # METHOD 1: Check platform for actual resolution
            if platform == "polymarket" and slug:
                try:
                    # Query Gamma API for the market by slug
                    r = req.get("{}/markets".format(POLY_GAMMA_API),
                                params={"slug": slug}, timeout=8)
                    if r.status_code == 200:
                        markets = r.json()
                        market = markets[0] if isinstance(markets, list) and markets else markets if isinstance(markets, dict) else None
                        if market:
                            closed = market.get("closed", False)
                            if closed:
                                outcome_prices = market.get("outcomePrices")
                                if isinstance(outcome_prices, str):
                                    try: outcome_prices = json.loads(outcome_prices)
                                    except: outcome_prices = None
                                if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                                    # Check outcome ordering
                                    outcomes_raw = market.get("outcomes")
                                    if isinstance(outcomes_raw, str):
                                        try: outcomes_raw = json.loads(outcomes_raw)
                                        except: outcomes_raw = None
                                    up_idx = 0
                                    if isinstance(outcomes_raw, list) and len(outcomes_raw) >= 2:
                                        o0 = str(outcomes_raw[0]).lower().strip()
                                        if o0 in ("no", "down", "below"):
                                            up_idx = 1
                                    up_price = float(outcome_prices[up_idx])
                                    if up_price > 0.9:
                                        actual = "UP"
                                    elif up_price < 0.1:
                                        actual = "DOWN"
                                    else:
                                        actual = None  # Not clearly resolved yet
                            else:
                                continue  # Market not closed yet
                except Exception as e:
                    print("[V2] Poly resolve check error {}: {}".format(slug[:30], e))

            elif platform == "limitless" and slug:
                try:
                    r = req.get("{}/markets/{}".format(LIMITLESS_API, slug), timeout=8)
                    if r.status_code == 200:
                        market = r.json()
                        status = market.get("status", "")
                        if status in ("resolved", "closed"):
                            winner = market.get("winningOutcome") or market.get("winner") or ""
                            if str(winner).lower() in ("yes", "up", "above", "0"):
                                actual = "UP"
                            elif str(winner).lower() in ("no", "down", "below", "1"):
                                actual = "DOWN"
                            else:
                                # Try outcomePrices
                                op = market.get("outcomePrices") or market.get("prices")
                                if isinstance(op, list) and len(op) >= 2:
                                    if float(op[0]) > 0.9: actual = "UP"
                                    elif float(op[0]) < 0.1: actual = "DOWN"
                        else:
                            continue  # Not resolved yet
                except Exception as e:
                    print("[V2] Limitless resolve check error {}: {}".format(slug[:30], e))

            # METHOD 2: Fallback — Binance price vs PTB (only if platform check failed)
            if not actual:
                close_price = _get_binance_price(asset)
                ptb = t.get("ptb")
                if not close_price or not ptb or ptb <= 0:
                    continue
                if close_price > ptb:
                    actual = "UP"
                elif close_price < ptb:
                    actual = "DOWN"
                else:
                    actual = "FLAT"

            direction = t["direction"]
            entry_odds = t.get("entry_odds", 50) or 50
            stake = t.get("stake", 3.0) or 3.0

            # Calculate P&L
            if actual == direction:
                odds_decimal = entry_odds / 100
                payout = (stake / odds_decimal) - stake if odds_decimal > 0 else 0
                outcome = "WIN"
                pnl = payout
            elif actual == "FLAT":
                outcome = "PUSH"
                pnl = 0
            else:
                outcome = "LOSS"
                pnl = -stake

            # Hedge P&L
            if t.get("hedged") and t.get("hedge_odds"):
                hedge_odds = t["hedge_odds"]
                hedge_dir = t.get("hedge_direction")
                hedge_stake = stake * 0.5
                if actual == hedge_dir:
                    hedge_pnl = (hedge_stake / (hedge_odds / 100)) - hedge_stake
                else:
                    hedge_pnl = -hedge_stake
                pnl += hedge_pnl
            else:
                hedge_pnl = 0

            # Update balance
            bal = _v2_balances.get(platform, {})
            bal["balance"] = bal.get("balance", 100) + pnl
            if outcome == "WIN":
                bal["wins"] = bal.get("wins", 0) + 1
            elif outcome == "LOSS":
                bal["losses"] = bal.get("losses", 0) + 1
            bal["peak_balance"] = max(bal.get("peak_balance", 100), bal["balance"])
            _v2_balances[platform] = bal
            _v2_save_balance(platform)

            # Update trade record
            try:
                conn2 = get_db()
                conn2.run("""
                    UPDATE v2_paper_trades SET
                    close_price = :cp, actual_result = :ar, outcome = :oc,
                    pnl = :pnl, balance_after = :bal, hedge_pnl = :hpnl,
                    status = :st, resolved_at = NOW()
                    WHERE id = :tid
                """, cp=_get_binance_price(asset), ar=actual, oc=outcome,
                    pnl=round(pnl, 4), bal=round(bal["balance"], 2),
                    hpnl=round(hedge_pnl, 4) if hedge_pnl else None,
                    st="RESOLVED", tid=t["id"])
                conn2.close()
                resolved += 1
            except Exception as e:
                print("[V2] Resolve update error: {}".format(e))

            emoji = "✅" if outcome == "WIN" else "❌"
            send_telegram("{} V2 {} {} {} {} @ {:.0f}c → {} | P&L ${:+.2f} | Bal ${:.2f}".format(
                emoji, t["timeframe"], asset, direction,
                platform[:4].upper(), entry_odds, outcome,
                pnl, bal["balance"]))

        return resolved
    except Exception as e:
        print("[V2] Resolve error: {}".format(e))
        return 0


# ═══════════════════════════════════════════════════════════
# V2 WATCHER THREADS
# ═══════════════════════════════════════════════════════════

# Track active trades per boundary to avoid duplicates
_v2_active_boundaries = {}  # {"BTC_1H_1748390400": True, ...}
_fill_failures = {}  # Track consecutive 404s per order for expiry
FLAT_STAKE = 3.00  # $3 flat per confirmed entry

def _v2_scan_timeframe(timeframe):
    """Core scanning logic shared by 1H, 15M, and DAILY watchers.
    Scans BOTH Polymarket and Limitless.
    SELECTIVE: only enters the best 2 trades per scan cycle, ranked by confidence."""

    ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB"]
    MAX_ENTRIES_PER_SCAN = 2  # Only the best 2 trades per cycle
    tf_label = timeframe

    # Timeframe-specific config — correct candle intervals
    if tf_label == "1H":
        intra_interval = "15m"      # 15M candles for hourly structure
        prev_interval = "1h"        # Previous 1H candle
        min_intra_candles = 2       # Need 2+ completed 15M candles (at T+45 we have 3)
        boundary_secs = 3600
        poly_tf_filter = "1H"
        scan_sleep = 120
        entry_window_start = 2700   # T+45min
        entry_window_end = 3540     # T+59min
    elif tf_label == "15M":
        intra_interval = "5m"       # 5M candles for 15M structure
        prev_interval = "15m"       # Previous 15M candle
        min_intra_candles = 1       # Need 1+ completed 5M candle (at T+5 we have 1, at T+10 we have 2)
        boundary_secs = 900
        poly_tf_filter = "15M"
        scan_sleep = 60
        entry_window_start = 300    # T+5min
        entry_window_end = 840      # T+14min
    else:  # DAILY
        intra_interval = "4h"       # 4H candles for daily structure
        prev_interval = "1d"        # Previous daily candle
        min_intra_candles = 3       # Need 3+ completed 4H candles
        boundary_secs = 86400
        poly_tf_filter = "DAILY"
        scan_sleep = 1800
        entry_window_start = 0      # Handled by quiet hours
        entry_window_end = 86400

    while True:
        try:
            now = datetime.now(timezone.utc)
            now_ts = int(now.timestamp())

            if tf_label == "DAILY":
                # Daily boundary = midnight UTC
                boundary_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                secs_into_period = now_ts - boundary_ts
                # Spec: check 3-6 hours before close during quiet hours
                # Quiet hours: 17-22 UTC or 4-11 UTC
                h = now.hour
                is_quiet = (17 <= h <= 22) or (4 <= h <= 11)
                if not is_quiet:
                    time.sleep(600)
                    continue
                # Need at least 6 hours of data
                if secs_into_period < 21600:
                    time.sleep(max(60, 21600 - secs_into_period))
                    continue
            else:
                boundary_ts = (now_ts // boundary_secs) * boundary_secs
                secs_into_period = now_ts - boundary_ts
                # Spec: only enter within the entry window
                if secs_into_period < entry_window_start:
                    time.sleep(entry_window_start - secs_into_period + 5)
                    continue
                if secs_into_period > entry_window_end:
                    # Past the window — wait for next period
                    time.sleep(boundary_secs - secs_into_period + 5)
                    continue

            # Session filter
            session_label, session_safe = _v2_session_filter(now.hour)

            # Fetch markets from BOTH platforms
            poly_markets = _poly_fetch_markets()
            poly_tf = {m["asset"]: m for m in (poly_markets or []) if m.get("timeframe") == poly_tf_filter}

            lmts_markets = _limitless_fetch_markets()
            lmts_tf = {m["asset"]: m for m in (lmts_markets or []) if m.get("timeframe") == tf_label}

            # Log what was found for this timeframe
            poly_assets = sorted(poly_tf.keys()) if poly_tf else []
            lmts_assets = sorted(lmts_tf.keys()) if lmts_tf else []
            if poly_assets or lmts_assets:
                print("[V2] {} scan: POLY={} LMTS={}".format(
                    tf_label, ",".join(poly_assets) or "none", ",".join(lmts_assets) or "none"))
            else:
                print("[V2] {} scan: no markets found on either platform".format(tf_label))

            # Collect candidates, then enter only the best
            _scan_candidates = []

            for asset in ASSETS:
                # Try both platforms for this asset
                platforms_to_try = []
                if asset in poly_tf:
                    platforms_to_try.append(("polymarket", poly_tf[asset]))
                if asset in lmts_tf:
                    platforms_to_try.append(("limitless", lmts_tf[asset]))
                if not platforms_to_try:
                    continue  # No market on either platform — skip

                for platform, market_data in platforms_to_try:
                    boundary_key = "{}_{}_{}_{}".format(asset, tf_label, platform[:4], boundary_ts)
                    if boundary_key in _v2_active_boundaries:
                        continue

                    # Fetch intra-period candles from Binance
                    intra_candles = _fetch_binance_candles(asset, interval=intra_interval, limit=30)
                    if not intra_candles or len(intra_candles) < 3:
                        print("[V2] SKIP {} {} {} — no candles from Binance ({})".format(
                            tf_label, asset, platform[:4], intra_interval))
                        continue

                    # Filter to THIS period only
                    period_candles = [c for c in intra_candles if c["t"] >= boundary_ts * 1000]
                    if len(period_candles) < min_intra_candles:
                        print("[V2] SKIP {} {} {} — only {} candles in period (need {})".format(
                            tf_label, asset, platform[:4], len(period_candles), min_intra_candles))
                        continue

                    # Previous completed candle
                    prev_candles = _fetch_binance_candles(asset, interval=prev_interval, limit=5)
                    if not prev_candles or len(prev_candles) < 2:
                        print("[V2] SKIP {} {} {} — no prev candles".format(tf_label, asset, platform[:4]))
                        continue
                    prev_candle = _v2_analyze_prev_candle(prev_candles[-2])

                    # Structure analysis
                    structure = _v2_analyze_structure(period_candles)
                    if not structure:
                        print("[V2] SKIP {} {} {} — structure analysis returned None".format(
                            tf_label, asset, platform[:4]))
                        continue

                    # Volatility
                    current_range = max(c["h"] for c in period_candles) - min(c["l"] for c in period_candles)
                    vol_label, vol_safe = _v2_volatility_check(prev_candles[:-1], current_range)

                    # Get current price — prefer Chainlink RTDS (what markets resolve against)
                    price = _rtds_current_price(asset)
                    if not price or price <= 0:
                        price = _get_binance_price(asset)

                    # Get PTB — this is the opening price of the period
                    # Priority: (1) market title baseline, (2) Chainlink boundary capture,
                    # (3) period's first candle open — NOT current price
                    ptb = None
                    # From market data (parsed from title/description)
                    if market_data and market_data.get("baseline") and market_data["baseline"] > 0:
                        ptb = market_data["baseline"]
                    # From Chainlink boundary capture at period start
                    if not ptb:
                        key = "{}_{}".format(asset, tf_label)
                        entry = _chainlink_ptb.get(key)
                        if entry:
                            ptb = entry[1]
                    # From the opening price of the first intra-period candle
                    if not ptb and period_candles:
                        ptb = period_candles[0]["o"]
                    # Last resort: from the full candle data open
                    if not ptb and intra_candles:
                        # Find the candle at boundary start
                        for c in intra_candles:
                            if c["t"] >= boundary_ts * 1000:
                                ptb = c["o"]
                                break

                    if not ptb or ptb <= 0:
                        print("[V2] SKIP {} {} {} — no PTB found".format(tf_label, asset, platform[:4]))
                        continue

                    # Calculate time remaining
                    secs_remaining = boundary_secs - secs_into_period

                    # Entry decision — new signature
                    should, direction, confidence, reason = _v2_should_enter(
                        price, ptb, asset, structure, prev_candle,
                        vol_safe,
                        session_safe if tf_label != "DAILY" else True,
                        tf_label, secs_remaining
                    )

                    if not should:
                        print("[V2] REJECT {} {} {} — conf={} reason={}".format(
                            tf_label, asset, platform[:4], confidence, reason[:80] if reason else "none"))
                        continue

                    # Collect as candidate — don't enter yet
                    _scan_candidates.append({
                        "asset": asset, "platform": platform, "market_data": market_data,
                        "direction": direction, "confidence": confidence, "reason": reason,
                        "structure": structure, "prev_candle": prev_candle,
                        "prev_str": "{} body={:.0f}%".format(
                            prev_candle["strength"], prev_candle["body_pct"] * 100) if prev_candle else "",
                        "ptb": ptb, "price": price, "session_label": session_label,
                        "vol_label": vol_label, "boundary_key": boundary_key,
                        "secs_remaining": secs_remaining,
                    })

            # === SELECTIVITY: Rank candidates by confidence, enter only the best ===
            if _scan_candidates:
                # Sort by confidence descending
                _scan_candidates.sort(key=lambda c: c.get("confidence", 0), reverse=True)
                entered = 0

                for cand in _scan_candidates:
                    if entered >= MAX_ENTRIES_PER_SCAN:
                        break

                    asset = cand["asset"]
                    platform = cand["platform"]
                    market_data = cand["market_data"]
                    direction = cand["direction"]
                    confidence = cand["confidence"]
                    reason = cand["reason"]
                    structure = cand["structure"]
                    prev_candle = cand["prev_candle"]
                    prev_str = cand["prev_str"]
                    ptb = cand["ptb"]
                    price = cand["price"]
                    session_label = cand["session_label"]
                    vol_label = cand["vol_label"]
                    boundary_key = cand["boundary_key"]

                    if boundary_key in _v2_active_boundaries:
                        continue

                    # Get REAL book ask from order book
                    book_ask = _v2_get_odds(platform, market_data, direction)

                    # Calculate limit price
                    if book_ask:
                        limit_price, should_place = _v2_calc_limit_price(book_ask, confidence)
                        if not should_place:
                            print("[V2] {} {} {} — book_ask={:.0f}c, below 65c minimum".format(
                                tf_label, asset, direction, book_ask))
                            continue
                    else:
                        # No book data = can't confirm odds = skip
                        print("[V2] {} {} {} — no book data, skip".format(tf_label, asset, direction))
                        continue

                    entry_odds = limit_price

                    # Build entry note
                    secs_rem = cand.get("secs_remaining", 0)
                    note = _v2_build_entry_note(
                        asset, tf_label, direction, prev_candle, structure,
                        ptb, price, session_label, vol_label, confidence,
                        secs_remaining=secs_rem)
                    if book_ask:
                        note += " | Book: {:.0f}c → Limit: {:.0f}c".format(book_ask, limit_price)

                    # Record paper trade as PENDING
                    market_url = _v2_market_url(platform, market_data, asset, tf_label)
                    try:
                        conn = get_db()
                        conn.run("""
                            INSERT INTO v2_paper_trades (
                                platform, timeframe, asset, direction, ptb, entry_odds,
                                entry_price, stake, entry_note, hh_count, hl_count, ll_count, lh_count,
                                grind_rate, ptb_distance, session_label, volatility,
                                prev_candle, market_id, slug, condition_id,
                                up_token, down_token, confidence, market_url,
                                limit_price, book_ask, order_status, status
                            ) VALUES (
                                :plat, :tf, :asset, :dir, :ptb, :odds,
                                :price, :stake, :note, :hh, :hl, :ll, :lh,
                                :grind, :ptb_dist, :sess, :vol,
                                :prev, :mid, :slug, :cid,
                                :up_tok, :dn_tok, :conf, :murl,
                                :lim, :bask, 'PENDING', 'OPEN'
                            )
                        """,
                            plat=platform, tf=tf_label, asset=asset, dir=direction,
                            ptb=ptb, odds=entry_odds, price=price,
                            stake=FLAT_STAKE, note=note,
                            hh=structure.get("hh_count", 0) if structure else 0,
                            hl=structure.get("hl_count", 0) if structure else 0,
                            ll=structure.get("ll_count", 0) if structure else 0,
                            lh=structure.get("lh_count", 0) if structure else 0,
                            grind=structure.get("grind_type", "") if structure else "",
                            ptb_dist=0, sess=session_label, vol=vol_label,
                            prev=prev_str or "",
                            mid=market_data.get("market_id", "") if market_data else "",
                            slug=market_data.get("slug", "") if market_data else "",
                            cid=market_data.get("condition_id", "") if market_data else "",
                            up_tok=market_data.get("up_token", "") if market_data else "",
                            dn_tok=market_data.get("down_token", "") if market_data else "",
                            conf=str(confidence) if confidence else "",
                            murl=market_url,
                            lim=limit_price, bask=book_ask,
                        )
                        conn.close()
                    except Exception as e:
                        print("[V2] Record PENDING error: {}".format(e))
                        continue

                    _v2_active_boundaries[boundary_key] = True
                    entered += 1

                    url_str = "\n🔗 {}".format(market_url) if market_url else ""
                    send_telegram(
                        "📋 V2 LIMIT {} {} {} {} @ {:.0f}c (book {:.0f}c)\n"
                        "Conf {} | ${:.2f} | BEST {}/{}{}".format(
                            platform[:4].upper(), tf_label, asset, direction,
                            limit_price, book_ask or 0, confidence,
                            FLAT_STAKE, entered, len(_scan_candidates), url_str))

                if _scan_candidates:
                    print("[V2] {} scan: {} candidates, entered {}".format(
                        tf_label, len(_scan_candidates), entered))

            time.sleep(scan_sleep)

        except Exception as e:
            print("[V2] {} watcher error: {}".format(tf_label, e))
            import traceback; traceback.print_exc()
            time.sleep(30)


def _v2_hourly_watcher():
    """HOURLY WATCHER — scans every 2 minutes. Both Polymarket + Limitless."""
    print("[V2] Hourly watcher started")
    _v2_scan_timeframe("1H")


def _v2_fifteen_min_watcher():
    """15M WATCHER — scans every 1 minute. Stricter confidence (75+)."""
    print("[V2] 15M watcher started")
    _v2_scan_timeframe("15M")


def _v2_daily_watcher():
    """DAILY WATCHER — scans every 10 minutes. Both Polymarket + Limitless."""
    print("[V2] Daily watcher started")
    _v2_scan_timeframe("DAILY")


def _v2_monitor_thread():
    """Monitor open positions for structure breaks → hedge.
    Hedge = buy opposite side at REAL order book odds."""
    print("[V2] Monitor thread started")

    while True:
        try:
            conn = get_db()
            rows = conn.run("""
                SELECT id, platform, timeframe, asset, direction, ptb, entry_odds,
                       stake, fired_at, hedged, market_id, slug, condition_id,
                       up_token, down_token
                FROM v2_paper_trades WHERE status = 'OPEN' AND hedged = FALSE
                AND (order_status = 'FILLED' OR order_status IS NULL)
            """)
            cols = ["id", "platform", "timeframe", "asset", "direction", "ptb",
                    "entry_odds", "stake", "fired_at", "hedged", "market_id",
                    "slug", "condition_id", "up_token", "down_token"]
            trades = [dict(zip(cols, r)) for r in rows]
            conn.close()

            for t in trades:
                asset = t["asset"]
                tf = t["timeframe"]

                # Get current intra-period candles for structure check
                interval = "1m" if tf == "15M" else "5m"
                candles = _fetch_binance_candles(asset, interval=interval, limit=15)
                if not candles or len(candles) < 3:
                    continue

                structure = _v2_analyze_structure(candles[-10:])
                
                # Get current price and PTB for hedge confirmation
                current_ptb = t.get("ptb")
                should_hedge, hedge_reason = _v2_check_hedge(t, structure, candles, current_ptb)

                if not should_hedge:
                    continue

                # Hedge direction is opposite of original trade
                hedge_dir = "DOWN" if t["direction"] == "UP" else "UP"

                # Get REAL opposite-side odds from order book
                hedge_odds = None
                market_data = {
                    "up_token": t.get("up_token", ""),
                    "down_token": t.get("down_token", ""),
                    "slug": t.get("slug", ""),
                    "condition_id": t.get("condition_id", ""),
                    "market_id": t.get("market_id", ""),
                }
                if market_data.get("up_token") or market_data.get("down_token") or market_data.get("slug"):
                    hedge_odds = _v2_get_odds(t.get("platform", "polymarket"), market_data, hedge_dir)

                if not hedge_odds:
                    hedge_odds = 30.0  # Cheap hedge assumption for paper

                # Hedge stake = 50% of original
                hedge_stake = (t.get("stake", FLAT_STAKE) or FLAT_STAKE) * 0.5

                # Record hedge on the trade
                try:
                    conn2 = get_db()
                    conn2.run("""
                        UPDATE v2_paper_trades SET
                        hedged = TRUE, hedge_odds = :ho,
                        hedge_direction = :hd, hedge_note = :hn
                        WHERE id = :tid
                    """, ho=hedge_odds, hd=hedge_dir,
                        hn="{} | Hedge stake=${:.2f}".format(hedge_reason, hedge_stake),
                        tid=t["id"])
                    conn2.close()
                except:
                    pass

                market_url = _v2_market_url("polymarket", market_data, asset, tf)
                url_str = "\n🔗 {}".format(market_url) if market_url else ""

                send_telegram(
                    "🛡️ V2 HEDGE {} {} {} → {} @ {:.0f}c | ${:.2f}\n"
                    "📝 {}{}".format(
                        tf, asset, t["direction"], hedge_dir,
                        hedge_odds, hedge_stake, hedge_reason[:80], url_str))

            time.sleep(30)

        except Exception as e:
            print("[V2] Monitor error: {}".format(e))
            time.sleep(60)


def _v2_resolve_loop():
    """Background thread to resolve paper trades periodically."""
    print("[V2] Resolve loop started")
    while True:
        try:
            resolved = _v2_resolve_trades()
            if resolved:
                print("[V2] Resolved {} trades".format(resolved))
        except Exception as e:
            print("[V2] Resolve loop error: {}".format(e))
        time.sleep(60)


def _v2_fill_checker():
    """Check PENDING limit orders — fill them if the book ask has dropped
    to our limit price or below. Expire them if the market period has ended."""
    print("[V2] Fill checker started")

    while True:
        try:
            conn = get_db()
            rows = conn.run("""
                SELECT id, platform, timeframe, asset, direction, limit_price,
                       market_id, slug, condition_id, up_token, down_token,
                       fired_at
                FROM v2_paper_trades
                WHERE order_status = 'PENDING' AND status = 'OPEN'
            """)
            cols = ["id", "platform", "timeframe", "asset", "direction", "limit_price",
                    "market_id", "slug", "condition_id", "up_token", "down_token", "fired_at"]
            orders = [dict(zip(cols, r)) for r in rows]
            conn.close()

            for o in orders:
                # Check if market period has expired (order should expire unfilled)
                if o.get("fired_at"):
                    fired = o["fired_at"]
                    if isinstance(fired, str):
                        try: fired = datetime.fromisoformat(fired.replace("Z", "+00:00"))
                        except: continue
                    if not fired.tzinfo:
                        fired = fired.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    tf = o["timeframe"]
                    max_age = {"15M": 15, "1H": 60, "DAILY": 1440}.get(tf, 60)
                    if (now - fired).total_seconds() / 60 > max_age:
                        # Expired unfilled — cancel the order
                        try:
                            conn2 = get_db()
                            conn2.run("""
                                UPDATE v2_paper_trades SET order_status = 'EXPIRED', status = 'RESOLVED',
                                outcome = 'EXPIRED', resolved_at = NOW()
                                WHERE id = :tid
                            """, tid=o["id"])
                            conn2.close()
                            print("[V2] EXPIRED unfilled: {} {} {}".format(o["timeframe"], o["asset"], o["direction"]))
                        except:
                            pass
                        continue

                # Check current book ask
                market_data = {
                    "up_token": o.get("up_token", ""),
                    "down_token": o.get("down_token", ""),
                    "slug": o.get("slug", ""),
                    "condition_id": o.get("condition_id", ""),
                    "market_id": o.get("market_id", ""),
                    "asset": o.get("asset", ""),
                    "timeframe": o.get("timeframe", ""),
                }
                current_ask = _v2_get_odds(o["platform"], market_data, o["direction"])
                # Respect Limitless rate limits (300ms between calls)
                if o["platform"] == "limitless":
                    time.sleep(0.35)

                if not current_ask:
                    # Track consecutive failures — expire after 3
                    fail_key = "fail_{}".format(o["id"])
                    _fill_failures[fail_key] = _fill_failures.get(fail_key, 0) + 1
                    if _fill_failures[fail_key] >= 3:
                        # Token is dead — expire the order
                        try:
                            conn2 = get_db()
                            conn2.run("""
                                UPDATE v2_paper_trades SET order_status = 'EXPIRED', status = 'RESOLVED',
                                outcome = 'EXPIRED', resolved_at = NOW()
                                WHERE id = :tid
                            """, tid=o["id"])
                            conn2.close()
                            del _fill_failures[fail_key]
                            print("[V2] EXPIRED (dead token): {} {} {}".format(
                                o["timeframe"], o["asset"], o["direction"]))
                        except:
                            pass
                    continue

                limit = o.get("limit_price", 0) or 0

                # FILL if current ask <= our limit price AND ask is sane (>= 10c)
                # Below 10c means stale/dead market data — not a real fill
                if current_ask < 10:
                    continue
                if current_ask <= limit:
                    try:
                        conn2 = get_db()
                        conn2.run("""
                            UPDATE v2_paper_trades SET
                            order_status = 'FILLED', entry_odds = :odds,
                            book_ask = :bask, filled_at = NOW()
                            WHERE id = :tid
                        """, odds=current_ask, bask=current_ask, tid=o["id"])
                        conn2.close()
                        print("[V2] FILLED: {} {} {} @ {:.0f}c (limit was {:.0f}c)".format(
                            o["timeframe"], o["asset"], o["direction"], current_ask, limit))

                        send_telegram(
                            "✅ V2 FILLED {} {} {} @ {:.0f}c (limit {:.0f}c)\n"
                            "${:.2f} stake now active".format(
                                o["timeframe"], o["asset"], o["direction"],
                                current_ask, limit, FLAT_STAKE))
                    except Exception as e:
                        print("[V2] Fill update error: {}".format(e))

            time.sleep(30)  # Check fills every 30 seconds

        except Exception as e:
            print("[V2] Fill checker error: {}".format(e))
            time.sleep(30)


def _v2_cleanup_loop():
    """Clean up old boundary keys every hour."""
    while True:
        time.sleep(3600)
        now_ts = int(time.time())
        old_keys = [k for k, v in _v2_active_boundaries.items()
                    if now_ts - int(k.split("_")[-1]) > 86400]
        for k in old_keys:
            del _v2_active_boundaries[k]


# ═══════════════════════════════════════════════════════════
# DASHBOARD HTML
# ═══════════════════════════════════════════════════════════

DASHBOARD_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=JetBrains+Mono:wght@400;500&display=swap');
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'DM Sans', sans-serif; background: #0a0f0d; color: #e8ede9; min-height: 100vh; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px; }
.header { display: flex; justify-content: space-between; align-items: center; padding: 20px 0; border-bottom: 1px solid #1a2e1f; margin-bottom: 24px; }
.header h1 { font-size: 1.5rem; color: #4ade80; font-weight: 700; }
.header .subtitle { font-size: 0.85rem; color: #6b8f74; }
.nav { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 24px; }
.nav a { color: #4ade80; text-decoration: none; font-size: 0.85rem; padding: 6px 14px; border: 1px solid #1a2e1f; border-radius: 6px; transition: all 0.2s; }
.nav a:hover, .nav a.active { background: #1a2e1f; border-color: #4ade80; }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 24px; }
.stat-card { background: #111a14; border: 1px solid #1a2e1f; border-radius: 10px; padding: 16px; }
.stat-card .label { font-size: 0.75rem; color: #6b8f74; text-transform: uppercase; letter-spacing: 0.5px; }
.stat-card .value { font-size: 1.5rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; margin-top: 4px; }
.stat-card .value.green { color: #4ade80; }
.stat-card .value.red { color: #f87171; }
.stat-card .value.blue { color: #60a5fa; }
table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
thead th { background: #111a14; color: #6b8f74; text-transform: uppercase; font-size: 0.7rem; letter-spacing: 0.5px; padding: 10px 8px; text-align: left; border-bottom: 1px solid #1a2e1f; position: sticky; top: 0; }
tbody td { padding: 10px 8px; border-bottom: 1px solid #0f1a12; font-family: 'JetBrains Mono', monospace; font-size: 0.78rem; }
tbody tr:hover { background: #111a14; }
.up { color: #4ade80; }
.down { color: #f87171; }
.win { color: #4ade80; font-weight: 700; }
.loss { color: #f87171; font-weight: 700; }
.pend { color: #fbbf24; }
.hedge-badge { background: #1e3a5f; color: #60a5fa; padding: 2px 6px; border-radius: 4px; font-size: 0.7rem; }
.conf-high { color: #4ade80; }
.conf-med { color: #fbbf24; }
.conf-low { color: #f87171; }
.note-cell { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: pointer; color: #6b8f74; font-size: 0.72rem; }
.note-cell:hover { white-space: normal; color: #e8ede9; }
.filter-bar { display: flex; gap: 8px; margin-bottom: 16px; }
.filter-btn { background: #111a14; border: 1px solid #1a2e1f; color: #6b8f74; padding: 6px 12px; border-radius: 6px; cursor: pointer; font-size: 0.8rem; }
.filter-btn.active { background: #1a2e1f; color: #4ade80; border-color: #4ade80; }
.rtds-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.rtds-dot.on { background: #4ade80; box-shadow: 0 0 6px #4ade80; }
.rtds-dot.off { background: #f87171; }
.empty { text-align: center; padding: 40px; color: #6b8f74; }
</style>
"""


def _v2_dashboard_html(platform, trades, bal):
    """Build dashboard HTML for a platform."""
    import html as _html

    total = len(trades)
    wins = sum(1 for t in trades if t.get("outcome") == "WIN")
    losses = sum(1 for t in trades if t.get("outcome") == "LOSS")
    active = sum(1 for t in trades if t.get("status") == "OPEN")
    resolved = wins + losses
    wr = round(wins / resolved * 100, 1) if resolved > 0 else 0
    balance = bal.get("balance", 100)
    peak = bal.get("peak_balance", 100)
    total_pnl = sum(t.get("pnl", 0) or 0 for t in trades if t.get("pnl") is not None)

    h = '<!DOCTYPE html><html><head><meta charset="utf-8">'
    h += '<meta name="viewport" content="width=device-width, initial-scale=1">'
    h += '<title>Cmvng Bot v2 — {}</title>'.format(platform.title())
    h += DASHBOARD_CSS
    h += '</head><body><div class="container">'

    # Header
    h += '<div class="header">'
    h += '<div><h1>CMVNG BOT v2</h1>'
    h += '<div class="subtitle">Confirmation Trading — {} Paper</div></div>'.format(platform.title())
    h += '<div><span class="rtds-dot {}"></span>{}</div>'.format(
        "on" if _chainlink_connected else "off",
        "RTDS Live" if _chainlink_connected else "RTDS Off")
    h += '</div>'

    # Nav
    h += '<div class="nav">'
    h += '<a href="/app/paper-poly" class="{}">Polymarket</a>'.format("active" if platform == "polymarket" else "")
    h += '<a href="/app/paper-limitless" class="{}">Limitless</a>'.format("active" if platform == "limitless" else "")
    h += '<a href="/v2/status">Engine Status</a>'
    h += '</div>'

    # Stats
    h += '<div class="stats-grid">'
    h += '<div class="stat-card"><div class="label">Balance</div><div class="value green">${:.2f}</div></div>'.format(balance)
    h += '<div class="stat-card"><div class="label">Peak</div><div class="value blue">${:.2f}</div></div>'.format(peak)
    h += '<div class="stat-card"><div class="label">Win Rate</div><div class="value {}">{:.1f}%</div></div>'.format(
        "green" if wr >= 70 else "red" if wr < 50 else "", wr)
    h += '<div class="stat-card"><div class="label">Record</div><div class="value">{}W / {}L</div></div>'.format(wins, losses)
    h += '<div class="stat-card"><div class="label">Active</div><div class="value blue">{}</div></div>'.format(active)
    h += '<div class="stat-card"><div class="label">Total P&L</div><div class="value {}">${:+.2f}</div></div>'.format(
        "green" if total_pnl >= 0 else "red", total_pnl)
    h += '</div>'

    # Trade table
    h += '<table><thead><tr>'
    h += '<th>#</th><th>Time</th><th>TF</th><th>Asset</th><th>Dir</th>'
    h += '<th>Limit</th><th>Ask</th><th>Fill</th><th>Conf</th><th>PTB</th><th>Result</th>'
    h += '<th>P&L</th><th>Bal</th><th>Hedge</th><th>Market</th><th>Note</th>'
    h += '</tr></thead><tbody>'

    if not trades:
        h += '<tr><td colspan="16" class="empty">No trades yet — watchers are scanning...</td></tr>'
    else:
        for t in trades:
            tid = t.get("id", "")
            fired = t.get("fired_at", "")
            if isinstance(fired, datetime):
                fired_str = fired.strftime("%m-%d %H:%M")
            elif fired:
                fired_str = str(fired)[:16]
            else:
                fired_str = ""

            tf = t.get("timeframe", "")
            asset = t.get("asset", "")
            direction = t.get("direction", "")
            dir_cls = "up" if direction == "UP" else "down"
            limit_p = t.get("limit_price")
            limit_str = "{:.0f}c".format(limit_p) if limit_p else "-"
            book_a = t.get("book_ask")
            ask_str = "{:.0f}c".format(book_a) if book_a else "-"
            order_st = t.get("order_status", "FILLED") or "FILLED"
            if order_st == "FILLED":
                fill_cls = "win"
                fill_str = "FILLED"
            elif order_st == "PENDING":
                fill_cls = "pend"
                fill_str = "PENDING"
            elif order_st == "EXPIRED":
                fill_cls = "loss"
                fill_str = "EXPIRED"
            else:
                fill_cls = ""
                fill_str = order_st
            conf = t.get("confidence", "")
            conf_val = int(conf) if conf and str(conf).isdigit() else 0
            conf_cls = "conf-high" if conf_val >= 80 else "conf-med" if conf_val >= 65 else "conf-low"
            ptb = t.get("ptb")
            ptb_str = "${:,.2f}".format(ptb) if ptb else "-"
            outcome = t.get("outcome") or t.get("status", "OPEN")
            oc_cls = "win" if outcome == "WIN" else "loss" if outcome == "LOSS" else "pend"
            pnl = t.get("pnl")
            pnl_str = "${:+.2f}".format(pnl) if pnl is not None else "-"
            bal_after = t.get("balance_after")
            bal_str = "${:.2f}".format(bal_after) if bal_after else "-"
            hedged = t.get("hedged")
            hedge_str = '<span class="hedge-badge">HEDGED</span>' if hedged else ""
            market_url = t.get("market_url", "")
            if market_url:
                link_str = '<a href="{}" target="_blank" style="color:#60a5fa;text-decoration:none;font-size:0.72rem;">View ↗</a>'.format(
                    _html.escape(market_url))
            else:
                link_str = "-"
            note = _html.escape(str(t.get("entry_note", "") or ""))

            h += '<tr>'
            h += '<td>{}</td><td>{}</td><td>{}</td><td>{}</td>'.format(tid, fired_str, tf, asset)
            h += '<td class="{}">{}</td>'.format(dir_cls, direction)
            h += '<td>{}</td><td>{}</td><td class="{}">{}</td>'.format(limit_str, ask_str, fill_cls, fill_str)
            h += '<td class="{}">{}</td><td>{}</td>'.format(conf_cls, conf, ptb_str)
            h += '<td class="{}">{}</td>'.format(oc_cls, outcome)
            h += '<td>{}</td><td>{}</td><td>{}</td><td>{}</td>'.format(pnl_str, bal_str, hedge_str, link_str)
            h += '<td class="note-cell" title="{}">{}</td>'.format(note, note[:60])
            h += '</tr>'

    h += '</tbody></table></div></body></html>'
    return h


# ═══════════════════════════════════════════════════════════
# LANDING PAGE
# ═══════════════════════════════════════════════════════════

LANDING_HTML = """<!DOCTYPE html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cmvng Bot v2</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700;900&family=JetBrains+Mono:wght@400;500&display=swap');
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'DM Sans',sans-serif; background:#0a0f0d; color:#e8ede9; min-height:100vh; }
.hero { max-width:800px; margin:0 auto; padding:80px 20px 40px; text-align:center; }
.hero h1 { font-size:3rem; font-weight:900; background:linear-gradient(135deg,#4ade80,#22c55e); -webkit-background-clip:text; -webkit-text-fill-color:transparent; margin-bottom:12px; }
.hero p { color:#6b8f74; font-size:1.1rem; max-width:500px; margin:0 auto 32px; line-height:1.6; }
.hero .tagline { font-family:'JetBrains Mono',monospace; color:#4ade80; font-size:0.9rem; border:1px solid #1a2e1f; display:inline-block; padding:8px 20px; border-radius:20px; margin-bottom:40px; }
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:16px; max-width:800px; margin:0 auto 40px; padding:0 20px; }
.card { background:#111a14; border:1px solid #1a2e1f; border-radius:12px; padding:24px; }
.card h3 { color:#4ade80; font-size:1rem; margin-bottom:8px; }
.card p { color:#6b8f74; font-size:0.85rem; line-height:1.5; }
.cta { text-align:center; padding:20px; }
.cta a { color:#0a0f0d; background:#4ade80; padding:12px 32px; border-radius:8px; text-decoration:none; font-weight:700; font-size:0.95rem; }
.cta a:hover { background:#22c55e; }
.stats-row { display:flex; justify-content:center; gap:40px; margin:30px 0; }
.stat { text-align:center; }
.stat .num { font-family:'JetBrains Mono',monospace; font-size:1.8rem; font-weight:700; color:#4ade80; }
.stat .lab { font-size:0.75rem; color:#6b8f74; text-transform:uppercase; letter-spacing:0.5px; }
</style></head><body>
<div class="hero">
<div class="tagline">CONFIRMATION TRADING ENGINE</div>
<h1>Cmvng Bot v2</h1>
<p>Not prediction. Confirmation. Wait for the candle to form. Confirm direction won't reverse. Enter late at high odds.</p>
<div class="stats-row">
<div class="stat"><div class="num">{{ paper_total }}</div><div class="lab">Paper Trades</div></div>
<div class="stat"><div class="num">{{ wr }}%</div><div class="lab">Win Rate</div></div>
<div class="stat"><div class="num">${{ balance }}</div><div class="lab">Paper Balance</div></div>
</div>
</div>
<div class="cards">
<div class="card"><h3>🕐 Hourly</h3><p>5M intra-hour candles. HH/HL structure. Scans both Polymarket + Limitless. $3 flat per entry.</p></div>
<div class="card"><h3>⚡ 15-Min</h3><p>1M candle structure. Stricter confidence. Both platforms. Enter when the move is obvious.</p></div>
<div class="card"><h3>📅 Daily</h3><p>Hourly candles for intra-day structure. Both platforms. Session-safe. Scans every 10 minutes.</p></div>
</div>
<div class="cta"><a href="/app/paper-poly">View Dashboard →</a></div>
</body></html>"""


# ═══════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════

@app.route("/")
def landing():
    try:
        conn = get_db()
        rows = conn.run("SELECT COUNT(*), COALESCE(SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN outcome IN ('WIN','LOSS') THEN 1 ELSE 0 END), 0) FROM v2_paper_trades")
        row = list(rows)[0] if rows else (0, 0, 0)
        paper_total = int(row[0] or 0)
        wins = int(row[1] or 0)
        resolved = int(row[2] or 0)
        conn.close()
    except:
        paper_total = 0; wins = 0; resolved = 0

    wr = round(wins / resolved * 100, 1) if resolved > 0 else 0
    balance = _v2_balances.get("polymarket", {}).get("balance", 100)

    return render_template_string(LANDING_HTML,
        paper_total=paper_total, wr=wr, balance="{:.2f}".format(balance))


@app.route("/app/paper-poly")
def paper_poly():
    try:
        conn = get_db()
        rows = conn.run("SELECT * FROM v2_paper_trades WHERE platform = 'polymarket' ORDER BY id DESC LIMIT 200")
        cols = [c['name'] for c in conn.columns]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()
    except:
        trades = []
    bal = _v2_balances.get("polymarket", {"balance": 100, "peak_balance": 100})
    return _v2_dashboard_html("polymarket", trades, bal)


@app.route("/app/paper-limitless")
def paper_limitless():
    try:
        conn = get_db()
        rows = conn.run("SELECT * FROM v2_paper_trades WHERE platform = 'limitless' ORDER BY id DESC LIMIT 200")
        cols = [c['name'] for c in conn.columns]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()
    except:
        trades = []
    bal = _v2_balances.get("limitless", {"balance": 100, "peak_balance": 100})
    return _v2_dashboard_html("limitless", trades, bal)


@app.route("/v2/status")
def v2_status():
    now = datetime.now(timezone.utc)
    session_label, session_safe = _v2_session_filter(now.hour)

    status = {
        "engine": "CMVNG BOT v2",
        "mode": "PAPER",
        "utc_time": now.isoformat(),
        "lagos_time": now.astimezone(LAGOS_TZ).strftime("%Y-%m-%d %H:%M"),
        "session": {"label": session_label, "safe": session_safe},
        "rtds": {"connected": _chainlink_connected, "prices": dict(_chainlink_prices)},
        "balances": dict(_v2_balances),
        "active_boundaries": len(_v2_active_boundaries),
        "threads": {
            "hourly_watcher": "scanning every 2min",
            "fifteen_min_watcher": "scanning every 1min",
            "daily_watcher": "scanning every 10min",
            "monitor": "hedge check every 30s",
            "resolver": "resolve every 60s",
        },
    }
    return jsonify(status)


@app.route("/v2/trades")
def v2_trades_api():
    """API endpoint for trade data."""
    platform = request.args.get("platform", "polymarket")
    timeframe = request.args.get("timeframe", "")
    limit = int(request.args.get("limit", 100))

    try:
        conn = get_db()
        query = "SELECT * FROM v2_paper_trades WHERE platform = :p"
        params = {"p": platform}
        if timeframe:
            query += " AND timeframe = :tf"
            params["tf"] = timeframe
        query += " ORDER BY id DESC LIMIT :lim"
        params["lim"] = limit
        rows = conn.run(query, **params)
        cols = [c['name'] for c in conn.columns]
        trades = [dict(zip(cols, r)) for r in rows]
        conn.close()

        # Serialize datetimes
        for t in trades:
            for k, v in t.items():
                if isinstance(v, datetime):
                    t[k] = v.isoformat()

        return jsonify({"trades": trades, "balance": _v2_balances.get(platform, {})})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/v2/prices")
def v2_prices():
    """Current Chainlink prices."""
    return jsonify({
        "connected": _chainlink_connected,
        "prices": dict(_chainlink_prices),
        "ptb": {k: {"ts": v[0], "price": v[1]} for k, v in _chainlink_ptb.items()},
    })


# ═══════════════════════════════════════════════════════════
# SPORTS PREDICTION MODULE — Football Consensus Scanner v2
# ═══════════════════════════════════════════════════════════
# Scrapes prediction sites (league-specific pages for overlap),
# finds consensus, matches against Polymarket sports markets,
# scores picks, sends Telegram alerts.
# v2 fixes: correct Polymarket API (/public-search, /sports metadata,
#   /markets?tag_id + sports_market_types), league-targeted scraping,
#   robust score/probability parsing.
# ═══════════════════════════════════════════════════════════

import requests as _sports_req
from bs4 import BeautifulSoup
import re as _sports_re

SPORTS_SCAN_INTERVAL = 21600  # 6 hours between full scans
SPORTS_MIN_SCORE = 30         # Lower for testing — raise to 70 once validated
SPORTS_SOURCES = [
    "footballpredictions.com",
    "forebet.com",
    "footballpredictions.net",
]

# League pages that BOTH FP.com and Forebet cover — ensures overlap
_SPORTS_LEAGUES = {
    "epl": {
        "fp": "https://footballpredictions.com/footballpredictions/premierleaguepredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/england/premier-league",
    },
    "la_liga": {
        "fp": "https://footballpredictions.com/footballpredictions/primeradivisionpredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/spain/la-liga",
    },
    "serie_a": {
        "fp": "https://footballpredictions.com/footballpredictions/serieapredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/italy/serie-a",
    },
    "bundesliga": {
        "fp": "https://footballpredictions.com/footballpredictions/bundesligapredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/germany/bundesliga",
    },
    "ligue_1": {
        "fp": "https://footballpredictions.com/footballpredictions/ligue1predictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/france/ligue-1",
    },
    "ucl": {
        "fp": "https://footballpredictions.com/footballpredictions/championsleaguepredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/champions-league",
    },
    "uel": {
        "fp": "https://footballpredictions.com/footballpredictions/europaleaguepredictions/",
        "forebet": "https://www.forebet.com/en/football-predictions/europa-league",
    },
}

_sports_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}

# Cache Polymarket soccer tag_id so we don't refetch every scan
_sports_poly_soccer_tag_id = None


def _sports_scrape_footballpredictions_com():
    """Scrape footballpredictions.com — both tip pages AND league-specific pages."""
    predictions = []

    # Tip pages (broad coverage — friendlies, playoffs, etc.)
    tip_pages = [
        ("correct-score", "https://footballpredictions.com/betting-tips/correct-score/"),
        ("over-2-5", "https://footballpredictions.com/betting-tips/over-2-5-goals/"),
        ("btts", "https://footballpredictions.com/betting-tips/btts/"),
        ("predictions", "https://footballpredictions.com/footballpredictions/"),
    ]
    # League pages (targeted — same leagues as Forebet for overlap)
    for league_key, urls in _SPORTS_LEAGUES.items():
        fp_url = urls.get("fp")
        if fp_url:
            tip_pages.append((league_key, fp_url))

    for tip_type, url in tip_pages:
        try:
            r = _sports_req.get(url, headers=_sports_headers, timeout=15)
            if r.status_code != 200:
                print("[SPORTS] FP.com {} — HTTP {}".format(tip_type, r.status_code))
                continue
            soup = BeautifulSoup(r.text, "html.parser")

            # Find links with "-vs-" in href
            links = soup.find_all("a", href=True)
            for link in links:
                href = link.get("href", "")
                text = link.get_text(" ", strip=True)
                if "-vs-" in href.lower():
                    for part in href.split("/"):
                        if "-vs-" in part.lower():
                            teams = _sports_re.sub(r'-prediction.*|-tips.*|-betting.*|-odds.*|-preview.*', '', part)
                            team_parts = teams.split("-vs-")
                            if len(team_parts) == 2:
                                home = team_parts[0].replace("-", " ").strip().title()
                                away = team_parts[1].replace("-", " ").strip().title()
                                if len(home) > 2 and len(away) > 2:
                                    # Get parent context for score extraction
                                    parent = link.parent
                                    if parent:
                                        grandparent = parent.parent
                                    else:
                                        grandparent = None
                                    # Search in widening context
                                    contexts = [text]
                                    if parent:
                                        contexts.append(parent.get_text(" ", strip=True))
                                    if grandparent:
                                        contexts.append(grandparent.get_text(" ", strip=True))

                                    score = None
                                    for ctx in contexts:
                                        # Pattern 1: "Prediction: 2-1" or "Score: 1-0"
                                        sm = _sports_re.search(
                                            r'(?:score|prediction|tip|result)[:\s]*(\d)\s*[-–:]\s*(\d)',
                                            ctx.lower())
                                        if sm:
                                            h, a = int(sm.group(1)), int(sm.group(2))
                                            if h <= 6 and a <= 6:
                                                score = "{}-{}".format(h, a)
                                                break
                                        # Pattern 2: standalone low-digit score NOT inside large numbers
                                        sms = _sports_re.findall(
                                            r'(?<![0-9/])([0-5])\s*[-]\s*([0-5])(?![0-9/])', ctx)
                                        for sm2 in sms:
                                            candidate = "{}-{}".format(sm2[0], sm2[1])
                                            # Reject common false positives
                                            if candidate not in ("0-0",) and candidate not in ctx[:3]:
                                                score = candidate
                                                break
                                        if score:
                                            break

                                    predictions.append({
                                        "source": "footballpredictions.com",
                                        "type": tip_type,
                                        "home": home, "away": away,
                                        "score": score,
                                        "text": (contexts[1] if len(contexts) > 1 else text)[:200],
                                    })
                            break
                # Also try text-based matching for links without -vs- in href
                elif " vs " in text.lower() or " v " in text.lower():
                    vs_match = _sports_re.search(r'(.+?)\s+(?:vs?\.?)\s+(.+?)$', text)
                    if vs_match:
                        home = vs_match.group(1).strip()[:40]
                        away = vs_match.group(2).strip()[:40]
                        if len(home) > 2 and len(away) > 2:
                            predictions.append({
                                "source": "footballpredictions.com",
                                "type": tip_type,
                                "home": home, "away": away,
                                "score": None,
                                "text": text[:200],
                            })

            count = len([p for p in predictions if p["type"] == tip_type])
            print("[SPORTS] FP.com {}: {} predictions".format(tip_type, count))
        except Exception as e:
            print("[SPORTS] FP.com {} error: {}".format(tip_type, e))

    # Deduplicate
    seen = set()
    unique = []
    for p in predictions:
        key = (_sports_normalize_team(p["home"]), _sports_normalize_team(p["away"]), p["type"])
        if key not in seen:
            seen.add(key)
            unique.append(p)

    print("[SPORTS] FP.com total: {} unique predictions".format(len(unique)))
    for p in unique[:3]:
        print("[SPORTS] FP.com sample: {} vs {} ({}) → {}".format(
            p["home"], p["away"], p["type"], p.get("score", "?")))
    return unique


def _sports_scrape_footballpredictions_net():
    """Scrape footballpredictions.net for correct score predictions."""
    predictions = []
    url = "https://footballpredictions.net/correct-score-predictions-betting-tips"
    try:
        r = _sports_req.get(url, headers=_sports_headers, timeout=15)
        if r.status_code != 200:
            print("[SPORTS] FP.net — HTTP {}".format(r.status_code))
            return predictions
        soup = BeautifulSoup(r.text, "html.parser")

        # FP.net uses table rows with match data
        # Look for links to individual match pages — they contain team names
        links = soup.find_all("a", href=True)
        for link in links:
            href = link.get("href", "")
            text = link.get_text(" ", strip=True)
            # Match pages have format: /team-a-vs-team-b-prediction/
            if "-vs-" in href and "prediction" in href:
                # Extract teams from the href
                parts = href.split("/")
                for part in parts:
                    if "-vs-" in part:
                        teams = part.replace("-prediction", "").replace("-tips", "")
                        team_parts = teams.split("-vs-")
                        if len(team_parts) == 2:
                            home = team_parts[0].replace("-", " ").strip().title()
                            away = team_parts[1].replace("-", " ").strip().title()
                            if len(home) > 2 and len(away) > 2:
                                # Look for score in nearby text
                                parent = link.parent
                                parent_text = parent.get_text(" ", strip=True) if parent else text
                                score_match = _sports_re.search(r'(\d)\s*[-–:]\s*(\d)', parent_text)
                                score = "{}-{}".format(score_match.group(1), score_match.group(2)) if score_match else None
                                predictions.append({
                                    "source": "footballpredictions.net",
                                    "type": "correct-score",
                                    "home": home, "away": away,
                                    "score": score,
                                    "text": parent_text[:200] if parent_text else text[:200],
                                })

        # Deduplicate
        seen = set()
        unique = []
        for p in predictions:
            key = (_sports_normalize_team(p["home"]), _sports_normalize_team(p["away"]))
            if key not in seen:
                seen.add(key)
                unique.append(p)
        predictions = unique

        print("[SPORTS] FP.net: {} predictions".format(len(predictions)))
        # Debug: show first 3
        for p in predictions[:3]:
            print("[SPORTS] FP.net sample: {} vs {} → {}".format(p["home"], p["away"], p.get("score", "?")))
    except Exception as e:
        print("[SPORTS] FP.net error: {}".format(e))
    return predictions


def _sports_scrape_forebet():
    """Scrape Forebet for mathematical predictions — 1X2, over/under, BTTS, correct score.
    Scrapes both the main today page AND league-specific pages for overlap with FP.com."""
    predictions = []

    # Main today page + all league-specific pages
    urls_to_scrape = [
        ("today", "https://www.forebet.com/en/football-tips-and-predictions-for-today"),
    ]
    for league_key, league_urls in _SPORTS_LEAGUES.items():
        forebet_url = league_urls.get("forebet")
        if forebet_url:
            urls_to_scrape.append((league_key, forebet_url))

    for page_name, url in urls_to_scrape:
        try:
            r = _sports_req.get(url, headers=_sports_headers, timeout=15)
            if r.status_code != 200:
                print("[SPORTS] Forebet {} — HTTP {}".format(page_name, r.status_code))
                continue
            soup = BeautifulSoup(r.text, "html.parser")

            # Forebet match rows: div.rcnt or tr with class starting with "tr_"
            rows = soup.find_all("div", class_="rcnt")
            if not rows:
                rows = soup.find_all("tr", class_=_sports_re.compile(r"tr_|pred"))
            # Fallback: look inside contentmiddle container
            if not rows:
                container = soup.find("div", id="contentmiddle") or soup.find("section", class_="schema")
                if container:
                    rows = container.find_all("div", class_=_sports_re.compile(r"rcnt|predictionRow"))

            for row in rows:
                text = row.get_text(" ", strip=True)
                if len(text) < 10:
                    continue

                # Extract teams — multiple selector strategies
                home = away = None

                # Strategy 1: homeTeam/awayTeam spans
                home_el = row.find("span", class_=_sports_re.compile(r"homeTeam|home_team"))
                away_el = row.find("span", class_=_sports_re.compile(r"awayTeam|away_team"))
                if home_el and away_el:
                    home = home_el.get_text(strip=True)[:40]
                    away = away_el.get_text(strip=True)[:40]

                # Strategy 2: tnms container with two spans/anchors
                if not home or not away:
                    tnms = row.find("span", class_="tnms") or row.find("div", class_="tnms")
                    if tnms:
                        team_els = tnms.find_all("a") or tnms.find_all("span")
                        if len(team_els) >= 2:
                            home = team_els[0].get_text(strip=True)[:40]
                            away = team_els[1].get_text(strip=True)[:40]
                        else:
                            # Single element with "vs" or "-" separator
                            tnms_text = tnms.get_text(" ", strip=True)
                            vs_m = _sports_re.search(r'(.+?)\s+(?:vs?\.?|[-–])\s+(.+)', tnms_text)
                            if vs_m:
                                home = vs_m.group(1).strip()[:40]
                                away = vs_m.group(2).strip()[:40]

                # Strategy 3: href-based extraction
                if not home or not away:
                    match_link = row.find("a", href=_sports_re.compile(r"-vs-|-against-"))
                    if match_link:
                        href = match_link.get("href", "")
                        for part in href.split("/"):
                            if "-vs-" in part:
                                team_parts = part.split("-vs-")
                                if len(team_parts) == 2:
                                    home = team_parts[0].replace("-", " ").strip().title()[:40]
                                    away = team_parts[1].replace("-", " ").strip().title()[:40]
                                break

                # Strategy 4: regex on row text
                if not home or not away:
                    vs_match = _sports_re.search(r'(.{3,30}?)\s+(?:vs?\.?|[-–])\s+(.{3,30}?)(?:\s+\d|$)', text)
                    if vs_match:
                        home = vs_match.group(1).strip()[:40]
                        away = vs_match.group(2).strip()[:40]

                if not home or not away or len(home) < 3 or len(away) < 3:
                    continue

                # Extract 1X2 probabilities — try multiple CSS class patterns
                prob_1 = prob_x = prob_2 = None
                for prob_class in [r"fpr\b", r"fprc\b", r"predict", r"prob"]:
                    probs = row.find_all("span", class_=_sports_re.compile(prob_class))
                    if len(probs) >= 3:
                        try:
                            prob_1 = int(probs[0].get_text(strip=True).replace("%", ""))
                            prob_x = int(probs[1].get_text(strip=True).replace("%", ""))
                            prob_2 = int(probs[2].get_text(strip=True).replace("%", ""))
                            if prob_1 + prob_x + prob_2 > 50:  # Sanity: should sum near 100
                                break
                            else:
                                prob_1 = prob_x = prob_2 = None
                        except (ValueError, IndexError):
                            prob_1 = prob_x = prob_2 = None

                # Fallback: look for percentage values in text
                if not prob_1:
                    pct_matches = _sports_re.findall(r'(\d{1,2})%', text)
                    if len(pct_matches) >= 3:
                        try:
                            vals = [int(x) for x in pct_matches[:3]]
                            if 80 < sum(vals) < 120:
                                prob_1, prob_x, prob_2 = vals
                        except:
                            pass

                # Extract correct score prediction
                score = None
                for sc_class in [r"ex_sc", r"foremark", r"scorePred", r"correct.?score"]:
                    score_el = row.find("span", class_=_sports_re.compile(sc_class))
                    if score_el:
                        sc_text = score_el.get_text(strip=True)
                        if _sports_re.match(r'\d+-\d+$', sc_text):
                            score = sc_text
                            break

                # Fallback: look for score pattern in specific containers
                if not score:
                    for tag in row.find_all(["span", "td", "div"]):
                        t = tag.get_text(strip=True)
                        if _sports_re.match(r'^[0-5]-[0-5]$', t):
                            score = t
                            break

                # Extract over/under average goals
                avg_goals = None
                for ou_class in [r"ou_", r"avg_goals", r"total"]:
                    ou_el = row.find("span", class_=_sports_re.compile(ou_class))
                    if ou_el:
                        try:
                            avg_goals = float(ou_el.get_text(strip=True))
                            break
                        except:
                            pass

                predictions.append({
                    "source": "forebet.com",
                    "type": "full",
                    "league": page_name,
                    "home": home, "away": away,
                    "score": score,
                    "prob_home": prob_1, "prob_draw": prob_x, "prob_away": prob_2,
                    "avg_goals": avg_goals,
                    "text": text[:200],
                })

            page_count = len([p for p in predictions if p.get("league") == page_name])
            print("[SPORTS] Forebet {}: {} predictions".format(page_name, page_count))
        except Exception as e:
            print("[SPORTS] Forebet {} error: {}".format(page_name, e))

    # Deduplicate across all pages
    seen = set()
    unique = []
    for p in predictions:
        key = (_sports_normalize_team(p["home"]), _sports_normalize_team(p["away"]))
        if key not in seen:
            seen.add(key)
            unique.append(p)

    print("[SPORTS] Forebet total: {} unique predictions".format(len(unique)))
    for p in unique[:3]:
        print("[SPORTS] Forebet sample: {} vs {} → {} ({}% / {}% / {}%)".format(
            p["home"], p["away"], p.get("score", "?"),
            p.get("prob_home", "?"), p.get("prob_draw", "?"), p.get("prob_away", "?")))
    return unique


def _sports_scrape_predictz():
    """Scrape PredictZ — often blocked (403). Gracefully skip if so."""
    predictions = []
    url = "https://www.predictz.com/predictions/today/"
    try:
        r = _sports_req.get(url, headers=_sports_headers, timeout=10)
        if r.status_code != 200:
            print("[SPORTS] PredictZ — HTTP {} (blocked, skipping)".format(r.status_code))
            return predictions
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.find_all("tr", class_=_sports_re.compile(r"pointed|pttr"))
        if not rows:
            rows = soup.find_all("div", class_=_sports_re.compile(r"match|fixture"))
        for row in rows:
            text = row.get_text(" ", strip=True)
            if not text or len(text) < 10:
                continue
            vs_match = _sports_re.search(r'(.+?)\s+(?:vs?\.?|[-–])\s+(.+?)(?:\s+\d|$)', text)
            if vs_match:
                home = vs_match.group(1).strip()[:40]
                away = vs_match.group(2).strip()[:40]
                score_match = _sports_re.search(r'(\d)\s*[-–:]\s*(\d)', text)
                score = "{}-{}".format(score_match.group(1), score_match.group(2)) if score_match else None
                predictions.append({
                    "source": "predictz.com",
                    "type": "correct-score",
                    "home": home, "away": away,
                    "score": score,
                    "text": text[:200],
                })
        print("[SPORTS] PredictZ: {} predictions".format(len(predictions)))
    except Exception as e:
        print("[SPORTS] PredictZ error: {}".format(e))
    return predictions


def _sports_normalize_team(name):
    """Normalize team names for matching across sources."""
    if not name:
        return ""
    n = name.lower().strip()
    # Remove common suffixes/prefixes
    for suffix in [" fc", " cf", " sc", " ac", " afc", " united", " city",
                   " town", " rovers", " wanderers", " athletic", " sporting",
                   " de ", " del "]:
        n = n.replace(suffix, " ")
    # Common abbreviations
    abbrevs = {
        "psg": "paris saint germain",
        "man utd": "manchester united", "man united": "manchester united",
        "man city": "manchester city",
        "spurs": "tottenham", "tottenham hotspur": "tottenham",
        "wolves": "wolverhampton",
        "newcastle utd": "newcastle",
        "west ham utd": "west ham",
        "real madrid": "real madrid", "r madrid": "real madrid",
        "atletico madrid": "atletico",
        "atletico": "atletico",
        "inter milan": "inter", "internazionale": "inter",
        "ac milan": "milan",
        "bayern munich": "bayern", "bayern munchen": "bayern",
        "borussia dortmund": "dortmund", "bvb": "dortmund",
        "rb leipzig": "leipzig",
        "st etienne": "saint etienne",
    }
    for abbr, full in abbrevs.items():
        if abbr in n:
            n = n.replace(abbr, full)
    # Remove special chars
    n = _sports_re.sub(r'[^a-z0-9 ]', '', n)
    n = _sports_re.sub(r'\s+', ' ', n).strip()
    return n


def _sports_match_teams(pred_home, pred_away, market_text):
    """Check if a prediction's teams match a market title.
    Uses fuzzy matching — any significant word from BOTH teams must appear."""
    mt = _sports_normalize_team(market_text)

    ph = _sports_normalize_team(pred_home)
    pa = _sports_normalize_team(pred_away)

    # Get significant words (>2 chars) from each team
    ph_words = [w for w in ph.split() if len(w) > 2]
    pa_words = [w for w in pa.split() if len(w) > 2]

    if not ph_words or not pa_words:
        return False

    # At least one significant word from each team must be in the market text
    home_match = any(w in mt for w in ph_words)
    away_match = any(w in mt for w in pa_words)

    return home_match and away_match


def _sports_fetch_polymarket_sports(match_pairs=None):
    """Fetch soccer/football markets from Polymarket using correct API endpoints.

    Strategy:
    1. GET /sports → get soccer tag_id and series info
    2. GET /markets?tag_id=X&closed=false → all active soccer markets
    3. GET /public-search?q=<team names> → match-specific markets
    4. GET /markets?sports_market_types=moneyline,total → match-day markets

    The /search endpoint doesn't exist — use /public-search (documented)."""
    global _sports_poly_soccer_tag_id
    markets = []
    seen_ids = set()

    # Step 1: Get soccer-specific tag_ids from /sports metadata (cached)
    # tag_id=1 is shared across ALL sports — useless for filtering.
    # We need sport-specific tags like EPL=82, UCL=306, etc.
    if not _sports_poly_soccer_tag_id:
        try:
            r = _sports_req.get("{}/sports".format(POLY_GAMMA_API), timeout=10)
            if r.status_code == 200:
                sports_list = r.json() if isinstance(r.json(), list) else []
                print("[SPORTS] Poly /sports returned {} sports".format(len(sports_list)))
                soccer_tag_ids = set()
                soccer_sport_codes = ["epl", "ucl", "uel", "ser", "bun", "lig",  # leagues
                                       "mls", "lcu", "acn", "fif", "es2", "cdr",
                                       "ucf", "soc", "football"]
                for sport in sports_list:
                    sport_code = (sport.get("sport", "") or "").lower()
                    if any(sc in sport_code for sc in soccer_sport_codes):
                        tags_str = str(sport.get("tags", ""))
                        for t in tags_str.split(","):
                            t = t.strip()
                            if t and t != "1":  # Skip tag_id=1 (shared by all)
                                soccer_tag_ids.add(t)
                if soccer_tag_ids:
                    _sports_poly_soccer_tag_id = ",".join(list(soccer_tag_ids)[:5])
                    print("[SPORTS] Poly soccer tag_ids: {} (from {} soccer sports)".format(
                        _sports_poly_soccer_tag_id,
                        sum(1 for s in sports_list
                            if any(sc in (s.get("sport","") or "").lower() for sc in soccer_sport_codes))))
                else:
                    _sports_poly_soccer_tag_id = "NONE"
                    print("[SPORTS] No soccer-specific tags found, will rely on search + smt only")
        except Exception as e:
            print("[SPORTS] Poly /sports error: {}".format(e))

    def _parse_market(m, event_title=""):
        """Parse a market object into our standard format.
        Rejects stale markets (dates more than 3 days from today)."""
        mid = str(m.get("id", "") or m.get("conditionId", ""))
        if not mid or mid in seen_ids:
            return None
        seen_ids.add(mid)
        q = m.get("question", "") or ""
        slug = m.get("slug", "") or ""

        # Date freshness check — reject markets with dates > 3 days from today
        # Slugs often contain dates like "2026-05-30" or "2026-05-28"
        import datetime as _dt
        today = _dt.date.today()
        date_match = _sports_re.search(r'(\d{4})-(\d{2})-(\d{2})', slug)
        if date_match:
            try:
                market_date = _dt.date(int(date_match.group(1)),
                                        int(date_match.group(2)),
                                        int(date_match.group(3)))
                days_diff = abs((market_date - today).days)
                if days_diff > 3:
                    return None  # Stale or too far future
            except:
                pass

        op = m.get("outcomePrices", "")
        if isinstance(op, str):
            try:
                op = json.loads(op)
            except:
                op = []
        outcomes = m.get("outcomes", "")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except:
                outcomes = []
        # Build URL — use /event/{slug} for individual market pages
        market_url = ""
        if slug:
            market_url = "https://polymarket.com/event/{}".format(slug)
        return {
            "platform": "polymarket",
            "title": event_title or q,
            "question": q,
            "slug": slug,
            "market_id": mid,
            "condition_id": m.get("conditionId", ""),
            "outcome_prices": op,
            "outcomes": outcomes,
            "volume": float(m.get("volume", 0) or 0),
            "url": market_url,
            "best_ask": float(m.get("bestAsk", 0) or 0),
            "last_price": float(m.get("lastTradePrice", 0) or 0),
            "game_id": m.get("gameId", "") or "",
            "sports_market_type": m.get("sportsMarketType", "") or "",
        }

    # Step 2: Fetch active soccer markets by soccer-specific tag_ids
    if _sports_poly_soccer_tag_id and _sports_poly_soccer_tag_id != "NONE":
        for tag_id in _sports_poly_soccer_tag_id.split(",")[:3]:  # Query top 3 tags
            try:
                r = _sports_req.get("{}/markets".format(POLY_GAMMA_API),
                                   params={"tag_id": tag_id,
                                           "closed": False, "limit": 50,
                                           "order": "volume", "ascending": False},
                                   timeout=15)
                if r.status_code == 200:
                    data = r.json() if isinstance(r.json(), list) else []
                    tag_count = 0
                    for m in data:
                        parsed = _parse_market(m)
                        if parsed:
                            markets.append(parsed)
                            tag_count += 1
                    if tag_count:
                        print("[SPORTS] Poly tag={}: {} markets".format(tag_id, tag_count))
                time.sleep(0.2)
            except Exception as e:
                print("[SPORTS] Poly tag={} error: {}".format(tag_id, e))

    # Step 3: Fetch match-day sports markets by type (moneyline, total, btts)
    for smt in ["moneyline", "total", "btts", "spread"]:
        try:
            r = _sports_req.get("{}/markets".format(POLY_GAMMA_API),
                               params={"sports_market_types": smt,
                                       "closed": False, "limit": 50},
                               timeout=10)
            if r.status_code == 200:
                data = r.json() if isinstance(r.json(), list) else []
                count = 0
                for m in data:
                    q = (m.get("question", "") or "").lower()
                    smt_val = (m.get("sportsMarketType", "") or "").lower()
                    # EXCLUDE non-soccer sports market types
                    if any(x in smt_val for x in ["tennis", "map_handicap", "esport",
                                                    "round_handicap", "nba", "nfl",
                                                    "nhl", "mlb", "mma", "ufc"]):
                        continue
                    # Filter for soccer — team names, league names, or soccer-specific market types
                    soccer_market_types = ["moneyline", "total", "btts", "spread",
                                           "total_corners", "both_teams_to_score",
                                           "correct_score", "first_goal", "anytime_goal"]
                    is_soccer_smt = any(x in smt_val for x in soccer_market_types)
                    is_soccer_q = any(kw in q for kw in [
                        "goal", "soccer", " fc", "united",
                        "arsenal", "chelsea", "liverpool",
                        "barcelona", "real madrid", "bayern",
                        "psg", "juventus", "dortmund", "inter",
                        "milan", "atletico", "napoli", "benfica",
                        "porto", "ajax", "celtic", "rangers",
                        "premier league", "la liga", "serie a",
                        "bundesliga", "ligue 1", "champions league",
                        "ucl", "europa", "mls", "corner",
                        "both teams to score", "clean sheet",
                    ])
                    if is_soccer_smt or is_soccer_q:
                        parsed = _parse_market(m)
                        if parsed:
                            markets.append(parsed)
                            count += 1
                if count:
                    print("[SPORTS] Poly smt={}: {} soccer markets".format(smt, count))
        except Exception as e:
            print("[SPORTS] Poly smt={} error: {}".format(smt, e))
        time.sleep(0.3)

    # Step 4: Search for specific matches using /public-search
    if match_pairs:
        searched = 0
        for home, away in match_pairs[:15]:  # Limit API calls
            # Use shortened team names for better search results
            home_short = home.split()[-1] if home else ""  # Last word (e.g. "United")
            away_short = away.split()[-1] if away else ""
            query = "{} {}".format(home_short, away_short)
            if len(query.strip()) < 5:
                query = "{} {}".format(home, away)
            try:
                r = _sports_req.get("{}/public-search".format(POLY_GAMMA_API),
                                   params={"q": query, "limit_per_type": 5},
                                   timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    # /public-search returns {events: [...], tags: [...], profiles: [...]}
                    events = []
                    if isinstance(data, dict):
                        events = data.get("events", []) or []
                    elif isinstance(data, list):
                        events = data  # Fallback if format differs

                    for event in events:
                        event_title = event.get("title", "")
                        event_markets = event.get("markets", []) or []
                        if event_markets:
                            for m in event_markets:
                                parsed = _parse_market(m, event_title=event_title)
                                if parsed:
                                    markets.append(parsed)
                        else:
                            # Event itself might be a market
                            parsed = _parse_market(event)
                            if parsed:
                                markets.append(parsed)
                searched += 1
                time.sleep(0.3)
            except Exception as e:
                print("[SPORTS] Poly search '{}' error: {}".format(query[:30], e))
                searched += 1

    # Step 5: Also broad soccer searches for futures/props
    for broad_q in ["world cup", "champions league", "premier league",
                    "ballon d'or", "golden boot"]:
        try:
            r = _sports_req.get("{}/public-search".format(POLY_GAMMA_API),
                               params={"q": broad_q, "limit_per_type": 5},
                               timeout=10)
            if r.status_code == 200:
                data = r.json()
                events = data.get("events", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for event in (events or []):
                    event_title = event.get("title", "")
                    for m in (event.get("markets", []) or [event]):
                        parsed = _parse_market(m, event_title=event_title)
                        if parsed:
                            markets.append(parsed)
            time.sleep(0.3)
        except:
            pass

    print("[SPORTS] Polymarket total: {} soccer/football markets".format(len(markets)))
    # Show breakdown
    match_markets = [m for m in markets if m.get("game_id") or m.get("sports_market_type")]
    futures = [m for m in markets if not m.get("game_id") and not m.get("sports_market_type")]
    print("[SPORTS] Poly breakdown: {} match-level, {} futures/other".format(
        len(match_markets), len(futures)))
    # Show soccer-relevant samples (skip generic ones)
    shown = 0
    for m in markets:
        q = (m.get("question", "") or "").lower()
        if shown < 3 and any(kw in q for kw in ["vs", "goal", "corner", "win",
                                                   "fc", "united", "arsenal"]):
            print("[SPORTS] Poly sample: '{}' | smt={} | ask={}".format(
                m.get("question", "")[:60], m.get("sports_market_type", "?"),
                m.get("best_ask", "?")))
            shown += 1
    return markets


def _sports_fetch_limitless_sports():
    """Fetch sports markets from Limitless."""
    markets = []
    try:
        r = _sports_req.get("{}/markets/active/slugs".format(LIMITLESS_API), timeout=10)
        if r.status_code == 200:
            slugs = r.json() if isinstance(r.json(), list) else []
            # Filter out crypto slugs (they contain "up-or-down")
            sports_slugs = [s for s in slugs if "up-or-down" not in str(s).lower()
                           and "above" not in str(s).lower()]
            # Check each non-crypto slug for sports content
            checked = 0
            for slug in sports_slugs:
                if checked >= 50:  # Limit API calls
                    break
                try:
                    mr = _sports_req.get("{}/markets/{}".format(LIMITLESS_API, slug), timeout=8)
                    if mr.status_code == 200:
                        mdata = mr.json()
                        title = (mdata.get("title", "") or mdata.get("question", "") or str(slug)).lower()
                        # Check for any sports/football keywords
                        if any(kw in title for kw in [
                            "win", "goal", "score", "match", "game", "cup", "league",
                            "champion", "final", "premier", "world cup", "fifa", "uefa",
                            "soccer", "football", "fc", "united", "arsenal", "chelsea",
                            "liverpool", "barcelona", "real madrid", "bayern", "psg",
                            "juventus", "inter", "milan", "dortmund", "manager", "sack",
                            "transfer", "sign", "ballon", "golden boot", "relegat",
                            "promot", "trophy", "medal", "coach", "player",
                        ]):
                            markets.append({
                                "platform": "limitless",
                                "title": mdata.get("title", "") or str(slug),
                                "question": mdata.get("question", "") or mdata.get("title", str(slug)),
                                "slug": str(slug),
                                "market_id": str(slug),
                                "url": "https://limitless.exchange/markets/{}".format(slug),
                            })
                    checked += 1
                    time.sleep(0.35)
                except:
                    checked += 1
                    pass
        print("[SPORTS] Limitless: {} sports markets (checked {} slugs)".format(len(markets), checked))
    except Exception as e:
        print("[SPORTS] Limitless fetch error: {}".format(e))
    return markets


def _sports_extract_insights(predictions, home, away):
    """From all predictions for a match, extract actionable insights."""
    insights = {
        "match": "{} vs {}".format(home, away),
        "sources": [],
        "scores": [],
        "home_wins": 0, "draws": 0, "away_wins": 0,
        "total_goals_predicted": [],
        "over_25": 0, "under_25": 0,
        "btts_yes": 0, "btts_no": 0,
        "consensus_winner": None,
        "consensus_goals": None,
        "consensus_btts": None,
    }

    for p in predictions:
        insights["sources"].append(p["source"])
        score = p.get("score")
        if score and _sports_re.match(r'\d+-\d+', score):
            insights["scores"].append({"source": p["source"], "score": score})
            parts = score.split("-")
            try:
                h_goals = int(parts[0])
                a_goals = int(parts[1])
                total = h_goals + a_goals
                insights["total_goals_predicted"].append(total)
                if h_goals > a_goals:
                    insights["home_wins"] += 1
                elif a_goals > h_goals:
                    insights["away_wins"] += 1
                else:
                    insights["draws"] += 1
                if total > 2.5:
                    insights["over_25"] += 1
                else:
                    insights["under_25"] += 1
                if h_goals > 0 and a_goals > 0:
                    insights["btts_yes"] += 1
                else:
                    insights["btts_no"] += 1
            except:
                pass

        # Check tip type
        if p.get("type") == "over-2-5":
            insights["over_25"] += 1
        elif p.get("type") == "btts":
            insights["btts_yes"] += 1

        # Forebet probabilities
        if p.get("prob_home") and p.get("prob_away"):
            if p["prob_home"] > p["prob_away"] and p["prob_home"] > (p.get("prob_draw") or 0):
                insights["home_wins"] += 1
            elif p["prob_away"] > p["prob_home"]:
                insights["away_wins"] += 1
            else:
                insights["draws"] += 1

    n = len(insights["sources"])
    if n > 0:
        if insights["home_wins"] > n / 2:
            insights["consensus_winner"] = home
        elif insights["away_wins"] > n / 2:
            insights["consensus_winner"] = away

        if insights["over_25"] > n / 2:
            insights["consensus_goals"] = "OVER"
        elif insights["under_25"] > n / 2:
            insights["consensus_goals"] = "UNDER"

        if insights["btts_yes"] > n / 2:
            insights["consensus_btts"] = "YES"
        elif insights["btts_no"] > n / 2:
            insights["consensus_btts"] = "NO"

    return insights


def _sports_score_pick(insights, market):
    """Score a potential pick from 0-100."""
    score = 0
    reasons = []
    n_preds = len(insights["sources"])  # Total prediction count (may include duplicates)
    unique_sources = len(set(insights["sources"]))

    # Consensus strength — multi-source (max 30)
    if unique_sources >= 4:
        score += 30
        reasons.append("4+ sites agree")
    elif unique_sources >= 3:
        score += 20
        reasons.append("3+ sites agree")
    elif unique_sources >= 2:
        score += 10
        reasons.append("2 sites agree")
    # Single source gets base points from score predictions below

    # Score predictions support the pick (max 15)
    if insights["scores"]:
        score += min(len(insights["scores"]) * 5, 15)
        reasons.append("{} score predictions".format(len(insights["scores"])))

    # Goals data supports (max 15)
    if insights["total_goals_predicted"]:
        avg = sum(insights["total_goals_predicted"]) / len(insights["total_goals_predicted"])
        if avg > 2.5 and "over" in market.get("question", "").lower():
            score += 15
            reasons.append("Avg predicted goals: {:.1f}".format(avg))
        elif avg < 2.5 and "under" in market.get("question", "").lower():
            score += 15
            reasons.append("Avg predicted goals: {:.1f}".format(avg))
        elif avg > 2.0:
            score += 8
            reasons.append("Goals avg {:.1f}".format(avg))

    # Winner consensus matches market (max 15)
    mq = market.get("question", "").lower()
    if insights["consensus_winner"]:
        winner_norm = _sports_normalize_team(insights["consensus_winner"])
        if any(w in mq for w in winner_norm.split() if len(w) > 3):
            score += 15
            reasons.append("{} consensus winner".format(insights["consensus_winner"]))

    # BTTS consensus (max 10)
    if insights["consensus_btts"] == "YES" and "both" in mq and "score" in mq:
        score += 10
        reasons.append("BTTS YES consensus")

    # Forebet probability (max 15)
    for p in [pred for pred in insights.get("_raw_preds", []) if pred.get("prob_home")]:
        max_prob = max(p.get("prob_home", 0) or 0, p.get("prob_away", 0) or 0)
        if max_prob > 70:
            score += 15
            reasons.append("Forebet {}% confidence".format(max_prob))
            break
        elif max_prob > 55:
            score += 8
            reasons.append("Forebet {}%".format(max_prob))
            break

    return score, reasons


def _sports_scan_and_alert():
    """Main sports scanning function. Scrapes all sites, finds consensus, matches markets, sends alerts."""
    print("[SPORTS] Starting scan...")

    # 1. Scrape all prediction sites
    all_predictions = []
    all_predictions.extend(_sports_scrape_footballpredictions_com())
    all_predictions.extend(_sports_scrape_footballpredictions_net())
    all_predictions.extend(_sports_scrape_forebet())
    all_predictions.extend(_sports_scrape_predictz())
    print("[SPORTS] Total predictions scraped: {}".format(len(all_predictions)))

    if not all_predictions:
        print("[SPORTS] No predictions found — skipping")
        return

    # 2. Group predictions by match (normalize team names)
    # Also handle home/away swaps between prediction sites
    matches = {}
    for p in all_predictions:
        key = (_sports_normalize_team(p["home"]), _sports_normalize_team(p["away"]))
        rev_key = (key[1], key[0])
        if key[0] and key[1]:
            if key in matches:
                matches[key]["predictions"].append(p)
            elif rev_key in matches:
                matches[rev_key]["predictions"].append(p)
            else:
                matches[key] = {"home": p["home"], "away": p["away"], "predictions": []}
                matches[key]["predictions"].append(p)

    print("[SPORTS] Unique matches found: {}".format(len(matches)))

    # Debug: show first 3 matches and their sources
    for i, (key, md) in enumerate(list(matches.items())[:5]):
        sources = set(p["source"] for p in md["predictions"])
        print("[SPORTS] Match {}: '{}' vs '{}' — {} sources: {}".format(
            i+1, key[0], key[1], len(sources), ", ".join(sources)))

    # 3. Fetch markets — search for matches with predictions
    # Count unique SITES per match (not prediction count)
    match_pairs = []
    multi_source_count = 0
    for key, md in matches.items():
        unique_sites = set(p["source"] for p in md["predictions"])
        md["unique_sites"] = len(unique_sites)
        if len(unique_sites) >= 2:
            multi_source_count += 1
        # Include all matches that have at least 1 prediction source
        # (lower threshold for testing — raise to 2 once we have more working scrapers)
        match_pairs.append((md["home"], md["away"]))

    print("[SPORTS] {} matches with 2+ sites, {} total to search".format(
        multi_source_count, len(match_pairs)))
    # Put multi-source matches first in the search queue
    multi_pairs = [(md["home"], md["away"]) for md in matches.values() if md.get("unique_sites", 0) >= 2]
    single_pairs = [(md["home"], md["away"]) for md in matches.values() if md.get("unique_sites", 0) < 2]
    search_pairs = multi_pairs + single_pairs
    poly_markets = _sports_fetch_polymarket_sports(match_pairs=search_pairs[:30])
    lmts_markets = _sports_fetch_limitless_sports()
    all_markets = poly_markets + lmts_markets
    print("[SPORTS] Total sports markets: {} (Poly: {}, Limitless: {})".format(
        len(all_markets), len(poly_markets), len(lmts_markets)))

    # Debug: show first 3 markets
    for i, m in enumerate(all_markets[:3]):
        print("[SPORTS] Sample market {}: '{}' | '{}'".format(
            i+1, m.get("title", "")[:50], m.get("question", "")[:50]))

    if not all_markets:
        print("[SPORTS] No sports markets found — skipping")
        return

    # 4. Match predictions to markets and score
    print("[SPORTS] Starting prediction↔market matching: {} matches × {} markets...".format(
        len(matches), len(all_markets)))
    alerts_sent = 0
    matched_count = 0
    MAX_ALERTS_PER_MATCH = 5  # Cap alerts per match to avoid Telegram spam
    for key, match_data in matches.items():
        home = match_data["home"]
        away = match_data["away"]
        preds = match_data["predictions"]
        sources = set(p["source"] for p in preds)
        match_alerts = 0  # Track alerts for this match
        seen_alert_keys = set()  # Deduplicate same market appearing twice

        # Extract insights
        insights = _sports_extract_insights(preds, home, away)
        insights["_raw_preds"] = preds

        # Find matching markets
        for market in all_markets:
            mq = (market.get("question", "") + " " + market.get("title", "")).lower()
            if _sports_match_teams(home, away, mq):
                matched_count += 1

                # Deduplicate — same game slug with same market type shouldn't alert twice
                mslug = market.get("slug", "")
                smt = market.get("sports_market_type", "") or "general"
                # Normalize slug for dedup: sort team codes so home/away swap doesn't matter
                slug_parts = mslug.replace("-", " ").split()
                alert_key = (tuple(sorted(slug_parts)), smt)
                if alert_key in seen_alert_keys:
                    continue
                seen_alert_keys.add(alert_key)

                # Single source without a score prediction is too weak — skip
                if len(sources) == 1:
                    has_score = any(p.get("score") for p in preds)
                    if not has_score:
                        continue

                # Score this pick
                pick_score, reasons = _sports_score_pick(insights, market)

                # Multi-source bonus
                if len(sources) >= 2:
                    pick_score += 15
                    reasons.append("{} prediction sites".format(len(sources)))
                elif len(sources) == 1:
                    pick_score += 5
                    reasons.append("1 site with score prediction")

                # Log first 5 matches for debugging
                if matched_count <= 5:
                    print("[SPORTS] MATCH: {} vs {} ↔ '{}' — score={} reasons={}".format(
                        home, away, market.get("question", "")[:50],
                        pick_score, ", ".join(reasons[:3])))

                if pick_score >= SPORTS_MIN_SCORE and match_alerts < MAX_ALERTS_PER_MATCH:
                    # Build market type label
                    smt = market.get("sports_market_type", "")
                    smt_labels = {
                        "moneyline": "🏆 Match Winner",
                        "total": "⚽ Over/Under Goals",
                        "totals": "⚽ Over/Under Goals",
                        "btts": "🎯 Both Teams To Score",
                        "both_teams_to_score": "🎯 Both Teams To Score",
                        "spread": "📊 Handicap/Spread",
                        "total_corners": "🔲 Total Corners",
                        "correct_score": "🎯 Correct Score",
                        "first_goal": "1️⃣ First Goal Scorer",
                        "anytime_goal": "⚽ Anytime Goal Scorer",
                    }
                    market_label = smt_labels.get(smt, "📊 {}".format(smt.replace("_", " ").title() if smt else "Market"))
                    mq = market.get("question", "") or market.get("title", "")

                    # Build odds string from outcome prices
                    odds_str = ""
                    op = market.get("outcome_prices", [])
                    oc = market.get("outcomes", [])
                    if op and oc and len(op) == len(oc):
                        odds_parts = []
                        for i, (outcome, price) in enumerate(zip(oc, op)):
                            try:
                                pct = float(price) * 100
                                odds_parts.append("{}: {:.0f}%".format(outcome, pct))
                            except:
                                pass
                        if odds_parts:
                            odds_str = " | ".join(odds_parts)

                    # Build prediction context
                    scores_str = ", ".join("{}: {}".format(s["source"].split(".")[0], s["score"])
                                          for s in insights["scores"][:3])
                    consensus_parts = []
                    if insights["consensus_winner"]:
                        consensus_parts.append("Winner: {}".format(insights["consensus_winner"]))
                    if insights["consensus_goals"]:
                        consensus_parts.append("Goals: {} 2.5".format(insights["consensus_goals"]))
                    if insights["consensus_btts"]:
                        consensus_parts.append("BTTS: {}".format(insights["consensus_btts"]))

                    msg = (
                        "⚽ <b>SPORTS PICK</b>\n"
                        "🏟 <b>{home} vs {away}</b>\n\n"
                        "{market_label}\n"
                        "📊 <b>{question}</b>\n"
                        "{odds_line}"
                        "🔗 {url}\n\n"
                        "📈 Prediction ({n_sites} site{s}):\n"
                        "{consensus}\n"
                        "{scores_line}"
                        "💡 {reasons}\n"
                        "⭐ Confidence: {score}/100"
                    ).format(
                        home=home, away=away,
                        market_label=market_label,
                        question=mq,
                        odds_line="💰 Odds: {}\n".format(odds_str) if odds_str else "",
                        url=market.get("url", ""),
                        n_sites=len(sources),
                        s="" if len(sources) == 1 else "s",
                        consensus=" | ".join(consensus_parts) if consensus_parts else "N/A",
                        scores_line="🎯 Score predictions: {}\n".format(scores_str) if scores_str else "",
                        reasons=" | ".join(reasons[:4]),
                        score=pick_score,
                    )
                    send_telegram(msg)
                    alerts_sent += 1
                    match_alerts += 1
                    print("[SPORTS] ALERT: {} vs {} — {} — score {}/100 | {}".format(
                        home, away, smt or "general", pick_score,
                        market.get("url", "")[:50]))

    print("[SPORTS] Scan complete — {} predictions matched to markets, {} alerts sent".format(
        matched_count, alerts_sent))


def _sports_scanner_thread():
    """Background thread that runs sports scanning periodically."""
    print("[SPORTS] Scanner thread started")
    while True:
        try:
            _sports_scan_and_alert()
        except Exception as e:
            print("[SPORTS] Scanner error: {}".format(e))
            import traceback; traceback.print_exc()
        time.sleep(SPORTS_SCAN_INTERVAL)


# Sports dashboard page
@app.route("/app/sports")
def sports_dashboard():
    return render_template_string("""
    <html><head><title>Sports Predictions</title>
    <style>
    body { font-family: 'DM Sans', sans-serif; background: #f0faf0; padding: 20px; }
    h1 { color: #2d6a4f; }
    .info { background: white; border-radius: 12px; padding: 20px; margin: 10px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
    .sources { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 10px; }
    .src { background: #e8f5e9; padding: 10px; border-radius: 8px; font-size: 14px; }
    .src b { color: #2d6a4f; }
    code { background: #f5f5f5; padding: 2px 6px; border-radius: 4px; font-size: 13px; }
    </style></head><body>
    <h1>⚽ Sports Prediction Scanner v2</h1>
    <div class="info">
        <p><strong>Status:</strong> Running (scans every 6 hours)</p>
        <p><strong>Minimum score:</strong> {{ min_score }}/100</p>
        <p><strong>Strategy:</strong> Scrape same leagues from multiple sites → find consensus → match to Polymarket markets → alert on Telegram</p>
    </div>
    <div class="info">
        <h3>Prediction Sources</h3>
        <div class="sources">
            <div class="src"><b>footballpredictions.com</b><br>Tips + league pages (EPL, La Liga, UCL, etc.)</div>
            <div class="src"><b>forebet.com</b><br>Mathematical predictions + league pages (same leagues)</div>
            <div class="src"><b>footballpredictions.net</b><br>Correct scores (backup)</div>
            <div class="src"><b>predictz.com</b><br>Scores (often blocked, graceful skip)</div>
        </div>
    </div>
    <div class="info">
        <h3>Market Sources</h3>
        <p><b>Polymarket</b> — <code>/public-search</code> + <code>/sports</code> metadata + <code>/markets?tag_id</code> + <code>sports_market_types</code></p>
        <p><b>Limitless</b> — Active slug scan (usually no individual match markets)</p>
    </div>
    <div class="info">
        <h3>Leagues Targeted (for cross-site overlap)</h3>
        <p>EPL, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, Europa League</p>
    </div>
    </body></html>
    """, min_score=SPORTS_MIN_SCORE)


# ═══════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════

print("=" * 60)
print("CMVNG BOT v2 — CONFIRMATION TRADING ENGINE")
print("=" * 60)

try:
    init_db()
    # reset_db()  # Uncomment to reset — already ran once on first deploy
    _v2_load_balances()
    print("[V2] Balances: {}".format(
        ", ".join("{}=${:.2f}".format(k, v["balance"]) for k, v in _v2_balances.items())))
except Exception as e:
    print("[V2] DB init error: {}".format(e))

# Start RTDS thread
threading.Thread(target=_rtds_loop, daemon=True, name="v2-rtds").start()
print("[V2] RTDS thread launched")

# Start watcher threads
threading.Thread(target=_v2_hourly_watcher, daemon=True, name="v2-hourly").start()
threading.Thread(target=_v2_fifteen_min_watcher, daemon=True, name="v2-15m").start()
threading.Thread(target=_v2_daily_watcher, daemon=True, name="v2-daily").start()
# Hedge monitor DISABLED — if entries are correct, hedging is unnecessary
# threading.Thread(target=_v2_monitor_thread, daemon=True, name="v2-monitor").start()
threading.Thread(target=_v2_resolve_loop, daemon=True, name="v2-resolve").start()
threading.Thread(target=_v2_fill_checker, daemon=True, name="v2-fills").start()
threading.Thread(target=_v2_cleanup_loop, daemon=True, name="v2-cleanup").start()

# Sports prediction scanner
threading.Thread(target=_sports_scanner_thread, daemon=True, name="sports-scanner").start()
print("[SPORTS] Scanner thread launched")

print("[V2] All threads launched — engine running")
print("=" * 60)

send_telegram("🚀 <b>CMVNG BOT v2 STARTED</b>\nConfirmation Trading Engine\nMode: PAPER | $50/platform | $3/trade\nWatchers: 1H + 15M + DAILY\nPlatforms: Polymarket + Limitless")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
