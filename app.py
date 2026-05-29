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

_LMTS_TF_DIAG = [0]  # cap Limitless timeframe-detection diagnostics


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

            # Detect timeframe from slug patterns or deadline proximity.
            # Limitless slugs look like: {asset}-up-or-down-{period}-{ts}
            timeframe = None
            if "hourly" in slug_lower or "-1h-" in slug_lower:
                timeframe = "1H"
            elif "daily" in slug_lower or "-1d-" in slug_lower:
                timeframe = "DAILY"
            elif any(k in slug_lower for k in ("-15m", "15-min", "15min", "quarter")):
                timeframe = "15M"
            elif any(k in slug_lower for k in ("-5m", "5-min", "5min")):
                timeframe = "5M"
            elif any(k in slug_lower for k in ("weekly", "-1w-", "-7d-")):
                timeframe = "WEEKLY"
            elif "-on-" in slug_lower or slug_lower.endswith("-on") or "daily" in slug_lower:
                timeframe = "DAILY"

            # Fallback: estimate from time remaining (only if slug gave nothing)
            if not timeframe:
                if mins_left <= 1:
                    continue
                elif mins_left <= 7:
                    timeframe = "5M"
                elif mins_left <= 20:
                    timeframe = "15M"
                elif mins_left <= 75:
                    timeframe = "1H"
                elif mins_left <= 1500:
                    timeframe = "DAILY"
                else:
                    timeframe = "WEEKLY"
                # One-time visibility into real slug patterns we couldn't name
                if _LMTS_TF_DIAG[0] < 10:
                    _LMTS_TF_DIAG[0] += 1
                    print("[LMTS-TF-DIAG] slug='{}' mins_left={:.0f} -> guessed {}".format(
                        slug[:50], mins_left, timeframe))

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

_SB_LMTS_DIAG = [0]  # cap Limitless resolution diagnostics per process


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
                                pass  # not closed yet — let gated fallback decide
                except Exception as e:
                    print("[V2] Poly resolve check error {}: {}".format(slug[:30], e))

            elif platform == "limitless" and slug:
                try:
                    r = req.get("{}/markets/{}".format(LIMITLESS_API, slug), timeout=8)
                    if r.status_code == 200:
                        market = r.json()
                        status = str(market.get("status", "")).lower()
                        is_done = (status in ("resolved", "closed", "settled", "expired", "ended")
                                   or market.get("closed") is True
                                   or market.get("resolved") is True
                                   or market.get("expired") is True
                                   or market.get("winningOutcomeIndex") is not None
                                   or market.get("winningOutcome") not in (None, ""))
                        if is_done:
                            winner = (market.get("winningOutcome") or market.get("winner") or "")
                            widx = market.get("winningOutcomeIndex")
                            if str(winner).lower() in ("yes", "up", "above"):
                                actual = "UP"
                            elif str(winner).lower() in ("no", "down", "below"):
                                actual = "DOWN"
                            elif widx is not None:
                                actual = "UP" if int(widx) == 0 else "DOWN"
                            else:
                                op = market.get("outcomePrices") or market.get("prices")
                                if isinstance(op, list) and len(op) >= 2:
                                    if float(op[0]) > 0.9: actual = "UP"
                                    elif float(op[0]) < 0.1: actual = "DOWN"
                        if not actual and _SB_LMTS_DIAG[0] < 6:
                            _SB_LMTS_DIAG[0] += 1
                            print("[LMTS-RESOLVE-DIAG] {} status={} keys={} winner={} widx={}".format(
                                slug[:40], status, list(market.keys())[:14],
                                market.get("winningOutcome"), market.get("winningOutcomeIndex")))
                        # NOTE: no 'continue' here — if undetermined, the gated
                        # price fallback below resolves genuinely stuck trades.
                except Exception as e:
                    print("[V2] Limitless resolve check error {}: {}".format(slug[:30], e))

            # METHOD 2: Fallback — ONLY for genuinely stuck trades. The platform
            # (Polymarket/Limitless) is authoritative and matches what you see on
            # the market page. We only guess from price if the platform hasn't
            # resolved well past expiry (3x the timeframe) — otherwise we WAIT,
            # so we never post a wrong result before the real one is available.
            if not actual:
                tf_min = {"15M": 15, "1H": 60, "DAILY": 1440}.get(t["timeframe"], 60)
                age_min = 0
                if t.get("fired_at"):
                    try:
                        age_min = (datetime.now(timezone.utc) - fired).total_seconds() / 60
                    except Exception:
                        age_min = 0
                if age_min < tf_min * 3:
                    continue  # give the platform time to resolve correctly
                close_price = _get_binance_price(asset)
                ptb = t.get("ptb")
                if not close_price or not ptb or ptb <= 0:
                    continue
                print("[V2] FALLBACK resolve {} {} {} (platform unresolved {:.0f}min) close={} ptb={}".format(
                    t["timeframe"], asset, t["direction"], age_min, close_price, ptb))
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

                # FILL LOGIC (guaranteed fills so we never miss a fast market):
                #  • first 30s: only fill if the ask drops to our limit (better price)
                #  • after 30s: fill at the live market ask (1:1) — don't miss it
                # Only a sane ask (>= 10c) counts; <10c = stale/dead data.
                if current_ask < 10:
                    continue
                try:
                    age_sec = (datetime.now(timezone.utc) - fired).total_seconds()
                except Exception:
                    age_sec = 999
                GRACE_SEC = 30
                should_fill = (current_ask <= limit) or (age_sec >= GRACE_SEC)
                if should_fill:
                    try:
                        conn2 = get_db()
                        conn2.run("""
                            UPDATE v2_paper_trades SET
                            order_status = 'FILLED', entry_odds = :odds,
                            book_ask = :bask, filled_at = NOW()
                            WHERE id = :tid
                        """, odds=current_ask, bask=current_ask, tid=o["id"])
                        conn2.close()
                        fill_kind = "limit" if current_ask <= limit else "market"
                        print("[V2] FILLED ({}): {} {} {} @ {:.0f}c (limit was {:.0f}c)".format(
                            fill_kind, o["timeframe"], o["asset"], o["direction"], current_ask, limit))

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
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700;900&family=JetBrains+Mono:wght@400;500;700&display=swap');
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: 'DM Sans', sans-serif; color: #0f2417; min-height: 100vh;
  background:
    radial-gradient(1200px 600px at 10% -10%, rgba(74,222,128,0.12), transparent 60%),
    radial-gradient(1000px 500px at 90% 0%, rgba(34,197,94,0.10), transparent 55%),
    linear-gradient(160deg, #eafaf0 0%, #dff5e8 35%, #d2f0de 100%);
}
.container { max-width: 1100px; margin: 0 auto; padding: 22px 18px 60px; }
.header { display: flex; justify-content: space-between; align-items: center; padding: 10px 0 18px; margin-bottom: 18px; }
.header h1 { font-size: 1.6rem; color: #15803d; font-weight: 900; letter-spacing: -0.5px; }
.header .subtitle { font-size: 0.85rem; color: #42795a; font-weight: 500; }
.nav { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 22px; }
.nav a { color: #356148; text-decoration: none; font-size: 0.8rem; font-weight: 700; padding: 8px 14px; border-radius: 999px; transition: all 0.15s; }
.nav a:hover { background: rgba(74,222,128,0.18); }
.nav a.active { background: #15803d; color: #fff; }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 22px; }
.stat-card {
  background: rgba(255,255,255,0.62); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px);
  border: 1px solid rgba(255,255,255,0.8); border-radius: 18px; padding: 16px;
  box-shadow: 0 8px 32px rgba(21,128,61,0.08), inset 0 1px 0 rgba(255,255,255,0.6);
}
.stat-card .label { font-size: 0.7rem; color: #5b8a6e; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 700; }
.stat-card .value { font-size: 1.5rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; margin-top: 4px; color:#0f2417; }
.stat-card .value.green { color: #15803d; }
.stat-card .value.red { color: #dc2626; }
.stat-card .value.blue { color: #0ea5e9; }
.table-wrap { background: rgba(255,255,255,0.62); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); border: 1px solid rgba(255,255,255,0.8); border-radius: 18px; padding: 8px; overflow-x: auto; box-shadow: 0 8px 32px rgba(21,128,61,0.08); }
table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
thead th { color: #5b8a6e; text-transform: uppercase; font-size: 0.66rem; letter-spacing: 0.5px; padding: 10px 8px; text-align: left; border-bottom: 1px solid rgba(21,128,61,0.15); }
tbody td { padding: 10px 8px; border-bottom: 1px solid rgba(21,128,61,0.08); font-family: 'JetBrains Mono', monospace; font-size: 0.76rem; color:#1a3d2a; }
tbody tr:hover { background: rgba(74,222,128,0.10); }
.up { color: #15803d; }
.down { color: #dc2626; }
.win { color: #15803d; font-weight: 700; }
.loss { color: #dc2626; font-weight: 700; }
.pend { color: #ca8a04; }
.hedge-badge { background: rgba(14,165,233,0.15); color: #0ea5e9; padding: 2px 6px; border-radius: 6px; font-size: 0.7rem; }
.conf-high { color: #15803d; }
.conf-med { color: #ca8a04; }
.conf-low { color: #dc2626; }
.note-cell { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: pointer; color: #6b9580; font-size: 0.72rem; }
.note-cell:hover { white-space: normal; color: #0f2417; }
.filter-bar { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
.filter-btn { background: rgba(255,255,255,0.5); border: 1px solid rgba(21,128,61,0.2); color: #42795a; padding: 7px 14px; border-radius: 999px; cursor: pointer; font-size: 0.8rem; font-weight: 600; }
.filter-btn.active { background: #15803d; color: #fff; border-color: #15803d; }
.rtds-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.rtds-dot.on { background: #15803d; box-shadow: 0 0 6px #15803d; }
.rtds-dot.off { background: #dc2626; }
.empty { text-align: center; padding: 40px; color: #5b8a6e; }
@media (max-width:600px){
  .header h1 { font-size: 1.25rem; }
  .nav { gap: 4px; overflow-x: auto; flex-wrap: nowrap; }
  .nav a { padding: 7px 11px; font-size: 0.72rem; white-space: nowrap; }
  .stats-grid { grid-template-columns: repeat(2, 1fr); }
  .container { padding: 18px 12px 50px; }
}
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
    h += '<a href="/app/picks">⚽ Picks</a>'
    h += '<a href="/app/codes">🎫 Codes</a>'
    h += '<a href="/app/results">📈 Results</a>'
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
    h += '<div class="table-wrap"><table><thead><tr>'
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

    h += '</tbody></table></div></div></body></html>'
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
<div class="cta" style="padding-top:0"><a href="/app/picks" style="background:transparent;border:1px solid #1a2e1f;color:#4ade80;margin:0 4px">⚽ Football Picks</a><a href="/app/codes" style="background:transparent;border:1px solid #1a2e1f;color:#4ade80;margin:0 4px">🎫 SportyBet Codes</a></div>
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
        # Build URL — sports match markets use /sports/{sport}/{game_slug}
        # Non-sports events use /event/{slug}
        # The game slug is the slug without trailing market type suffix
        # Sport code is the first segment of the slug (e.g. "ucl", "fif", "epl", "es2")
        market_url = ""
        game_id_val = m.get("gameId", "") or ""
        smt_val = m.get("sportsMarketType", "") or ""
        if slug:
            if game_id_val or smt_val or _sports_re.match(r'^[a-z]{2,5}-[a-z]{2,5}-[a-z]{2,5}-\d{4}', slug):
                # Sports market — extract sport code and game slug
                parts = slug.split("-")
                sport_code = parts[0] if parts else ""
                # Game slug is the date-based portion: {sport}-{team1}-{team2}-{date}
                # Strip trailing market type suffixes
                game_slug = _sports_re.sub(
                    r'-(moneyline|totals?|btts|spread|both-teams-to-score|'
                    r'corners?|total-corners|draw|soccer-halftime-result|'
                    r'will-[a-z-]+win[a-z-]*|o-u-\d+.*|handicap.*)$',
                    '', slug)
                # If stripping didn't change it, use slug up to date portion
                if game_slug == slug:
                    date_m = _sports_re.search(r'(\d{4}-\d{2}-\d{2})', slug)
                    if date_m:
                        end_idx = date_m.end()
                        game_slug = slug[:end_idx]
                market_url = "https://polymarket.com/sports/{}/{}".format(
                    sport_code, game_slug)
            else:
                # Non-sports event
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


def _sports_fetch_limitless_sports(match_pairs=None):
    """Fetch sports markets from Limitless using documented API endpoints.

    Strategy (from docs.limitless.exchange):
    1. GET /markets/active?automationType=sports — returns all sports markets directly
    2. GET /markets/search?query=<team names> — semantic search for specific matches
    3. GET /markets/categories/count — discover what categories exist
    """
    markets = []
    seen_ids = set()

    def _parse_limitless(m):
        """Parse a Limitless market object."""
        mid = str(m.get("id", "") or m.get("address", "") or m.get("slug", ""))
        if not mid or mid in seen_ids:
            return None
        seen_ids.add(mid)
        title = m.get("title", "") or ""
        slug = m.get("slug", "") or ""
        address = m.get("address", "") or ""
        prices = m.get("prices", [])
        # Build URL — Limitless uses /markets/{slug} or /markets/{address}
        market_url = ""
        if slug:
            market_url = "https://limitless.exchange/markets/{}".format(slug)
        elif address:
            market_url = "https://limitless.exchange/markets/{}".format(address)
        return {
            "platform": "limitless",
            "title": title,
            "question": title,
            "slug": slug,
            "market_id": mid,
            "condition_id": m.get("conditionId", ""),
            "outcome_prices": [str(p/100) for p in prices] if prices else [],
            "outcomes": ["Yes", "No"],
            "volume": float(m.get("volumeFormatted", 0) or 0),
            "url": market_url,
            "best_ask": 0,
            "last_price": float(prices[0]/100) if prices else 0,
            "game_id": "",
            "sports_market_type": "",
        }

    # Step 1: Try automationType=sports (may 400 if Limitless doesn't support it yet)
    try:
        r = _sports_req.get("{}/markets/active".format(LIMITLESS_API),
                           params={"automationType": "sports", "page": 1, "limit": 50},
                           timeout=15)
        if r.status_code == 200:
            data = r.json()
            items = data.get("data", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for m in items:
                title_lower = (m.get("title", "") or "").lower()
                if any(kw in title_lower for kw in [
                    "goal", "soccer", "football", " fc", "united",
                    "arsenal", "chelsea", "liverpool", "barcelona",
                    "real madrid", "bayern", "psg", "juventus",
                    "premier league", "la liga", "serie a",
                    "bundesliga", "champions league", "ucl",
                    "europa", "mls", "cup", "corner", "btts",
                    "both teams", "clean sheet", " vs ", " v ",
                ]):
                    parsed = _parse_limitless(m)
                    if parsed:
                        markets.append(parsed)
            if items:
                print("[SPORTS] Limitless automationType=sports: {} soccer markets".format(len(markets)))
        else:
            print("[SPORTS] Limitless automationType=sports — HTTP {} (trying alternatives)".format(r.status_code))
    except Exception as e:
        print("[SPORTS] Limitless sports browse error: {}".format(e))

    # Step 2: Discover categories first, then browse sports category if it exists
    try:
        r = _sports_req.get("{}/markets/categories/count".format(LIMITLESS_API), timeout=10)
        if r.status_code == 200:
            cat_data = r.json()
            cat_counts = cat_data.get("category", {}) if isinstance(cat_data, dict) else {}
            print("[SPORTS] Limitless categories: {}".format(
                ", ".join("{}={}".format(k, v) for k, v in list(cat_counts.items())[:8])))
            # Try each category to find sports-related ones
            for cat_id, count in cat_counts.items():
                if int(count) > 0:
                    try:
                        cr = _sports_req.get("{}/markets/active/{}".format(LIMITLESS_API, cat_id),
                                           params={"page": 1, "limit": 20}, timeout=10)
                        if cr.status_code == 200:
                            cdata = cr.json()
                            citems = cdata.get("data", []) if isinstance(cdata, dict) else []
                            for m in citems[:3]:  # Sample first 3
                                title = (m.get("title", "") or "").lower()
                                tags = " ".join(str(t).lower() for t in (m.get("tags", []) or []))
                                cats = " ".join(str(c).lower() for c in (m.get("categories", []) or []))
                                if any(kw in (title + tags + cats) for kw in [
                                    "soccer", "football", "goal", " fc ", "match",
                                    "premier", "champions", "world cup", "btts", " vs "
                                ]):
                                    # This category has sports — fetch all
                                    print("[SPORTS] Limitless cat={} has sports markets, fetching...".format(cat_id))
                                    for fm in citems:
                                        parsed = _parse_limitless(fm)
                                        if parsed:
                                            markets.append(parsed)
                                    break
                        time.sleep(0.2)
                    except:
                        pass
    except Exception as e:
        print("[SPORTS] Limitless categories error: {}".format(e))

    # Step 3: Semantic search for specific matches
    if match_pairs:
        searched = 0
        for home, away in match_pairs[:10]:
            query = "{} {}".format(home.split()[-1] if home else "", away.split()[-1] if away else "").strip()
            if len(query) < 4:
                query = "{} {}".format(home, away)
            try:
                r = _sports_req.get("{}/markets/search".format(LIMITLESS_API),
                                   params={"query": query, "limit": 5},
                                   timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    items = data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []
                    for m in items:
                        parsed = _parse_limitless(m)
                        if parsed:
                            markets.append(parsed)
                searched += 1
                time.sleep(0.3)
            except:
                searched += 1

    print("[SPORTS] Limitless total: {} sports markets".format(len(markets)))
    for m in markets[:3]:
        print("[SPORTS] Limitless sample: '{}' | {}".format(
            m.get("title", "")[:60], m.get("url", "")))
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
        elif insights["draws"] > n / 2:
            insights["consensus_winner"] = "DRAW"

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
    """Score a potential pick from 0-100.
    Scoring philosophy: a specific score prediction (e.g. PSG 2-1 Arsenal)
    from even ONE site is useful if it aligns with the market type."""
    score = 0
    reasons = []
    n_preds = len(insights["sources"])
    unique_sources = len(set(insights["sources"]))

    # 1. Multi-source consensus bonus (max 30)
    if unique_sources >= 4:
        score += 30
        reasons.append("4+ sites agree")
    elif unique_sources >= 3:
        score += 20
        reasons.append("3+ sites agree")
    elif unique_sources >= 2:
        score += 10
        reasons.append("2 sites agree")

    # 2. Score predictions (max 20) — specific score predictions are high-value signals
    n_scores = len(insights["scores"])
    if n_scores >= 3:
        score += 20
        reasons.append("{} score predictions".format(n_scores))
    elif n_scores >= 1:
        score += 15
        reasons.append("{} score prediction{}".format(n_scores, "s" if n_scores > 1 else ""))

    # 3. Goals alignment with market (max 15)
    mq = market.get("question", "").lower()
    if insights["total_goals_predicted"]:
        avg = sum(insights["total_goals_predicted"]) / len(insights["total_goals_predicted"])
        if avg > 2.5 and ("over" in mq or "o/u" in mq):
            score += 15
            reasons.append("Goals avg {:.1f} (over)".format(avg))
        elif avg <= 2.5 and "under" in mq:
            score += 15
            reasons.append("Goals avg {:.1f} (under)".format(avg))
        elif avg >= 2.0:
            score += 8
            reasons.append("Goals avg {:.1f}".format(avg))

    # 4. Winner/draw consensus matches market (max 15)
    if insights["consensus_winner"] == "DRAW":
        if "draw" in mq or "end in a draw" in mq:
            score += 15
            reasons.append("DRAW consensus")
        # Also boost BTTS if draw predicted with goals
        if insights["total_goals_predicted"]:
            avg_g = sum(insights["total_goals_predicted"]) / len(insights["total_goals_predicted"])
            if avg_g >= 2 and ("both" in mq and "score" in mq):
                score += 10
                reasons.append("Draw {:.0f}-{:.0f} → BTTS likely".format(avg_g/2, avg_g/2))
    elif insights["consensus_winner"]:
        winner_norm = _sports_normalize_team(insights["consensus_winner"])
        winner_words = [w for w in winner_norm.split() if len(w) > 3]
        if any(w in mq for w in winner_words):
            score += 15
            reasons.append("{} predicted winner".format(insights["consensus_winner"]))

    # 5. BTTS consensus (max 10)
    if insights["consensus_btts"] == "YES" and ("both" in mq and "score" in mq):
        score += 10
        reasons.append("BTTS YES consensus")
    elif insights["consensus_btts"] == "NO" and ("both" in mq and "score" in mq):
        score += 5
        reasons.append("BTTS NO consensus")

    # 6. Forebet probability (max 15)
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


# Football v3: cache of recent sports alerts per platform (for /sports menu)
_sports_market_cache = {"polymarket": [], "limitless": []}


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
    lmts_markets = _sports_fetch_limitless_sports(match_pairs=search_pairs[:10])
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

    # Debug: log details for high-profile matches
    for key, md in matches.items():
        if "paris" in key[0] or "arsenal" in key[1] or "psg" in key[0]:
            preds = md["predictions"]
            scores = [p.get("score") for p in preds if p.get("score")]
            types = [p.get("type", "?") for p in preds]
            print("[SPORTS] DEBUG PSG: {} preds, types={}, scores={}, home='{}' away='{}'".format(
                len(preds), types, scores, md["home"], md["away"]))

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

                # Deduplicate — same match + same market type shouldn't alert twice
                # e.g. "Will Switzerland win?" and "Will Jordan win?" are both moneyline for same game
                mslug = market.get("slug", "")
                smt = market.get("sports_market_type", "") or "general"
                # Extract game identifier from slug: typically "league-team1-team2-date"
                # e.g. "fif-che-jor-2026-05-30-will-switzerland-win" → game="2026-05-30"
                date_match = _sports_re.search(r'(\d{4}-\d{2}-\d{2})', mslug)
                game_date = date_match.group(1) if date_match else ""
                # Dedup key: (match_key, market_type, game_date)
                # match_key (home/away) is already the outer loop key
                alert_key = (key, smt, game_date)
                if alert_key in seen_alert_keys:
                    continue
                seen_alert_keys.add(alert_key)

                # Single source without any useful signal is too weak — skip
                if len(sources) == 1:
                    has_score = any(p.get("score") for p in preds)
                    has_consensus = insights.get("consensus_winner") is not None
                    if not has_score and not has_consensus:
                        continue

                # Score this pick
                pick_score, reasons = _sports_score_pick(insights, market)

                # Source count bonus
                if len(sources) >= 2:
                    pick_score += 15
                    reasons.append("{} prediction sites".format(len(sources)))
                elif len(sources) == 1:
                    has_score = any(p.get("score") for p in preds)
                    if has_score:
                        pick_score += 5
                        reasons.append("1 site with score prediction")
                    else:
                        pick_score += 3
                        reasons.append("1 site prediction")

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
                    try:
                        _u = market.get('url', '')
                        _plat = 'limitless' if 'limitless' in _u else 'polymarket'
                        _winner = insights.get('consensus_winner') or ''
                        _pick_txt = market_label
                        _sports_market_cache.setdefault(_plat, [])
                        _sports_market_cache[_plat].insert(0, {
                            'home': home, 'away': away,
                            'winner': _winner,
                            'market': _pick_txt,
                            'question': (mq or '')[:90],
                            'url': _u,
                            'score': pick_score,
                            'odds': odds_str,
                        })
                        # Dedup by match, keep newest 30
                        _seen = set(); _clean = []
                        for _it in _sports_market_cache[_plat]:
                            _k = (_it.get('home'), _it.get('away'), _it.get('market'))
                            if _k in _seen:
                                continue
                            _seen.add(_k); _clean.append(_it)
                        _sports_market_cache[_plat] = _clean[:30]
                    except Exception:
                        pass
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
    def _cards(platform):
        items = []
        try:
            items = list(_sports_market_cache.get(platform, []))
        except Exception:
            items = []
        if not items:
            return ('<div class="glass empty"><div class="big">📊</div>'
                    '<div>No {} picks cached yet. The scanner runs every few hours.</div></div>'.format(
                        platform.title()))
        blocks = []
        for m in items[:20]:
            if not isinstance(m, dict):
                continue
            winner = m.get("winner", "")
            winner_line = ""
            if winner and winner.upper() != "DRAW":
                winner_line = '<div class="pick">🏆 Pick: <b>{}</b></div>'.format(winner)
            elif winner:
                winner_line = '<div class="pick">🏆 Pick: <b>Draw</b></div>'
            market = (m.get("market", "") or "").replace("🏆 ", "").replace("⚽ ", "")
            url = m.get("url", "")
            link = ('<a href="{}" target="_blank">Place bet →</a>'.format(url)) if url else ""
            blocks.append(
                '<div class="glass match-card"><div class="mhead">'
                '<div class="teams">{} vs {}</div>'
                '<div class="league">{}/100</div></div>'
                '{}'
                '<div class="meta">📍 {}</div>'
                '<div class="code-box" style="margin-top:10px">'
                '<div><div class="label">{}</div></div>{}</div></div>'.format(
                    m.get("home", ""), m.get("away", ""), m.get("score", ""),
                    winner_line, market or "Market", platform.title(), link))
        return "".join(blocks)

    poly_cards = _cards("polymarket")
    lmts_cards = _cards("limitless")

    html = """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sports Markets — Cmvng Bot</title><style>{css}</style></head><body>
{nav}<div class="wrap">
<div class="page-head"><h1>Sports Markets</h1>
<div class="sub">Live picks matched to Polymarket & Limitless</div></div>
<h2 style="font-size:1rem;font-weight:900;color:#15803d;margin:6px 4px 12px">📊 Polymarket</h2>
{poly}
<h2 style="font-size:1rem;font-weight:900;color:#15803d;margin:22px 4px 12px">📊 Limitless</h2>
{lmts}
<div class="disclaimer">Picks are cross-validated across prediction sites then matched to live
prediction-market questions. Tap "Place bet" to open the market.</div>
</div></body></html>""".format(css=FB_CSS, nav=_nav("sports"), poly=poly_cards, lmts=lmts_cards)
    return html




# ═══════════════════════════════════════════════════════════════════════════
# CMVNG BOT v3 — FOOTBALL ENGINE (auto-assembled from modules)
# ═══════════════════════════════════════════════════════════════════════════
"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — FOOTBALL ANALYSIS ENGINE
═══════════════════════════════════════════════════════════════════
Pure-logic core: scoring + accumulator building.
No network calls here — fully testable in isolation.

Flow:
  1. analyze_fixture(data) -> scores every market type for one match
  2. build_accumulator(picks, tier) -> packs best picks into an odds tier
═══════════════════════════════════════════════════════════════════
"""

import math


# ═══════════════════════════════════════════════════════════════════
# ODDS ESTIMATION
# Convert a win probability into fair decimal odds, then shade it to
# look like a real bookmaker price (bookmakers add ~5-8% margin).
# ═══════════════════════════════════════════════════════════════════

def prob_to_odds(prob_pct, margin=0.06):
    """Convert probability % to realistic decimal odds with bookmaker margin."""
    p = max(0.01, min(0.99, prob_pct / 100.0))
    fair = 1.0 / p
    # Bookmaker shortens odds (adds margin) -> divide by (1+margin)
    shaded = fair / (1.0 + margin)
    return round(max(1.01, shaded), 2)


# ═══════════════════════════════════════════════════════════════════
# MARKET SCORING
# Each function returns a confidence percentage (0-100) for one market,
# using the 6 criteria the user specified.
# ═══════════════════════════════════════════════════════════════════

def _form_points(form_str):
    """Convert form string 'WWDLW' to avg points per game (0-3)."""
    if not form_str:
        return 1.5
    pts = {"W": 3, "D": 1, "L": 0}
    vals = [pts.get(c.upper(), 1) for c in form_str if c.upper() in pts]
    return sum(vals) / len(vals) if vals else 1.5


def _safe(d, key, default=0.0):
    """Safely pull a numeric value from a dict."""
    v = d.get(key, default)
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def analyze_fixture(fx):
    """
    Score every market type for a single fixture.
    `fx` is a dict with all scraped data (form, xg, stats, h2h, injuries).
    Returns a list of scored picks (each a dict).

    All fields are optional — missing data degrades the relevant market's
    confidence rather than crashing.
    """
    home = fx.get("home_team", "Home")
    away = fx.get("away_team", "Away")
    league = fx.get("league", "")
    kickoff = fx.get("kickoff_time", "")
    match_label = "{} vs {}".format(home, away)

    # ── Pull the inputs (all safe) ──
    home_form_pts = _form_points(fx.get("home_form", ""))
    away_form_pts = _form_points(fx.get("away_form", ""))
    home_xg_for = _safe(fx, "home_xg_for", 1.3)
    home_xg_against = _safe(fx, "home_xg_against", 1.3)
    away_xg_for = _safe(fx, "away_xg_for", 1.3)
    away_xg_against = _safe(fx, "away_xg_against", 1.3)
    home_gf = _safe(fx, "home_goals_scored_avg", home_xg_for)
    home_ga = _safe(fx, "home_goals_conceded_avg", home_xg_against)
    away_gf = _safe(fx, "away_goals_scored_avg", away_xg_for)
    away_ga = _safe(fx, "away_goals_conceded_avg", away_xg_against)
    home_corners_for = _safe(fx, "home_corners_for_avg", 5.0)
    home_corners_against = _safe(fx, "home_corners_against_avg", 5.0)
    away_corners_for = _safe(fx, "away_corners_for_avg", 5.0)
    away_corners_against = _safe(fx, "away_corners_against_avg", 5.0)
    home_cards = _safe(fx, "home_cards_avg", 2.0)
    away_cards = _safe(fx, "away_cards_avg", 2.0)
    home_btts = _safe(fx, "home_btts_pct", 50.0)
    away_btts = _safe(fx, "away_btts_pct", 50.0)
    home_cs = _safe(fx, "home_clean_sheet_pct", 30.0)
    away_cs = _safe(fx, "away_clean_sheet_pct", 30.0)
    home_pos = int(_safe(fx, "home_position", 10))
    away_pos = int(_safe(fx, "away_position", 10))

    # Injuries: count of key players out (passed as int)
    home_inj = int(_safe(fx, "home_key_injuries", 0))
    away_inj = int(_safe(fx, "away_key_injuries", 0))

    # ── CRITERION 1+2: Relative strength (form + home advantage + table) ──
    HOME_ADV = 0.35  # home advantage boost in "strength points"
    home_strength = home_form_pts + HOME_ADV
    away_strength = away_form_pts

    # Table context adjustment: higher team (lower position number) gets boost
    pos_gap = away_pos - home_pos  # positive = home ranked higher
    home_strength += pos_gap * 0.05
    away_strength -= pos_gap * 0.05

    # Injury penalty
    home_strength -= home_inj * 0.20
    away_strength -= away_inj * 0.20

    total_strength = home_strength + away_strength
    if total_strength <= 0:
        total_strength = 1.0
    home_win_raw = home_strength / total_strength
    away_win_raw = away_strength / total_strength

    # ── Expected goals for THIS match (blend attack vs defense) ──
    exp_home_goals = (home_gf + away_ga) / 2.0
    exp_away_goals = (away_gf + home_ga) / 2.0
    exp_total_goals = exp_home_goals + exp_away_goals

    picks = []

    def add(market_type, pick_label, confidence, reasoning):
        confidence = max(1.0, min(99.0, confidence))
        picks.append({
            "match": match_label,
            "home": home,
            "away": away,
            "league": league,
            "kickoff": kickoff,
            "market_type": market_type,
            "pick": pick_label,
            "confidence": round(confidence, 1),
            "odds": prob_to_odds(confidence),
            "reasoning": reasoning,
            "kickoff_ts": fx.get("kickoff_ts", 0),
            # SportyBet IDs (event id pre-resolved during enrichment)
            "sb_event_id": fx.get("sb_event_id", ""),
            "sb_market_id": "",
            "sb_specifier": None,
            "sb_outcome_id": "",
            "result": "pending",
        })

    # ── MATCH RESULT ──
    # If we have prediction-site probabilities (Forebet etc.), use them directly —
    # they're a stronger signal than our form heuristic. Recompute raw shares too,
    # so double-chance and combos downstream stay consistent.
    pred_ph = fx.get("pred_prob_home")
    pred_pd = fx.get("pred_prob_draw")
    pred_pa = fx.get("pred_prob_away")
    if pred_ph is not None:
        ph = max(1.0, float(pred_ph))
        pd = max(1.0, float(pred_pd)) if pred_pd is not None else 26.0
        pa = max(1.0, float(pred_pa)) if pred_pa is not None else max(1.0, 100 - ph - pd)
        tot_p = ph + pd + pa
        home_win_raw = ph / tot_p
        away_win_raw = pa / tot_p
        home_win_conf = ph
        away_win_conf = pa
        draw_conf = pd
        _win_reason = "prediction sites: {:.0f}% / {:.0f}% / {:.0f}%".format(ph, pd, pa)
    else:
        home_win_conf = home_win_raw * 100 * 0.85  # temper raw probability
        away_win_conf = away_win_raw * 100 * 0.80
        draw_conf = (1 - abs(home_win_raw - away_win_raw)) * 35  # draws ~25-32% typically
        _win_reason = "form {:.1f} vs {:.1f}, table {} vs {}".format(
            home_form_pts, away_form_pts, home_pos, away_pos)

    add("home_win", "{} to Win".format(home), home_win_conf,
        "{}: {}".format(home, _win_reason))
    add("away_win", "{} to Win".format(away), away_win_conf,
        "{}: {}".format(away, _win_reason))
    add("draw", "Draw", draw_conf,
        "Draw probability: {:.0f}%".format(draw_conf))

    # ── DOUBLE CHANCE (much safer than straight win) ──
    dc_1x = (home_win_raw + (draw_conf/100)) * 100 * 0.92
    dc_x2 = (away_win_raw + (draw_conf/100)) * 100 * 0.92
    add("double_chance_1X", "{} or Draw".format(home), dc_1x,
        "{} home + draw cover, form {:.1f}pts".format(home, home_form_pts))
    add("double_chance_X2", "{} or Draw".format(away), dc_x2,
        "{} + draw cover".format(away))

    # ── OVER/UNDER GOALS ──
    # Poisson-ish heuristic from expected total goals
    over_05 = min(98, 70 + exp_total_goals * 10)
    over_15 = min(96, 45 + exp_total_goals * 13)
    over_25 = min(90, 20 + exp_total_goals * 16)
    over_35 = min(80, exp_total_goals * 15)
    under_25 = 100 - over_25
    under_35 = 100 - over_35

    add("over_0.5", "Over 0.5 Goals", over_05,
        "Expected {:.1f} total goals".format(exp_total_goals))
    add("over_1.5", "Over 1.5 Goals", over_15,
        "Expected {:.1f} goals ({} {:.1f}xG, {} {:.1f}xG)".format(
            exp_total_goals, home, home_xg_for, away, away_xg_for))
    add("over_2.5", "Over 2.5 Goals", over_25,
        "Expected {:.1f} goals, both attacks active".format(exp_total_goals))
    add("over_3.5", "Over 3.5 Goals", over_35,
        "High-scoring projection {:.1f}".format(exp_total_goals))
    add("under_2.5", "Under 2.5 Goals", under_25,
        "Lower-scoring projection {:.1f}".format(exp_total_goals))
    add("under_3.5", "Under 3.5 Goals", under_35,
        "Defensive projection {:.1f}".format(exp_total_goals))

    # ── BTTS ──
    btts_yes = (home_btts + away_btts) / 2.0
    # Adjust by clean sheet tendency
    btts_yes -= (home_cs + away_cs) / 4.0
    btts_yes = max(15, min(88, btts_yes + (exp_total_goals - 2.5) * 8))
    btts_no = 100 - btts_yes
    add("btts_yes", "Both Teams to Score - Yes", btts_yes,
        "{} BTTS {:.0f}%, {} BTTS {:.0f}%, exp {:.1f} goals".format(
            home, home_btts, away, away_btts, exp_total_goals))
    add("btts_no", "Both Teams to Score - No", btts_no,
        "Clean sheet tendency: {} {:.0f}%, {} {:.0f}%".format(home, home_cs, away, away_cs))

    # ── CORNERS ──
    exp_corners = (home_corners_for + away_corners_against) / 2.0 + \
                  (away_corners_for + home_corners_against) / 2.0
    over_75c = min(92, exp_corners * 8)
    over_85c = min(85, exp_corners * 7)
    over_95c = min(75, exp_corners * 6)
    add("corners_over_7.5", "Over 7.5 Corners", over_75c,
        "Expected {:.1f} corners ({} {:.1f}, {} {:.1f})".format(
            exp_corners, home, home_corners_for, away, away_corners_for))
    add("corners_over_8.5", "Over 8.5 Corners", over_85c,
        "Expected {:.1f} corners".format(exp_corners))
    add("corners_over_9.5", "Over 9.5 Corners", over_95c,
        "Expected {:.1f} corners, both teams attack wide".format(exp_corners))

    # ── CARDS ──
    exp_cards = home_cards + away_cards
    over_25cards = min(88, exp_cards * 22)
    over_35cards = min(75, exp_cards * 17)
    add("cards_over_2.5", "Over 2.5 Cards", over_25cards,
        "Expected {:.1f} cards combined".format(exp_cards))
    add("cards_over_3.5", "Over 3.5 Cards", over_35cards,
        "Expected {:.1f} cards, physical matchup".format(exp_cards))

    # ── COMBOS ──
    home_win_btts = (home_win_conf/100) * (btts_yes/100) * 100 * 1.05
    add("home_win_btts", "{} Win & BTTS".format(home), home_win_btts,
        "{} favored + both score".format(home))
    home_win_over25 = (home_win_conf/100) * (over_25/100) * 100 * 1.05
    add("home_win_over_2.5", "{} Win & Over 2.5".format(home), home_win_over25,
        "{} win in high-scoring game".format(home))
    wd_over15 = (dc_1x/100) * (over_15/100) * 100
    add("dc_over_1.5", "{} or Draw & Over 1.5".format(home), wd_over15,
        "Safe double chance + goals")

    # ── HANDICAP ──
    if home_win_raw > 0.55:
        hcp = home_win_conf * 0.65
        add("handicap_home_-1.5", "{} -1.5".format(home), hcp,
            "{} strongly favored to win by 2+".format(home))
    if away_win_raw > 0.55:
        hcp = away_win_conf * 0.65
        add("handicap_away_-1.5", "{} -1.5".format(away), hcp,
            "{} strongly favored to win by 2+".format(away))

    # ── CORRECT SCORE (top likely scorelines) ──
    h = max(0, round(exp_home_goals))
    a = max(0, round(exp_away_goals))
    cs_conf = 12 + (10 if home_win_raw > 0.5 else 5)  # correct scores are low prob
    add("correct_score", "{} {}-{} {}".format(home, h, a, away), cs_conf,
        "Most likely scoreline from xG ({:.1f}-{:.1f})".format(exp_home_goals, exp_away_goals))

    return picks


# ═══════════════════════════════════════════════════════════════════
# ACCUMULATOR BUILDER
# Pack the best picks into each odds tier.
# ═══════════════════════════════════════════════════════════════════

# Tier configuration: per-selection odds band + packing rules
TIER_CONFIG = {
    "2_odds": {
        "target": 2.0, "min_conf": 78, "min_sel": 4, "max_sel": 8,
        "odds_lo": 1.04, "odds_hi": 1.32,
        "prefer": ["over_1.5", "double_chance_1X", "double_chance_X2", "btts_yes",
                   "over_0.5", "under_3.5", "btts_no"],
        "label": "2 ODDS — BANKER", "emoji": "🟢",
    },
    "3_odds": {
        "target": 3.0, "min_conf": 70, "min_sel": 4, "max_sel": 7,
        "odds_lo": 1.12, "odds_hi": 1.48,
        "prefer": ["double_chance_1X", "over_1.5", "home_win", "btts_yes",
                   "over_2.5", "double_chance_X2", "corners_over_7.5", "under_2.5"],
        "label": "3 ODDS — SAFE", "emoji": "🟢",
    },
    "5_odds": {
        "target": 5.0, "min_conf": 58, "min_sel": 3, "max_sel": 6,
        "odds_lo": 1.22, "odds_hi": 1.80,
        "prefer": ["home_win", "over_2.5", "btts_yes", "corners_over_8.5",
                   "dc_over_1.5", "cards_over_2.5", "away_win", "home_win_btts"],
        "label": "5 ODDS — VALUE", "emoji": "🟡",
    },
    "10_odds": {
        "target": 10.0, "min_conf": 48, "min_sel": 4, "max_sel": 6,
        "odds_lo": 1.38, "odds_hi": 2.40,
        "prefer": ["home_win", "away_win", "home_win_btts", "corners_over_9.5",
                   "handicap_home_-1.5", "cards_over_3.5", "over_2.5",
                   "home_win_over_2.5", "over_3.5"],
        "label": "10 ODDS — RISK", "emoji": "🟠",
    },
    "1000_odds": {
        "target": 1000.0, "min_conf": 15, "min_sel": 8, "max_sel": 16,
        "odds_lo": 1.45, "odds_hi": 15.0,
        "prefer": ["correct_score", "home_win_btts", "home_win_over_2.5",
                   "handicap_home_-1.5", "handicap_away_-1.5", "over_3.5",
                   "cards_over_3.5", "away_win", "corners_over_9.5"],
        "label": "1000+ ODDS — MOONSHOT", "emoji": "🔴",
    },
}


def build_accumulator(all_picks, tier_key):
    """
    Build one accumulator tier with a DIVERSE mix of market types.
    Strategy:
      1. Filter picks to this tier's confidence floor + odds band
      2. Greedily pack (max 1 per match) but cap how many of each market
         type can appear, so a slip is a genuine mix (not all "win or draw"
         or all "over 8.5 corners")
      3. Stop when total odds reaches the tier target
    Returns dict {selections, total_odds, ...} or None if not buildable.
    """
    cfg = TIER_CONFIG[tier_key]
    target = cfg["target"]
    prefer_set = set(cfg["prefer"])

    eligible = [
        p for p in all_picks
        if p["confidence"] >= cfg["min_conf"]
        and cfg["odds_lo"] <= p["odds"] <= cfg["odds_hi"]
    ]
    if not eligible:
        return None

    # How many distinct market types are available? Use it to set a diversity cap.
    distinct_types = len(set(p["market_type"] for p in eligible))
    # Allow at most ~ceil(max_sel / distinct_types)+1 of any one type, min 1.
    import math as _m
    max_per_type = max(1, _m.ceil(cfg["max_sel"] / max(1, distinct_types)) + 1)
    if distinct_types >= cfg["max_sel"]:
        max_per_type = 1  # plenty of variety -> force every leg a different type

    def base_rank(p):
        # Preferred types first, then by confidence
        return (0 if p["market_type"] in prefer_set else 1, -p["confidence"])

    remaining = sorted(eligible, key=base_rank)

    slip = []
    used_matches = set()
    type_count = {}
    running = 1.0

    def try_pack(per_type_cap):
        nonlocal running
        # Greedy with diversity: each step, pick the highest-ranked candidate
        # whose market type is still under the cap and that doesn't overshoot.
        progressed = True
        while progressed and len(slip) < cfg["max_sel"]:
            progressed = False
            for p in remaining:
                if p in slip or p["match"] in used_matches:
                    continue
                if type_count.get(p["market_type"], 0) >= per_type_cap:
                    continue
                new_running = running * p["odds"]
                if new_running > target * 1.18 and len(slip) >= cfg["min_sel"]:
                    continue
                slip.append(p)
                used_matches.add(p["match"])
                type_count[p["market_type"]] = type_count.get(p["market_type"], 0) + 1
                running = new_running
                progressed = True
                if running >= target * 0.92 and len(slip) >= cfg["min_sel"]:
                    return True
                break  # restart scan from the top for best-ranked next pick
        return running >= target * 0.92 and len(slip) >= cfg["min_sel"]

    # First pass: strict diversity cap
    done = try_pack(max_per_type)
    # If we couldn't reach the target band, relax the cap and keep packing
    if not done and len(slip) < cfg["min_sel"]:
        try_pack(max_per_type + 2)
    if not done and len(slip) < cfg["min_sel"]:
        try_pack(cfg["max_sel"])  # last resort: allow repeats to hit min legs

    if len(slip) < cfg["min_sel"]:
        return None

    return {
        "tier": tier_key,
        "label": cfg["label"],
        "emoji": cfg["emoji"],
        "target_odds": target,
        "total_odds": round(running, 2),
        "num_selections": len(slip),
        "selections": slip,
    }


def build_all_accumulators(all_picks):
    """Build all 5 tiers. Returns dict of {tier_key: accumulator or None}."""
    result = {}
    for tier_key in ["2_odds", "3_odds", "5_odds", "10_odds", "1000_odds"]:
        result[tier_key] = build_accumulator(all_picks, tier_key)
    return result


def top_picks_per_match(all_picks, n=3):
    """Group picks by match, return top N per match by confidence."""
    by_match = {}
    for p in all_picks:
        by_match.setdefault(p["match"], []).append(p)
    out = {}
    for match, picks in by_match.items():
        picks.sort(key=lambda x: x["confidence"], reverse=True)
        out[match] = picks[:n]
    return out


"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — FOOTBALL DATA SCRAPERS
═══════════════════════════════════════════════════════════════════
Scrapes Sofascore (fixtures/form/h2h/injuries/standings),
Understat (xG), and FootyStats (corners/cards/btts).

EVERY function is wrapped so a failure returns safe defaults instead
of crashing. The engine fills gaps with league-average assumptions.

NOTE: These endpoints cannot be tested from the build sandbox
(network restricted). They are written from documented API shapes
and need one round of Railway validation. Failures degrade gracefully.
═══════════════════════════════════════════════════════════════════
"""

import time
import json
import datetime as _dt

try:
    import requests as _req
except ImportError:
    _req = None

try:
    from bs4 import BeautifulSoup as _BS
except ImportError:
    _BS = None

# Browser-like headers to avoid trivial blocks
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

SOFA = "https://api.sofascore.com/api/v1"


def _get_json(url, timeout=12, retries=2):
    """GET a URL and parse JSON. Returns None on any failure."""
    if _req is None:
        return None
    for attempt in range(retries):
        try:
            r = _req.get(url, headers=_HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(3)  # rate limited, back off
        except Exception:
            pass
        time.sleep(1)
    return None


def _get_html(url, timeout=12):
    """GET a URL and return text. Returns None on any failure."""
    if _req is None:
        return None
    try:
        r = _req.get(url, headers=_HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════
# SOFASCORE — fixtures, form, H2H, injuries, standings
# ═══════════════════════════════════════════════════════════════════

# Soccer leagues we care about (Sofascore uniqueTournament IDs)
# These are stable IDs from Sofascore.
SOFA_LEAGUES = {
    "EPL": 17, "La Liga": 8, "Bundesliga": 35, "Serie A": 23,
    "Ligue 1": 34, "Eredivisie": 37, "Primeira Liga": 238,
    "Champions League": 7, "Europa League": 679, "Championship": 18,
}


def sofa_todays_fixtures(date_str=None, max_leagues=None):
    """
    Get today's football fixtures from Sofascore.
    Returns list of fixture dicts with event_id, teams, league, kickoff.
    """
    if date_str is None:
        date_str = _dt.date.today().isoformat()

    fixtures = []
    data = _get_json("{}/sport/football/scheduled-events/{}".format(SOFA, date_str))
    if not data or "events" not in data:
        return fixtures

    wanted_league_ids = set(SOFA_LEAGUES.values())
    for ev in data.get("events", []):
        try:
            tournament = ev.get("tournament", {})
            unique_t = tournament.get("uniqueTournament", {}) or {}
            league_id = unique_t.get("id")
            league_name = unique_t.get("name", tournament.get("name", ""))

            # Only keep leagues we track (or all if not restricting)
            if wanted_league_ids and league_id not in wanted_league_ids:
                continue

            status = ev.get("status", {}).get("type", "")
            if status not in ("notstarted", "inprogress"):
                continue  # skip finished games

            home = ev.get("homeTeam", {})
            away = ev.get("awayTeam", {})
            start_ts = ev.get("startTimestamp", 0)

            fixtures.append({
                "event_id": str(ev.get("id", "")),
                "home_team": home.get("name", ""),
                "away_team": away.get("name", ""),
                "home_id": home.get("id"),
                "away_id": away.get("id"),
                "league": league_name,
                "league_id": league_id,
                "season_id": unique_t.get("id"),  # resolved later
                "kickoff_time": _dt.datetime.fromtimestamp(start_ts).isoformat() if start_ts else "",
                "kickoff_ts": start_ts,
            })
        except Exception:
            continue

    return fixtures


def sofa_team_form(team_id, limit=5):
    """Get last N results for a team as a form string like 'WWDLW'."""
    if not team_id:
        return ""
    data = _get_json("{}/team/{}/events/last/0".format(SOFA, team_id))
    if not data or "events" not in data:
        return ""
    events = data.get("events", [])[-limit:]
    form = []
    for ev in reversed(events):  # most recent first
        try:
            home_id = ev.get("homeTeam", {}).get("id")
            hs = ev.get("homeScore", {}).get("current")
            as_ = ev.get("awayScore", {}).get("current")
            if hs is None or as_ is None:
                continue
            is_home = (home_id == team_id)
            my_score = hs if is_home else as_
            opp_score = as_ if is_home else hs
            if my_score > opp_score:
                form.append("W")
            elif my_score < opp_score:
                form.append("L")
            else:
                form.append("D")
        except Exception:
            continue
    return "".join(form)


def sofa_h2h(event_id):
    """Get head-to-head summary for a match. Returns dict with last meetings."""
    if not event_id:
        return {}
    data = _get_json("{}/event/{}/h2h".format(SOFA, event_id))
    if not data:
        return {}
    return data.get("teamDuel", {}) or {}


def sofa_injuries(team_id):
    """Get count of injured/suspended players for a team."""
    if not team_id:
        return 0
    data = _get_json("{}/team/{}/player/injuries".format(SOFA, team_id))
    if not data:
        return 0
    injuries = data.get("playerInjuries", data.get("injuries", []))
    if isinstance(injuries, list):
        return len(injuries)
    return 0


def sofa_match_stats_summary(event_id):
    """Get corners/cards from a finished match (for averages). Used in aggregation."""
    if not event_id:
        return {}
    data = _get_json("{}/event/{}/statistics".format(SOFA, event_id))
    if not data:
        return {}
    out = {}
    try:
        for period in data.get("statistics", []):
            if period.get("period") != "ALL":
                continue
            for group in period.get("groups", []):
                for item in group.get("statisticsItems", []):
                    name = item.get("name", "").lower()
                    if "corner" in name:
                        out["home_corners"] = _to_num(item.get("home"))
                        out["away_corners"] = _to_num(item.get("away"))
                    if "yellow" in name:
                        out["home_cards"] = _to_num(item.get("home"))
                        out["away_cards"] = _to_num(item.get("away"))
    except Exception:
        pass
    return out


def _to_num(v):
    try:
        return float(str(v).split()[0])
    except (ValueError, TypeError, IndexError, AttributeError):
        return 0.0


# ═══════════════════════════════════════════════════════════════════
# UNDERSTAT — xG data (JSON embedded in <script> tags)
# ═══════════════════════════════════════════════════════════════════

UNDERSTAT_LEAGUES = {
    "EPL": "EPL", "La Liga": "La_liga", "Bundesliga": "Bundesliga",
    "Serie A": "Serie_A", "Ligue 1": "Ligue_1",
}


def understat_team_xg(league_name, season="2025"):
    """
    Scrape Understat for team xG data.
    Returns dict: {team_name: {xg_for, xg_against, played}}
    Understat embeds data as JSON.parse('...') inside <script> tags.
    """
    out = {}
    us_league = UNDERSTAT_LEAGUES.get(league_name)
    if not us_league:
        return out

    html = _get_html("https://understat.com/league/{}/{}".format(us_league, season))
    if not html:
        return out

    try:
        # Find the teamsData script — format: var teamsData = JSON.parse('...')
        import re
        m = re.search(r"teamsData\s*=\s*JSON\.parse\('([^']+)'\)", html)
        if not m:
            return out
        # Decode the hex-escaped JSON
        raw = m.group(1).encode().decode("unicode_escape")
        teams_data = json.loads(raw)

        for team_id, tdata in teams_data.items():
            name = tdata.get("title", "")
            history = tdata.get("history", [])
            if not history:
                continue
            xg_for = sum(_to_num(h.get("xG")) for h in history)
            xg_against = sum(_to_num(h.get("xGA")) for h in history)
            played = len(history)
            if played > 0:
                out[name] = {
                    "xg_for": round(xg_for / played, 2),
                    "xg_against": round(xg_against / played, 2),
                    "played": played,
                }
    except Exception:
        pass

    return out


# ═══════════════════════════════════════════════════════════════════
# FOOTYSTATS — corners, cards, BTTS (HTML tables)
# ═══════════════════════════════════════════════════════════════════

def footystats_team(team_slug):
    """
    Scrape FootyStats team page for corners/cards/btts stats.
    Returns dict of stats or empty dict on failure.
    NOTE: FootyStats slugs are unpredictable; this is best-effort.
    """
    out = {}
    if _BS is None:
        return out
    html = _get_html("https://footystats.org/clubs/{}".format(team_slug))
    if not html:
        return out
    try:
        soup = _BS(html, "html.parser")
        text = soup.get_text().lower()
        # Best-effort extraction — FootyStats layout varies
        # This is a placeholder structure; refined after Railway inspection
        import re
        btts_m = re.search(r"btts[^\d]*(\d+)%", text)
        if btts_m:
            out["btts_pct"] = float(btts_m.group(1))
    except Exception:
        pass
    return out


# ═══════════════════════════════════════════════════════════════════
# AGGREGATOR — combine all sources into one fixture record
# ═══════════════════════════════════════════════════════════════════

def build_fixture_dataset(date_str=None, rate_limit=1.5, max_fixtures=30):
    """
    Master function: scrape everything and return enriched fixture dicts
    ready for the analysis engine.

    Rate-limited to be respectful to Sofascore (Cloudflare).
    """
    fixtures = sofa_todays_fixtures(date_str)
    if not fixtures:
        print("[FB] No fixtures found for {}".format(date_str or "today"))
        return []

    fixtures = fixtures[:max_fixtures]
    print("[FB] Found {} fixtures, enriching...".format(len(fixtures)))

    # Pre-fetch Understat xG per league (one call per league)
    xg_cache = {}
    leagues_present = set(f["league"] for f in fixtures)
    for lg in leagues_present:
        if lg in UNDERSTAT_LEAGUES:
            xg_cache[lg] = understat_team_xg(lg)
            time.sleep(rate_limit)

    enriched = []
    for fx in fixtures:
        try:
            # Form (2 calls)
            fx["home_form"] = sofa_team_form(fx.get("home_id"))
            time.sleep(rate_limit)
            fx["away_form"] = sofa_team_form(fx.get("away_id"))
            time.sleep(rate_limit)

            # Injuries (2 calls)
            fx["home_key_injuries"] = sofa_injuries(fx.get("home_id"))
            fx["away_key_injuries"] = sofa_injuries(fx.get("away_id"))
            time.sleep(rate_limit)

            # xG from Understat cache (name matching)
            lg_xg = xg_cache.get(fx["league"], {})
            home_xg = _match_team_xg(lg_xg, fx["home_team"])
            away_xg = _match_team_xg(lg_xg, fx["away_team"])
            if home_xg:
                fx["home_xg_for"] = home_xg["xg_for"]
                fx["home_xg_against"] = home_xg["xg_against"]
            if away_xg:
                fx["away_xg_for"] = away_xg["xg_for"]
                fx["away_xg_against"] = away_xg["xg_against"]

            enriched.append(fx)
        except Exception as e:
            print("[FB] enrich error for {}: {}".format(fx.get("home_team"), e))
            enriched.append(fx)  # keep it with whatever data we have

    print("[FB] Enriched {} fixtures".format(len(enriched)))
    return enriched


def _match_team_xg(xg_dict, team_name):
    """Fuzzy-match a team name to Understat data (names differ slightly)."""
    if not xg_dict or not team_name:
        return None
    # Exact
    if team_name in xg_dict:
        return xg_dict[team_name]
    # Partial — match on last word or substring
    tn_lower = team_name.lower()
    for name, data in xg_dict.items():
        nl = name.lower()
        if nl in tn_lower or tn_lower in nl:
            return data
        # Match on significant word overlap
        tn_words = set(tn_lower.replace("fc", "").replace("afc", "").split())
        n_words = set(nl.replace("fc", "").replace("afc", "").split())
        if tn_words & n_words:
            return data
    return None


"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — SPORTYBET BOOKING CODE GENERATOR
═══════════════════════════════════════════════════════════════════
Confirmed endpoints (from sacsbrainz/betconverter source):

  READ a code:
    GET https://www.sportybet.com/api/ng/orders/share/{CODE}
    -> {message:"success", data:{outcomes:[{eventId, markets:[{id, specifier, outcomes:[{id}]}]}]}}

  CREATE a code:
    POST https://www.sportybet.com/api/ng/orders/share
    body: {"selections":[{eventId, marketId, specifier, outcomeId}, ...]}
    -> {message:"success", data:{code:"A7K2M9"}}

To map analysis picks -> SportyBet IDs, we:
  1. Search SportyBet fixtures for the match (by team name)
  2. Pull that event's markets
  3. Match our pick to the right market+outcome by description
  4. Build selections and POST

NOTE: cannot be tested from build sandbox (network restricted).
Written from confirmed endpoint shapes. Needs Railway validation.
Every step degrades gracefully — an unmappable pick is skipped, not fatal.
═══════════════════════════════════════════════════════════════════
"""

import time
import json

try:
    import requests as _req
except ImportError:
    _req = None

SB_BASE = "https://www.sportybet.com/api/ng"

_SB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Referer": "https://www.sportybet.com/ng/sport/football",
}


_SB_DIAG_COUNT = [0]
_SB_DIAG_MAX = 14


def _sb_get(url, timeout=12, diag=False):
    if _req is None:
        return None
    try:
        r = _req.get(url, headers=_SB_HEADERS, timeout=timeout)
        if diag and _SB_DIAG_COUNT[0] < _SB_DIAG_MAX:
            _SB_DIAG_COUNT[0] += 1
            body = (r.text or "")[:280].replace("\n", " ").replace("\r", "")
            print("[SB-DIAG] GET {} -> HTTP {} | {}".format(url[:95], r.status_code, body))
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return None
    except Exception as e:
        if diag and _SB_DIAG_COUNT[0] < _SB_DIAG_MAX:
            _SB_DIAG_COUNT[0] += 1
            print("[SB-DIAG] GET {} -> EXCEPTION {}".format(url[:95], e))
    return None


def _sb_post(url, payload, timeout=15, diag=False):
    if _req is None:
        return None
    try:
        r = _req.post(url, headers=_SB_HEADERS, json=payload, timeout=timeout)
        if diag:
            body = (r.text or "")[:300].replace("\n", " ").replace("\r", "")
            print("[SB-DIAG] POST {} -> HTTP {} | payload={} | resp={}".format(
                url[:60], r.status_code, json.dumps(payload)[:200], body))
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        if diag:
            print("[SB-DIAG] POST {} -> EXCEPTION {}".format(url[:60], e))
    return None


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Find SportyBet event for a fixture
# ═══════════════════════════════════════════════════════════════════

_SB_MARKET_CACHE = {}  # eventId -> markets (captured from search results)
_SB_STRUCT_LOGGED = [False]
_SB_EVENT_INFO = {}     # eventId -> {kickoff_ts, match_status, scores, teams}


def sb_search_event(home_team, away_team):
    """
    Search SportyBet for a match by team name. Returns eventId or None.
    Uses the confirmed football.com/SportyBet shared backend path
    'factsCenter/event/firstSearch'. Captures inline markets when present.
    """
    import urllib.parse as _up
    ts = int(time.time() * 1000)
    kwq = _up.quote(home_team.strip())
    candidates = [
        "{}/factsCenter/event/firstSearch?keyword={}&offset=0&pageSize=20&_t={}".format(SB_BASE, kwq, ts),
        "{}/factsCenter/query/frontend/search/event/page/v2?keyword={}&sportId=sr:sport:1&_t={}".format(SB_BASE, kwq, ts),
    ]
    for url in candidates:
        data = _sb_get(url, diag=True)
        if not data:
            continue
        try:
            events = _extract_events_from_search(data)
            for ev in events:
                ev_home = (ev.get("homeTeamName") or ev.get("home")
                           or ev.get("homeTeam") or "").lower()
                ev_away = (ev.get("awayTeamName") or ev.get("away")
                           or ev.get("awayTeam") or "").lower()
                if _team_match(home_team, ev_home) and _team_match(away_team, ev_away):
                    eid = ev.get("eventId") or ev.get("id")
                    # Capture kickoff time + status for display and settlement
                    try:
                        _SB_EVENT_INFO[eid] = {
                            "kickoff_ts": ev.get("estimateStartTime") or ev.get("startTime") or 0,
                            "match_status": ev.get("matchStatus") or "",
                            "status": ev.get("status"),
                            "home_score": ev.get("homeScore") or ev.get("setScore"),
                            "away_score": ev.get("awayScore"),
                            "home": ev.get("homeTeamName"), "away": ev.get("awayTeamName"),
                        }
                    except Exception:
                        pass
                    # Capture inline markets so we don't need a second call
                    mkts = ev.get("markets") or ev.get("marketList") or []
                    if mkts:
                        _SB_MARKET_CACHE[eid] = mkts
                        # One-time dump of UNIQUE market types (to map the
                        # ones still missing: double chance, corners, cards)
                        if not _SB_STRUCT_LOGGED[0]:
                            _SB_STRUCT_LOGGED[0] = True
                            seen_desc = set()
                            for mk in mkts:
                                desc = (mk.get("desc") or mk.get("name") or mk.get("marketName") or "")
                                if desc in seen_desc:
                                    continue
                                seen_desc.add(desc)
                                ocs = mk.get("outcomes") or mk.get("outcome") or []
                                oc_str = " ; ".join("{}={}".format(
                                    o.get("id"), (o.get("desc") or o.get("name") or "")) for o in ocs[:4])
                                print("[SB-STRUCT] id={} desc='{}' spec='{}' | {}".format(
                                    mk.get("id"), desc, mk.get("specifier", ""), oc_str))
                    print("[SB] matched {} vs {} -> {} ({} inline markets)".format(
                        home_team, away_team, eid, len(mkts)))
                    return eid
            if events:
                break  # endpoint works, team just not listed
        except Exception as e:
            print("[SB] search parse error: {}".format(e))
    return None


def _extract_events_from_search(data):
    """Recursively pull event dicts (with eventId + team names) from any shape."""
    found = []

    def walk(obj, depth=0):
        if depth > 6:
            return
        if isinstance(obj, dict):
            # An event-like dict has an id and team names
            if (obj.get("eventId") or obj.get("id")) and \
               (obj.get("homeTeamName") or obj.get("home") or obj.get("homeTeam")):
                found.append(obj)
            for v in obj.values():
                walk(v, depth + 1)
        elif isinstance(obj, list):
            for it in obj:
                walk(it, depth + 1)

    walk(data.get("data", data))
    return found


def _team_match(name, candidate):
    """Fuzzy team name match."""
    if not name or not candidate:
        return False
    n = name.lower().replace("fc", "").replace("afc", "").strip()
    c = candidate.lower().replace("fc", "").replace("afc", "").strip()
    if n in c or c in n:
        return True
    n_words = set(n.split())
    c_words = set(c.split())
    return bool(n_words & c_words)


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Get markets for an event, match our pick
# ═══════════════════════════════════════════════════════════════════

def sb_get_event_markets(event_id):
    """Return markets for a SportyBet event — inline-cached first, else fetch."""
    if not event_id:
        return []
    # Markets captured inline during search?
    if event_id in _SB_MARKET_CACHE:
        return _SB_MARKET_CACHE[event_id]
    ts = int(time.time() * 1000)
    candidates = [
        "{}/factsCenter/event?eventId={}&productId=3&_t={}".format(SB_BASE, event_id, ts),
        "{}/factsCenter/query/frontend/match/detail?eventId={}&_t={}".format(SB_BASE, event_id, ts),
        "{}/factsCenter/wapEvent?eventId={}&_t={}".format(SB_BASE, event_id, ts),
    ]
    for url in candidates:
        data = _sb_get(url, diag=True)
        if not data:
            continue
        try:
            d = data.get("data", data)
            markets = []
            if isinstance(d, dict):
                markets = d.get("markets") or d.get("marketList") or []
                # Sometimes nested under event
                if not markets and isinstance(d.get("event"), dict):
                    markets = d["event"].get("markets", [])
            if markets:
                return markets
        except Exception:
            pass
    return []


# Map engine market_type -> matching logic against SportyBet market descriptions
# Each entry: (market_name_keywords, outcome_matcher_function)
def _outcome_matches_home(desc, home, away):
    return _team_match(home, desc) or desc.strip() in ("1", "home")

def _outcome_matches_away(desc, home, away):
    return _team_match(away, desc) or desc.strip() in ("2", "away")

def _outcome_matches_draw(desc, home, away):
    return "draw" in desc.lower() or desc.strip().upper() == "X"


# Mapping: engine market_type -> (sb_market_name_keywords, specifier_value, outcome_desc_matcher)
SB_MARKET_MAP = {
    "home_win":            (["1x2"], None, "home"),
    "away_win":            (["1x2"], None, "away"),
    "draw":                (["1x2"], None, "draw"),
    "double_chance_1X":    (["double chance"], None, "1X"),
    "double_chance_X2":    (["double chance"], None, "X2"),
    "over_0.5":            (["over/under"], "0.5", "over"),
    "over_1.5":            (["over/under"], "1.5", "over"),
    "over_2.5":            (["over/under"], "2.5", "over"),
    "over_3.5":            (["over/under"], "3.5", "over"),
    "under_2.5":           (["over/under"], "2.5", "under"),
    "under_3.5":           (["over/under"], "3.5", "under"),
    "btts_yes":            (["gg/ng", "both teams to score"], None, "yes"),
    "btts_no":             (["gg/ng", "both teams to score"], None, "no"),
    "corners_over_7.5":    (["corner"], "7.5", "over"),
    "corners_over_8.5":    (["corner"], "8.5", "over"),
    "corners_over_9.5":    (["corner"], "9.5", "over"),
    "cards_over_2.5":      (["card", "booking"], "2.5", "over"),
    "cards_over_3.5":      (["card", "booking"], "3.5", "over"),
}


def sb_map_pick_to_selection(pick, markets):
    """
    Given an engine pick and the event's markets, find the matching
    SportyBet marketId + specifier + outcomeId.
    Handles SportyBet's standard outcome labels (Home/Draw/Away, 1/X/2,
    Over/Under, Yes/No) as well as full team names.
    """
    mt = pick["market_type"]
    home = pick["home"]
    away = pick["away"]

    mapping = SB_MARKET_MAP.get(mt)
    if not mapping:
        return None
    name_keywords, want_specifier, outcome_kind = mapping

    def is_home(d):
        d = d.strip().lower()
        return _team_match(home, d) or d in ("home", "1", "{} (home)".format(home.lower()))
    def is_away(d):
        d = d.strip().lower()
        return _team_match(away, d) or d in ("away", "2", "{} (away)".format(away.lower()))
    def is_draw(d):
        d = d.strip().lower()
        return d in ("draw", "x", "tie") or "draw" in d

    # Corners/cards markets use substring matching (names vary, e.g. "Total
    # Corners"); the core markets require an EXACT name to avoid grabbing
    # variant markets like "Monza Over/Under" or "1st Half - 1X2".
    use_substring = mt.startswith("corners") or mt.startswith("cards")

    for market in markets:
        m_name = (market.get("desc") or market.get("name")
                  or market.get("marketName") or "").lower().strip()
        m_specifier = market.get("specifier") or ""

        if use_substring:
            if not any(kw in m_name for kw in name_keywords):
                continue
        else:
            if m_name not in name_keywords:
                continue
        if want_specifier and want_specifier not in str(m_specifier) \
           and want_specifier not in m_name:
            continue

        outcomes = market.get("outcomes") or market.get("outcome") or []
        for oc in outcomes:
            oc_desc = (oc.get("desc") or oc.get("name") or "").strip()
            od = oc_desc.lower()
            matched = False
            if outcome_kind == "home":
                matched = is_home(od)
            elif outcome_kind == "away":
                matched = is_away(od)
            elif outcome_kind == "draw":
                matched = is_draw(od)
            elif outcome_kind == "1X":
                # SportyBet: "Home or Draw" (id 9)
                nd = od.replace(" ", "").replace("/", "")
                matched = ("draw" in od and ("home" in od or is_home(od))) or \
                          nd in ("1x", "1ordraw", "homeordraw")
            elif outcome_kind == "X2":
                # SportyBet: "Draw or Away" (id 11)
                nd = od.replace(" ", "").replace("/", "")
                matched = ("draw" in od and ("away" in od or is_away(od))) or \
                          nd in ("x2", "2ordraw", "awayordraw", "draworaway")
            elif outcome_kind == "over":
                matched = "over" in od or od.startswith("o ") or od == "o"
            elif outcome_kind == "under":
                matched = "under" in od or od.startswith("u ") or od == "u"
            elif outcome_kind == "yes":
                matched = od in ("yes", "gg") or "yes" in od
            elif outcome_kind == "no":
                matched = od in ("no", "ng") or od == "no"

            if matched:
                return {
                    "eventId": pick.get("sb_event_id", ""),
                    "marketId": str(market.get("id", "")),
                    "specifier": m_specifier if m_specifier else None,
                    "outcomeId": str(oc.get("id", "")),
                }
    return None


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Create a booking code from selections
# ═══════════════════════════════════════════════════════════════════

def sb_create_code(selections):
    """
    POST selections to SportyBet, return booking code or None.
    selections = [{eventId, marketId, specifier, outcomeId}, ...]
    """
    if not selections:
        return None
    url = "{}/orders/share".format(SB_BASE)
    resp = _sb_post(url, {"selections": selections}, diag=True)
    if not resp:
        return None
    # SportyBet success: message="Success" / bizCode=10000, code lives in data.shareCode
    ok = str(resp.get("message", "")).lower() == "success" or resp.get("bizCode") == 10000
    if ok:
        data = resp.get("data", {}) or {}
        return data.get("shareCode") or data.get("code") or data.get("shareURL", "").split("shareCode=")[-1] or None
    return None


def sb_decode_code(code):
    """
    Decode an existing SportyBet code (for testing / validation).
    Returns the selections list or None.
    """
    if not code:
        return None
    url = "{}/orders/share/{}".format(SB_BASE, code)
    resp = _sb_get(url)
    if not resp:
        return None
    if str(resp.get("message", "")).lower() == "success":
        return resp.get("data", {}).get("outcomes", [])
    return None


# ═══════════════════════════════════════════════════════════════════
# ORCHESTRATOR: accumulator -> SportyBet code
# ═══════════════════════════════════════════════════════════════════

def generate_code_for_accumulator(accumulator, event_id_cache=None):
    """
    Take an accumulator (from football_engine.build_accumulator) and
    generate a SportyBet booking code.

    Returns dict: {code, mapped, total, missing} where:
      code   = the booking code (or None)
      mapped = number of picks successfully mapped
      total  = total picks in the accumulator
      missing = list of picks that couldn't be mapped
    """
    if event_id_cache is None:
        event_id_cache = {}

    selections = []
    missing = []

    for pick in accumulator["selections"]:
        match_key = pick["match"]

        # Resolve event ID (cached per match)
        if match_key in event_id_cache:
            event_id = event_id_cache[match_key]
        else:
            event_id = sb_search_event(pick["home"], pick["away"])
            event_id_cache[match_key] = event_id
            time.sleep(0.4)

        if not event_id:
            missing.append("{} ({})".format(pick["match"], pick["pick"]))
            continue

        pick["sb_event_id"] = event_id
        # Attach kickoff time from the SportyBet event info we captured
        info = _SB_EVENT_INFO.get(event_id, {})
        if info.get("kickoff_ts"):
            pick["kickoff_ts"] = info["kickoff_ts"]

        # Get markets and map the pick
        markets = sb_get_event_markets(event_id)
        time.sleep(0.3)
        selection = sb_map_pick_to_selection(pick, markets)

        if selection and selection.get("outcomeId"):
            selections.append(selection)
        else:
            missing.append("{} ({})".format(pick["match"], pick["pick"]))

    code = sb_create_code(selections) if selections else None

    return {
        "code": code,
        "mapped": len(selections),
        "total": len(accumulator["selections"]),
        "missing": missing,
        "selections": selections,
    }


"""
CMVNG BOT v3 — DASHBOARD TEMPLATES
Glassmorphism, light-green theme, DM Sans + JetBrains Mono.
Matches the arcaprotocol aesthetic (frosted cards, bold display type).
"""

# Shared CSS for all v3 football pages
FB_CSS = """
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700;900&family=JetBrains+Mono:wght@400;500;700&display=swap');
* { margin:0; padding:0; box-sizing:border-box; }
body {
  font-family:'DM Sans',sans-serif;
  background:
    radial-gradient(1200px 600px at 10% -10%, rgba(74,222,128,0.12), transparent 60%),
    radial-gradient(1000px 500px at 90% 0%, rgba(34,197,94,0.10), transparent 55%),
    linear-gradient(160deg, #eafaf0 0%, #dff5e8 35%, #d2f0de 100%);
  color:#0f2417; min-height:100vh; padding-bottom:60px;
}
.nav {
  position:sticky; top:0; z-index:50;
  display:flex; align-items:center; justify-content:space-between;
  padding:16px 22px;
  background:rgba(255,255,255,0.55);
  backdrop-filter:blur(18px); -webkit-backdrop-filter:blur(18px);
  border-bottom:1px solid rgba(74,222,128,0.25);
}
.nav .logo { font-weight:900; font-size:1.15rem; color:#15803d; letter-spacing:-0.5px; }
.nav .logo span { color:#0f2417; }
.nav .tabs { display:flex; gap:6px; flex-wrap:wrap; }
.nav .tabs a {
  font-size:0.8rem; font-weight:700; text-decoration:none; color:#356148;
  padding:8px 14px; border-radius:999px; transition:all .15s;
}
.nav .tabs a:hover { background:rgba(74,222,128,0.18); }
.nav .tabs a.active { background:#15803d; color:#fff; }
.wrap { max-width:980px; margin:0 auto; padding:28px 18px 0; }
.page-head { margin:18px 4px 22px; }
.page-head h1 { font-size:2.1rem; font-weight:900; letter-spacing:-1px; color:#0f2417; }
.page-head .sub { color:#42795a; font-size:0.95rem; margin-top:4px; font-weight:500; }
.page-head .date { font-family:'JetBrains Mono',monospace; font-size:0.8rem; color:#5b8a6e; margin-top:6px; }

/* Glass card */
.glass {
  background:rgba(255,255,255,0.62);
  backdrop-filter:blur(20px); -webkit-backdrop-filter:blur(20px);
  border:1px solid rgba(255,255,255,0.8);
  border-radius:22px;
  box-shadow:0 8px 32px rgba(21,128,61,0.10), inset 0 1px 0 rgba(255,255,255,0.6);
  padding:22px; margin-bottom:18px;
}

/* Accumulator tier card */
.tier { position:relative; overflow:hidden; }
.tier-head { display:flex; align-items:center; justify-content:space-between; margin-bottom:16px; }
.tier-title { display:flex; align-items:center; gap:10px; }
.tier-title .dot { width:12px; height:12px; border-radius:50%; }
.tier-title h2 { font-size:1.15rem; font-weight:900; letter-spacing:-0.3px; }
.tier-odds {
  font-family:'JetBrains Mono',monospace; font-weight:700; font-size:1.4rem;
  color:#15803d;
}
.tier-odds .lbl { font-size:0.65rem; color:#5b8a6e; display:block; text-align:right; font-weight:500; letter-spacing:1px; }
.sel { display:flex; align-items:flex-start; gap:12px; padding:11px 0; border-top:1px solid rgba(21,128,61,0.10); }
.sel:first-of-type { border-top:none; }
.sel .ico { font-size:1.1rem; margin-top:1px; }
.sel .body { flex:1; min-width:0; }
.sel .match { font-weight:700; font-size:0.88rem; color:#0f2417; }
.sel .pick { font-size:0.82rem; color:#2f6347; margin-top:1px; }
.sel .reason { font-size:0.72rem; color:#6b9580; margin-top:3px; font-style:italic; }
.sel .odds { font-family:'JetBrains Mono',monospace; font-weight:700; font-size:0.95rem; color:#15803d; white-space:nowrap; }
.sel .conf { font-family:'JetBrains Mono',monospace; font-size:0.68rem; color:#5b8a6e; text-align:right; }
.code-box {
  margin-top:16px; padding:14px 16px; border-radius:14px;
  background:linear-gradient(135deg, rgba(74,222,128,0.20), rgba(34,197,94,0.12));
  border:1px dashed rgba(21,128,61,0.4);
  display:flex; align-items:center; justify-content:space-between; gap:10px;
}
.code-box .label { font-size:0.68rem; color:#42795a; font-weight:700; text-transform:uppercase; letter-spacing:1px; }
.code-box .code { font-family:'JetBrains Mono',monospace; font-weight:700; font-size:1.35rem; color:#15803d; letter-spacing:2px; }
.code-box a { font-size:0.75rem; font-weight:700; color:#fff; background:#15803d; padding:8px 14px; border-radius:999px; text-decoration:none; white-space:nowrap; }
.code-box.pending { background:rgba(120,120,120,0.08); border-color:rgba(120,120,120,0.3); }
.code-box.pending .code { color:#888; font-size:0.9rem; }

/* Match pick card */
.match-card .mhead { display:flex; align-items:center; justify-content:space-between; margin-bottom:6px; }
.match-card .teams { font-weight:900; font-size:1.05rem; color:#0f2417; letter-spacing:-0.3px; }
.match-card .league { font-size:0.68rem; color:#5b8a6e; font-weight:700; text-transform:uppercase; letter-spacing:1px; background:rgba(74,222,128,0.15); padding:3px 10px; border-radius:999px; }
.match-card .meta { font-family:'JetBrains Mono',monospace; font-size:0.72rem; color:#5b8a6e; margin-bottom:12px; }
.match-card .meta .inj { color:#c2410c; }
.match-card .status { display:inline-block; font-family:'JetBrains Mono',monospace; font-size:0.74rem; font-weight:800; padding:3px 10px; border-radius:8px; margin-bottom:10px; letter-spacing:0.3px; }
.match-card .status.live { background:rgba(220,38,38,0.12); color:#dc2626; }
.match-card .status.live .det { font-weight:600; opacity:0.8; }
.match-card .status.ft { background:rgba(21,128,61,0.12); color:#15803d; }
.match-card .status.pre { background:rgba(91,138,110,0.12); color:#5b8a6e; }
.pickrow { display:flex; align-items:center; gap:10px; padding:9px 0; border-top:1px solid rgba(21,128,61,0.08); }
.pickrow:first-of-type { border-top:none; }
.pickrow .rank { width:22px; height:22px; border-radius:50%; background:#15803d; color:#fff; font-size:0.7rem; font-weight:700; display:flex; align-items:center; justify-content:center; }
.pickrow .ptext { flex:1; font-weight:600; font-size:0.85rem; color:#1a3d2a; }
.pickrow .pct { font-family:'JetBrains Mono',monospace; font-weight:700; font-size:0.95rem; }
.bar { height:5px; background:rgba(21,128,61,0.12); border-radius:999px; margin-top:5px; overflow:hidden; }
.bar > div { height:100%; background:linear-gradient(90deg,#4ade80,#15803d); border-radius:999px; }
.empty { text-align:center; padding:50px 20px; color:#5b8a6e; }
.empty .big { font-size:2.2rem; margin-bottom:10px; }
.disclaimer { text-align:center; font-size:0.72rem; color:#6b9580; margin:24px 18px; line-height:1.5; }

/* Calendar */
.cal-head { display:flex; align-items:center; justify-content:space-between; margin-bottom:14px; }
.cal-head h2 { font-size:1.2rem; font-weight:900; color:#0f2417; }
.cal-nav a { text-decoration:none; color:#15803d; font-weight:700; font-size:1.3rem; padding:4px 12px; border-radius:10px; background:rgba(74,222,128,0.15); }
.cal-grid { display:grid; grid-template-columns:repeat(7,1fr); gap:6px; }
.cal-dow { text-align:center; font-size:0.65rem; font-weight:700; color:#5b8a6e; text-transform:uppercase; letter-spacing:0.5px; padding:4px 0; }
.cal-cell { aspect-ratio:1; border-radius:12px; background:rgba(255,255,255,0.4); border:1px solid rgba(255,255,255,0.6); display:flex; flex-direction:column; align-items:center; justify-content:center; text-decoration:none; color:#0f2417; position:relative; transition:transform .12s; }
.cal-cell.has-data { background:rgba(74,222,128,0.22); border-color:rgba(21,128,61,0.35); font-weight:700; }
.cal-cell.has-data:active { transform:scale(0.95); }
.cal-cell.empty-cell { background:transparent; border:none; }
.cal-cell.today { outline:2px solid #15803d; }
.cal-cell .dnum { font-size:0.85rem; }
.cal-cell .dcount { font-size:0.6rem; color:#15803d; font-family:'JetBrains Mono',monospace; margin-top:1px; }
.cal-cell .ddots { display:flex; gap:2px; margin-top:2px; }
.cal-cell .ddot { width:5px; height:5px; border-radius:50%; }
.legend { display:flex; gap:14px; justify-content:center; flex-wrap:wrap; margin-top:14px; font-size:0.72rem; color:#5b8a6e; }
.legend span { display:flex; align-items:center; gap:5px; }
.legend i { width:9px; height:9px; border-radius:50%; display:inline-block; }
.back-link { display:inline-block; margin-bottom:14px; color:#15803d; font-weight:700; text-decoration:none; font-size:0.85rem; }
.badge { font-size:0.62rem; font-weight:700; padding:2px 8px; border-radius:999px; text-transform:uppercase; letter-spacing:0.5px; }
.badge.won { background:rgba(21,128,61,0.15); color:#15803d; }
.badge.lost { background:rgba(220,38,38,0.13); color:#dc2626; }
.badge.pending { background:rgba(120,120,120,0.12); color:#777; }
.badge.void { background:rgba(120,120,120,0.12); color:#999; }
.tier .sel.won .pick { color:#15803d; }
.tier .sel.lost .pick { color:#dc2626; text-decoration:line-through; opacity:0.75; }

@media (max-width:600px){
  .page-head h1 { font-size:1.7rem; }
  .nav { padding:12px 14px; gap:8px; }
  .nav .logo { font-size:0.95rem; }
  .nav .tabs { gap:3px; overflow-x:auto; -webkit-overflow-scrolling:touch; flex-wrap:nowrap; max-width:62vw; }
  .nav .tabs a { padding:7px 10px; font-size:0.72rem; white-space:nowrap; }
  .tier-odds { font-size:1.2rem; }
  .wrap { padding:18px 12px 0; }
  .glass { padding:16px; border-radius:18px; }
  .sel .match { font-size:0.82rem; }
  .cal-grid { gap:4px; }
  .cal-cell .dnum { font-size:0.78rem; }
}
"""


def _nav(active):
    tabs = [
        ("picks", "/app/picks", "⚽ Picks"),
        ("codes", "/app/codes", "🎫 Codes"),
        ("crypto", "/app/paper-poly", "💰 Crypto"),
        ("sports", "/app/sports", "📊 Markets"),
        ("results", "/app/results", "📈 Results"),
    ]
    items = "".join(
        '<a href="{}" class="{}">{}</a>'.format(url, "active" if key == active else "", label)
        for key, url, label in tabs
    )
    return ('<div class="nav"><div class="logo">CMVNG<span>BOT</span></div>'
            '<div class="tabs">{}</div></div>').format(items)


def _fb_fmt_kickoff(ts):
    """Epoch-ms -> 'Sat 15:00' (UTC+1 Lagos), or '' if unknown."""
    if not ts:
        return ""
    try:
        dt = _dt.datetime.fromtimestamp(int(ts) / 1000, _dt.timezone.utc) + _dt.timedelta(hours=1)
        return dt.strftime("%a %H:%M")
    except Exception:
        return ""


def render_codes_page(accumulators, date_str):
    """Render the SportyBet codes page. accumulators = list of dicts with code info."""
    blocks = []
    for acca in accumulators:
        if not acca:
            continue
        rows = []
        for s in acca["selections"]:
            ko = _fb_fmt_kickoff(s.get("kickoff_ts"))
            ko_html = '<div class="reason">🕐 {}</div>'.format(ko) if ko else ""
            rows.append(
                '<div class="sel"><div class="ico">⚽</div>'
                '<div class="body"><div class="match">{}</div>'
                '<div class="pick">{}</div>{}</div>'
                '<div><div class="odds">{}</div><div class="conf">{:.0f}%</div></div></div>'.format(
                    s["match"], s["pick"], ko_html, s["odds"], s["confidence"]))
        sels = "".join(rows)
        if acca.get("code"):
            code_box = (
                '<div class="code-box"><div><div class="label">SportyBet Code</div>'
                '<div class="code">{}</div></div>'
                '<a href="https://www.sportybet.com/ng/sport/football?shareCode={}" target="_blank">Open →</a></div>'
            ).format(acca["code"], acca["code"])
        else:
            code_box = ('<div class="code-box pending"><div><div class="label">SportyBet Code</div>'
                        '<div class="code">build manually below</div></div></div>')

        dot_color = {"🟢": "#15803d", "🟡": "#ca8a04", "🟠": "#ea580c", "🔴": "#dc2626"}.get(acca["emoji"], "#15803d")
        blocks.append(
            '<div class="glass tier"><div class="tier-head"><div class="tier-title">'
            '<div class="dot" style="background:{}"></div><h2>{}</h2></div>'
            '<div class="tier-odds">{:.2f}<span class="lbl">TOTAL ODDS</span></div></div>'
            '{}{}</div>'.format(dot_color, acca["label"], acca["total_odds"], sels, code_box)
        )

    if not blocks:
        body = ('<div class="glass empty"><div class="big">🎫</div>'
                '<div>No codes generated yet. The engine runs every few hours — '
                'check back after the next scan.</div></div>')
    else:
        body = "".join(blocks)

    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SportyBet Codes — Cmvng Bot</title><style>{css}</style></head><body>
{nav}
<div class="wrap">
<div class="page-head"><h1>Today's Codes</h1>
<div class="sub">Accumulator booking codes for SportyBet</div>
<div class="date">{date}</div></div>
{body}
<div class="disclaimer">Codes are auto-generated from data analysis. Odds may shift before kickoff.
Always review selections in your SportyBet app before staking. No bet is guaranteed.</div>
</div></body></html>""".format(css=FB_CSS, nav=_nav("codes"), date=date_str, body=body)


def render_picks_page(match_picks, date_str):
    """Render the analyzed picks page. match_picks = dict {match: [top picks]}."""
    blocks = []
    for match, picks in match_picks.items():
        if not picks:
            continue
        first = picks[0]
        meta_bits = []
        if first.get("home_form_disp"):
            meta_bits.append("Form: {} {} | {} {}".format(
                first["home"], first.get("home_form_disp", "?"),
                first["away"], first.get("away_form_disp", "?")))
        meta = " · ".join(meta_bits) if meta_bits else first.get("reasoning", "")

        rows = "".join(
            '<div class="pickrow"><div class="rank">{}</div>'
            '<div class="ptext">{}<div class="bar"><div style="width:{:.0f}%"></div></div></div>'
            '<div class="pct" style="color:{}">{:.0f}%</div></div>'.format(
                i, p["pick"], p["confidence"],
                "#15803d" if p["confidence"] >= 70 else ("#ca8a04" if p["confidence"] >= 55 else "#ea580c"),
                p["confidence"])
            for i, p in enumerate(picks, 1)
        )

        # Live status + accurate league from ESPN (refreshed in background)
        league = first.get("league", "")
        status_html = ""
        try:
            live = _fb_live_status(first.get("home", ""), first.get("away", ""))
        except Exception:
            live = None
        if live:
            if live.get("league"):
                league = live["league"]
            st = live.get("state")
            hs, aw = live.get("hs"), live.get("aw")
            det = live.get("detail", "")
            if st == "in":
                status_html = ('<div class="status live">🔴 LIVE {}–{} '
                               '<span class="det">{}</span></div>').format(
                    hs if hs is not None else 0, aw if aw is not None else 0, det)
            elif st == "post":
                status_html = ('<div class="status ft">✅ FT {}–{}</div>').format(
                    hs if hs is not None else 0, aw if aw is not None else 0)
            elif st == "pre" and det:
                status_html = '<div class="status pre">⏳ {}</div>'.format(det)

        ko = first.get("kickoff_ts")
        if not status_html and ko:
            kt = _fb_fmt_kickoff(ko)
            if kt:
                status_html = '<div class="status pre">⏳ {}</div>'.format(kt)

        blocks.append(
            '<div class="glass match-card"><div class="mhead">'
            '<div class="teams">{}</div><div class="league">{}</div></div>'
            '{}<div class="meta">{}</div>{}</div>'.format(
                match, league, status_html, meta, rows)
        )

    if not blocks:
        body = ('<div class="glass empty"><div class="big">⚽</div>'
                '<div>No picks analyzed yet. The engine runs every few hours.</div></div>')
    else:
        body = "".join(blocks)

    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Football Picks — Cmvng Bot</title><style>{css}</style></head><body>
{nav}
<div class="wrap">
<div class="page-head"><h1>Football Picks</h1>
<div class="sub">Top 3 highest-probability picks per match</div>
<div class="date">{date}</div></div>
{body}
<div class="disclaimer">Picks generated from form, xG, head-to-head, injuries and team stats.
Percentages are model estimates, not guarantees.</div>
</div></body></html>""".format(css=FB_CSS, nav=_nav("picks"), date=date_str, body=body)


def render_results_page(stats, date_str):
    """Tier win-rate summary cards (used at top of the calendar page)."""
    blocks = []
    for st in stats:
        wr = (st["wins"] / st["settled"] * 100) if st.get("settled") else 0
        blocks.append(
            '<div class="glass"><div class="tier-head">'
            '<div class="tier-title"><h2>{}</h2></div>'
            '<div class="tier-odds">{:.0f}%<span class="lbl">WIN RATE</span></div></div>'
            '<div class="meta" style="font-family:JetBrains Mono,monospace;color:#5b8a6e;font-size:0.8rem">'
            '{} won / {} settled · {} pending</div></div>'.format(
                st["tier_label"], wr, st["wins"], st["settled"], st.get("pending", 0))
        )
    body = "".join(blocks) if blocks else (
        '<div class="glass empty"><div class="big">📈</div>'
        '<div>No settled results yet. Win rates appear after matches finish.</div></div>')
    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Results — Cmvng Bot</title><style>{css}</style></head><body>
{nav}<div class="wrap"><div class="page-head"><h1>Results</h1>
<div class="sub">Win-rate tracking per accumulator tier</div>
<div class="date">{date}</div></div>{body}</div></body></html>""".format(
        css=FB_CSS, nav=_nav("results"), date=date_str, body=body)


_MONTHS = ["", "January", "February", "March", "April", "May", "June",
           "July", "August", "September", "October", "November", "December"]


def render_results_calendar(year, month, day_data, tier_stats, today_iso):
    """
    Calendar view of results history.
    day_data = {day_int: {"slips":N, "won":N, "lost":N, "pending":N}}
    tier_stats = list of tier summary dicts (overall win rates)
    """
    import calendar as _cal
    cal = _cal.Calendar(firstweekday=0)  # Monday first
    weeks = cal.monthdayscalendar(year, month)

    # Prev / next month links
    pm, py = (12, year - 1) if month == 1 else (month - 1, year)
    nm, ny = (1, year + 1) if month == 12 else (month + 1, year)

    dow = "".join('<div class="cal-dow">{}</div>'.format(d)
                  for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])

    cells = []
    for week in weeks:
        for day in week:
            if day == 0:
                cells.append('<div class="cal-cell empty-cell"></div>')
                continue
            iso = "{:04d}-{:02d}-{:02d}".format(year, month, day)
            d = day_data.get(day)
            classes = "cal-cell"
            if iso == today_iso:
                classes += " today"
            if d and d["slips"] > 0:
                classes += " has-data"
                dots = ""
                if d["won"]:
                    dots += '<span class="ddot" style="background:#15803d"></span>'
                if d["lost"]:
                    dots += '<span class="ddot" style="background:#dc2626"></span>'
                if d["pending"]:
                    dots += '<span class="ddot" style="background:#9ca3af"></span>'
                cells.append(
                    '<a href="/app/results?date={}" class="{}">'
                    '<div class="dnum">{}</div>'
                    '<div class="dcount">{} slip{}</div>'
                    '<div class="ddots">{}</div></a>'.format(
                        iso, classes, day, d["slips"], "" if d["slips"] == 1 else "s", dots))
            else:
                cells.append('<div class="{}"><div class="dnum">{}</div></div>'.format(classes, day))

    grid = '<div class="cal-grid">{}{}</div>'.format(dow, "".join(cells))

    cal_card = (
        '<div class="glass"><div class="cal-head">'
        '<div class="cal-nav"><a href="/app/results?ym={:04d}-{:02d}">‹</a></div>'
        '<h2>{} {}</h2>'
        '<div class="cal-nav"><a href="/app/results?ym={:04d}-{:02d}">›</a></div>'
        '</div>{}'
        '<div class="legend">'
        '<span><i style="background:#15803d"></i>Won</span>'
        '<span><i style="background:#dc2626"></i>Lost</span>'
        '<span><i style="background:#9ca3af"></i>Pending</span></div></div>'
    ).format(py, pm, _MONTHS[month], year, ny, nm, grid)

    # Tier summary below
    summary = []
    for st in tier_stats:
        wr = (st["wins"] / st["settled"] * 100) if st.get("settled") else 0
        summary.append(
            '<div class="glass" style="padding:14px 18px"><div class="tier-head" style="margin:0">'
            '<div class="tier-title"><h2 style="font-size:0.95rem">{}</h2></div>'
            '<div class="tier-odds" style="font-size:1.1rem">{:.0f}%'
            '<span class="lbl">{} / {}</span></div></div></div>'.format(
                st["tier_label"], wr, st["wins"], st["settled"]))
    summary_html = "".join(summary)

    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Results — Cmvng Bot</title><style>{css}</style></head><body>
{nav}<div class="wrap"><div class="page-head"><h1>Results History</h1>
<div class="sub">Tap any highlighted day to see that day's slips</div></div>
{cal}{summary}</div></body></html>""".format(
        css=FB_CSS, nav=_nav("results"), cal=cal_card, summary=summary_html)


def render_results_day(date_iso, date_human, accas):
    """Detail view for one day's accumulators."""
    blocks = []
    for a in accas:
        status = (a.get("result") or "pending").lower()
        badge = '<span class="badge {}">{}</span>'.format(
            status, {"won": "WON", "lost": "LOST", "void": "VOID"}.get(status, "PENDING"))
        sels = ""
        try:
            sel_list = a.get("selections", [])
            parts = []
            for s in sel_list:
                lr = (s.get("result") or "").lower()
                if lr == "won":
                    ico, cls = "✅", "won"
                elif lr == "lost":
                    ico, cls = "❌", "lost"
                else:
                    ico, cls = "⚽", "pending"
                parts.append(
                    '<div class="sel {}"><div class="ico">{}</div><div class="body">'
                    '<div class="match">{}</div><div class="pick">{}</div></div>'
                    '<div class="odds">{}</div></div>'.format(
                        cls, ico, s.get("match", ""), s.get("pick", ""), s.get("odds", "")))
            sels = "".join(parts)
        except Exception:
            pass
        code = ""
        if a.get("sportybet_code"):
            code = ('<div class="code-box"><div><div class="label">SportyBet Code</div>'
                    '<div class="code">{}</div></div></div>'.format(a["sportybet_code"]))
        blocks.append(
            '<div class="glass tier"><div class="tier-head"><div class="tier-title">'
            '<h2>{}</h2>{}</div><div class="tier-odds">{:.2f}'
            '<span class="lbl">TOTAL ODDS</span></div></div>{}{}</div>'.format(
                a.get("label", ""), badge, a.get("total_odds", 0), sels, code))

    body = "".join(blocks) if blocks else (
        '<div class="glass empty"><div class="big">📅</div>'
        '<div>No slips were generated on this day.</div></div>')

    return """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{date} Results — Cmvng Bot</title><style>{css}</style></head><body>
{nav}<div class="wrap">
<a href="/app/results" class="back-link">‹ Back to calendar</a>
<div class="page-head"><h1>{date}</h1><div class="sub">Slips generated this day</div></div>
{body}</div></body></html>""".format(
        css=FB_CSS, nav=_nav("results"), date=date_human, body=body)


"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — TELEGRAM COMMAND SYSTEM
═══════════════════════════════════════════════════════════════════
Menu structure exactly as specified:

  /sports  -> [Polymarket Sports] [Limitless Sports] [Football Picks]
  /crypto  -> [Polymarket Crypto] [Limitless Crypto]
  /picks   -> today's football picks
  /codes   -> SportyBet booking codes (5 tiers)
  /live    -> all unresolved bets across platforms
  /results -> win rates per tier

Anyone can use / commands to browse. New picks/signals auto-send.

This module builds the message text + inline keyboards. The actual
send/answer happens via the Telegram Bot API. Wired in app.py.
═══════════════════════════════════════════════════════════════════
"""

import json

try:
    import requests as _req
except ImportError:
    _req = None


# ═══════════════════════════════════════════════════════════════════
# LOW-LEVEL TELEGRAM API
# ═══════════════════════════════════════════════════════════════════

def tg_send(token, chat_id, text, keyboard=None, parse_mode="HTML"):
    """Send a message, optionally with an inline keyboard."""
    if _req is None or not token or not chat_id:
        return None
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode,
               "disable_web_page_preview": True}
    if keyboard:
        payload["reply_markup"] = json.dumps({"inline_keyboard": keyboard})
    try:
        r = _req.post("https://api.telegram.org/bot{}/sendMessage".format(token),
                      json=payload, timeout=10)
        return r.json()
    except Exception as e:
        print("[TG] send error: {}".format(e))
        return None


def tg_answer_callback(token, callback_id, text=""):
    """Acknowledge a button tap so Telegram stops the loading spinner."""
    if _req is None or not token:
        return
    try:
        _req.post("https://api.telegram.org/bot{}/answerCallbackQuery".format(token),
                  json={"callback_query_id": callback_id, "text": text}, timeout=8)
    except Exception:
        pass


def tg_set_webhook(token, url):
    """Register the webhook URL with Telegram."""
    if _req is None or not token:
        return None
    try:
        r = _req.post("https://api.telegram.org/bot{}/setWebhook".format(token),
                      json={"url": url, "allowed_updates": ["message", "callback_query"]},
                      timeout=10)
        return r.json()
    except Exception as e:
        print("[TG] setWebhook error: {}".format(e))
        return None


def tg_set_commands(token):
    """Register the slash-command menu shown in the Telegram UI."""
    if _req is None or not token:
        return
    commands = [
        {"command": "picks", "description": "Today's football picks"},
        {"command": "codes", "description": "SportyBet booking codes"},
        {"command": "sports", "description": "Sports markets menu"},
        {"command": "crypto", "description": "Crypto signals menu"},
        {"command": "live", "description": "Unresolved bets (all platforms)"},
        {"command": "results", "description": "Win rates per tier"},
    ]
    try:
        _req.post("https://api.telegram.org/bot{}/setMyCommands".format(token),
                  json={"commands": commands}, timeout=8)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# MENU KEYBOARDS
# ═══════════════════════════════════════════════════════════════════

def kb_sports_menu():
    return [
        [{"text": "📊 Polymarket Sports", "callback_data": "sports_poly"}],
        [{"text": "📊 Limitless Sports", "callback_data": "sports_limitless"}],
        [{"text": "⚽ Football Picks", "callback_data": "show_picks"}],
        [{"text": "🎫 SportyBet Codes", "callback_data": "show_codes"}],
    ]


def kb_crypto_menu():
    return [
        [{"text": "💰 Polymarket Crypto", "callback_data": "crypto_poly"}],
        [{"text": "💰 Limitless Crypto", "callback_data": "crypto_limitless"}],
    ]


def kb_main_menu():
    return [
        [{"text": "⚽ Football Picks", "callback_data": "show_picks"},
         {"text": "🎫 Codes", "callback_data": "show_codes"}],
        [{"text": "📊 Sports Markets", "callback_data": "menu_sports"},
         {"text": "💰 Crypto", "callback_data": "menu_crypto"}],
        [{"text": "📈 Results", "callback_data": "show_results"},
         {"text": "🔴 Live Bets", "callback_data": "show_live"}],
    ]


# ═══════════════════════════════════════════════════════════════════
# MESSAGE FORMATTERS
# ═══════════════════════════════════════════════════════════════════

def fmt_welcome():
    return (
        "🤖 <b>CMVNG BOT</b>\n\n"
        "Your automated football + crypto prediction engine.\n\n"
        "<b>Commands:</b>\n"
        "/picks — today's football picks\n"
        "/codes — SportyBet booking codes\n"
        "/sports — sports markets menu\n"
        "/crypto — crypto signals menu\n"
        "/live — unresolved bets\n"
        "/results — win rates\n\n"
        "Pick a section below 👇"
    )


def fmt_codes(accumulators, date_str):
    """Format the SportyBet codes message for all tiers — clean and scannable."""
    if not accumulators:
        return ("🎫 <b>SPORTYBET CODES</b>\n\nNo codes generated yet. "
                "The engine runs every few hours — check back soon.")

    lines = ["🎫 <b>SPORTYBET CODES</b>", "<i>{}</i>".format(date_str), ""]
    for acca in accumulators:
        if not acca:
            continue
        lines.append("━━━━━━━━━━━━━━━")
        lines.append("{} <b>{}</b>".format(acca["emoji"], acca["label"]))
        lines.append("💰 Total odds: <b>{:.2f}</b>  ·  {} legs".format(
            acca["total_odds"], acca.get("num_selections", len(acca["selections"]))))
        lines.append("")
        for s in acca["selections"]:
            ko = _fb_fmt_kickoff(s.get("kickoff_ts")) if "_fb_fmt_kickoff" in globals() else ""
            ko_str = "  🕐 {}".format(ko) if ko else ""
            lines.append("  ⚽ {}{}".format(_short(s["match"]), ko_str))
            lines.append("     → <b>{}</b>  @ {:.2f}".format(s["pick"], s["odds"]))
        lines.append("")
        if acca.get("code"):
            lines.append("🎫 Code: <code>{}</code>".format(acca["code"]))
            lines.append('🔗 <a href="https://www.sportybet.com/ng/sport/football?shareCode={}">Open in SportyBet</a>'.format(acca["code"]))
        else:
            lines.append("🎫 <i>Build manually from the picks above</i>")
        lines.append("")
    return "\n".join(lines)


def fmt_picks(match_picks, date_str, limit=12):
    """Format the football picks message — top 3 per match, clean layout."""
    if not match_picks:
        return ("⚽ <b>FOOTBALL PICKS</b>\n\nNo picks analyzed yet. "
                "The engine runs every few hours.")

    lines = ["⚽ <b>FOOTBALL PICKS</b>", "<i>{}</i>".format(date_str),
             "<i>Top 3 picks per match</i>", ""]
    count = 0
    for match, picks in match_picks.items():
        if not picks or count >= limit:
            continue
        count += 1
        lines.append("🏟 <b>{}</b>".format(match))
        for i, p in enumerate(picks, 1):
            bar = "🟢" if p["confidence"] >= 70 else ("🟡" if p["confidence"] >= 55 else "🟠")
            lines.append("  {} {} — <b>{:.0f}%</b>".format(bar, p["pick"], p["confidence"]))
        lines.append("")
    lines.append("🎫 /codes for ready-made SportyBet slips")
    return "\n".join(lines)


def fmt_results(stats, date_str):
    if not stats:
        return "📈 <b>RESULTS</b>\n\nNo settled results yet. Win rates appear after matches finish."
    lines = ["📈 <b>WIN RATES BY TIER</b>", ""]
    for st in stats:
        wr = (st["wins"] / st["settled"] * 100) if st.get("settled") else 0
        lines.append("{}".format(st["tier_label"]))
        lines.append("   ✅ {} won / {} settled ({:.0f}%)  ·  ⏳ {} pending".format(
            st["wins"], st["settled"], wr, st.get("pending", 0)))
        lines.append("")
    return "\n".join(lines)


def _short(match, n=34):
    return match if len(match) <= n else match[:n-1] + "…"


"""
═══════════════════════════════════════════════════════════════════
CMVNG BOT v3 — FOOTBALL INTEGRATION GLUE
═══════════════════════════════════════════════════════════════════
This is the code that gets inlined into app.py. It assumes all the
engine/scraper/sportybet/web/telegram functions are in the same
namespace (they're concatenated above it in the final app.py).

Provides:
  - DB tables (football_picks, sportybet_accumulators, pick_results)
  - run_football_engine()  -> the daily scrape→analyze→build→codes→save→telegram
  - in-memory cache so /picks /codes serve instantly
  - background thread (every 6h)
  - Flask routes  /app/picks /app/codes /app/results
  - Telegram webhook  /api/telegram-webhook  + command router
═══════════════════════════════════════════════════════════════════
"""


import os
import json
import time
import threading
import datetime as _dt


# ═══════════════════════════════════════════════════════════════════
# IN-MEMORY CACHE — latest engine output (so commands respond instantly)
# ═══════════════════════════════════════════════════════════════════

_FB_CACHE = {
    "date": "",
    "match_picks": {},      # {match: [top picks]}
    "accumulators": [],     # [acca dict with code]
    "last_run": None,
    "running": False,
}


# ═══════════════════════════════════════════════════════════════════
# DB SETUP
# ═══════════════════════════════════════════════════════════════════

def fb_init_db(get_db):
    """Create football tables. Safe to call repeatedly."""
    try:
        conn = get_db()
        conn.run("""
            CREATE TABLE IF NOT EXISTS football_picks (
                id SERIAL PRIMARY KEY,
                match_date DATE,
                home TEXT, away TEXT, league TEXT,
                market_type TEXT, pick TEXT,
                confidence REAL, odds REAL,
                reasoning TEXT,
                result TEXT DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.run("""
            CREATE TABLE IF NOT EXISTS sportybet_accumulators (
                id SERIAL PRIMARY KEY,
                match_date DATE,
                tier TEXT, label TEXT,
                target_odds REAL, total_odds REAL,
                num_selections INT,
                selections_json TEXT,
                sportybet_code TEXT,
                status TEXT DEFAULT 'pending',
                result TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.run("""
            CREATE TABLE IF NOT EXISTS pick_results (
                id SERIAL PRIMARY KEY,
                match_date DATE,
                tier TEXT, total_picks INT, hits INT,
                won BOOLEAN, total_odds REAL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Migrations — in case an older football_picks/accumulators table exists
        # from a previous session with a different schema.
        _fp_cols = [
            ("match_date", "DATE"), ("home", "TEXT"), ("away", "TEXT"),
            ("league", "TEXT"), ("market_type", "TEXT"), ("pick", "TEXT"),
            ("confidence", "REAL"), ("odds", "REAL"), ("reasoning", "TEXT"),
            ("result", "TEXT DEFAULT 'pending'"), ("created_at", "TIMESTAMPTZ DEFAULT NOW()"),
        ]
        for col, typ in _fp_cols:
            try:
                conn.run("ALTER TABLE football_picks ADD COLUMN IF NOT EXISTS {} {}".format(col, typ))
            except Exception:
                pass
        _acc_cols = [
            ("match_date", "DATE"), ("tier", "TEXT"), ("label", "TEXT"),
            ("target_odds", "REAL"), ("total_odds", "REAL"), ("num_selections", "INT"),
            ("selections_json", "TEXT"), ("sportybet_code", "TEXT"),
            ("status", "TEXT DEFAULT 'pending'"), ("result", "TEXT"),
            ("created_at", "TIMESTAMPTZ DEFAULT NOW()"),
        ]
        for col, typ in _acc_cols:
            try:
                conn.run("ALTER TABLE sportybet_accumulators ADD COLUMN IF NOT EXISTS {} {}".format(col, typ))
            except Exception:
                pass
        conn.close()
        print("[FB] DB tables ready")
    except Exception as e:
        print("[FB] DB init error: {}".format(e))


def fb_save_run(get_db, date_str, all_picks, accumulators):
    """Persist the day's picks and accumulators."""
    try:
        conn = get_db()
        today = _dt.date.today()
        # Save top picks (limit to keep DB lean)
        for p in all_picks[:200]:
            conn.run("""INSERT INTO football_picks
                (match_date, home, away, league, market_type, pick, confidence, odds, reasoning)
                VALUES (:d,:h,:a,:l,:mt,:pk,:cf,:od,:rs)""",
                d=today, h=p["home"], a=p["away"], l=p["league"],
                mt=p["market_type"], pk=p["pick"], cf=p["confidence"],
                od=p["odds"], rs=p["reasoning"])
        # Save accumulators
        for acca in accumulators:
            if not acca:
                continue
            conn.run("""INSERT INTO sportybet_accumulators
                (match_date, tier, label, target_odds, total_odds, num_selections,
                 selections_json, sportybet_code)
                VALUES (:d,:t,:lb,:tg,:to,:ns,:sj,:cd)""",
                d=today, t=acca["tier"], lb=acca["label"],
                tg=acca["target_odds"], to=acca["total_odds"],
                ns=acca["num_selections"],
                sj=json.dumps([{"match": s["match"], "pick": s["pick"],
                                "odds": s["odds"], "confidence": s["confidence"],
                                "market_type": s.get("market_type", ""),
                                "home": s.get("home", ""), "away": s.get("away", ""),
                                "sb_event_id": s.get("sb_event_id", ""),
                                "kickoff_ts": s.get("kickoff_ts", 0),
                                "result": s.get("result", "pending")}
                               for s in acca["selections"]]),
                cd=acca.get("code"))
        conn.close()
    except Exception as e:
        print("[FB] save error: {}".format(e))


# ═══════════════════════════════════════════════════════════════════
# SETTLEMENT — mark accumulators won/lost from final scores
# ═══════════════════════════════════════════════════════════════════

def _fb_settle_pick(market_type, pick_text, hs, aw):
    """Evaluate a pick against a final score. Returns True/False/None (unknown)."""
    total = hs + aw
    mt = market_type
    if mt == "home_win":            return hs > aw
    if mt == "away_win":            return aw > hs
    if mt == "draw":                return hs == aw
    if mt == "double_chance_1X":    return hs >= aw
    if mt == "double_chance_X2":    return aw >= hs
    if mt == "over_0.5":            return total > 0.5
    if mt == "over_1.5":            return total > 1.5
    if mt == "over_2.5":            return total > 2.5
    if mt == "over_3.5":            return total > 3.5
    if mt == "under_2.5":           return total < 2.5
    if mt == "under_3.5":           return total < 3.5
    if mt == "btts_yes":            return hs > 0 and aw > 0
    if mt == "btts_no":             return not (hs > 0 and aw > 0)
    if mt == "home_win_btts":       return hs > aw and hs > 0 and aw > 0
    if mt == "home_win_over_2.5":   return hs > aw and total > 2.5
    if mt == "dc_over_1.5":         return hs >= aw and total > 1.5
    if mt == "handicap_home_-1.5":  return (hs - aw) > 1.5
    if mt == "handicap_away_-1.5":  return (aw - hs) > 1.5
    if mt == "correct_score":
        m = _sports_re.search(r'(\d+)-(\d+)', pick_text or "")
        if m:
            return hs == int(m.group(1)) and aw == int(m.group(2))
        return None
    # corners_* / cards_* can't be settled from the goal score alone
    return None


def sb_get_event_result(event_id, home, away):
    """
    Fallback: fetch score for a SportyBet event that's still listed (live or
    just-finished). SportyBet drops games soon after they end, so this only
    catches in-play/just-ended games — ESPN is the primary settlement source.
    """
    try:
        sb_search_event(home, away)
    except Exception:
        pass
    info = _SB_EVENT_INFO.get(event_id, {})
    hs, aw = info.get("home_score"), info.get("away_score")
    status = str(info.get("match_status") or "").lower()
    st = info.get("status")
    finished = ("end" in status or "ft" in status or "finish" in status
                or st in (3, 4, 100))
    if hs is not None and aw is not None:
        try:
            return int(hs), int(aw), finished
        except (ValueError, TypeError):
            return None
    return None


# ── ESPN scoreboard: the match-status + score feed (free, no key, keeps
#    finished games — unlike SportyBet which drops them, and livescore.com
#    whose own API is body-encrypted). Powers league labels, live scores,
#    in-play status AND settlement. One scoreboard call per league per date. ──
ESPN_SOCCER_LEAGUES = [
    "eng.1", "eng.2", "esp.1", "esp.2", "ita.1", "ita.2", "ger.1", "ger.2",
    "fra.1", "fra.2", "ned.1", "por.1", "sco.1", "tur.1", "bel.1", "gre.1",
    "usa.1", "mex.1", "aut.1", "sui.1", "rus.1", "ukr.1",
    "uefa.champions", "uefa.europa", "uefa.europa_conf", "uefa.nations",
    "fifa.friendly", "fifa.friendly.w", "fifa.world",
    "fifa.worldq.uefa", "fifa.worldq.conmebol", "fifa.worldq.concacaf",
    "fifa.worldq.afc", "fifa.worldq.caf", "fifa.worldq.ofc",
    "conmebol.libertadores", "conmebol.sudamericana", "conmebol.america",
    "afc.cup", "aus.1", "jpn.1", "bra.1", "arg.1", "col.1", "chi.1",
]

ESPN_LEAGUE_NAMES = {
    "eng.1": "Premier League", "eng.2": "Championship",
    "esp.1": "LaLiga", "esp.2": "LaLiga 2", "ita.1": "Serie A", "ita.2": "Serie B",
    "ger.1": "Bundesliga", "ger.2": "Bundesliga 2", "fra.1": "Ligue 1", "fra.2": "Ligue 2",
    "ned.1": "Eredivisie", "por.1": "Primeira Liga", "sco.1": "Scottish Prem",
    "tur.1": "Süper Lig", "bel.1": "Belgian Pro", "gre.1": "Super League Greece",
    "usa.1": "MLS", "mex.1": "Liga MX", "aut.1": "Austrian Bundesliga",
    "sui.1": "Swiss Super League", "rus.1": "Russian Premier", "ukr.1": "Ukrainian Premier",
    "uefa.champions": "Champions League", "uefa.europa": "Europa League",
    "uefa.europa_conf": "Conference League", "uefa.nations": "Nations League",
    "fifa.friendly": "International Friendly", "fifa.friendly.w": "Int'l Friendly (W)",
    "fifa.world": "World Cup",
    "fifa.worldq.uefa": "WC Qualifier (UEFA)", "fifa.worldq.conmebol": "WC Qualifier (CONMEBOL)",
    "fifa.worldq.concacaf": "WC Qualifier (CONCACAF)", "fifa.worldq.afc": "WC Qualifier (AFC)",
    "fifa.worldq.caf": "WC Qualifier (CAF)", "fifa.worldq.ofc": "WC Qualifier (OFC)",
    "conmebol.libertadores": "Copa Libertadores", "conmebol.sudamericana": "Copa Sudamericana",
    "conmebol.america": "Copa América", "afc.cup": "AFC Cup", "aus.1": "A-League",
    "jpn.1": "J1 League", "bra.1": "Brasileirão", "arg.1": "Liga Argentina",
    "col.1": "Liga Colombiana", "chi.1": "Primera Chile",
}


def _espn_scoreboard(slug, yyyymmdd):
    """One ESPN league+date scoreboard. Returns rich game dicts:
    {home, away, hs, aw, state(pre|in|post), completed, detail, league}."""
    if _req is None:
        return []
    url = ("https://site.api.espn.com/apis/site/v2/sports/soccer/{}"
           "/scoreboard?dates={}".format(slug, yyyymmdd))
    out = []
    league_name = ESPN_LEAGUE_NAMES.get(slug, slug)
    try:
        r = _req.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return []
        data = r.json()
        # ESPN also reports a precise league name at the top level
        try:
            lg = (data.get("leagues") or [{}])[0]
            league_name = lg.get("name") or lg.get("abbreviation") or league_name
        except Exception:
            pass
        for ev in data.get("events", []):
            comp = (ev.get("competitions") or [{}])[0]
            stype = ((ev.get("status") or {}).get("type")) or {}
            state = stype.get("state") or ""          # pre | in | post
            completed = bool(stype.get("completed"))
            detail = stype.get("shortDetail") or stype.get("detail") or ""
            home = away = None
            hs = aw = None
            for c in comp.get("competitors", []):
                team = c.get("team") or {}
                nm = team.get("displayName") or team.get("name") or team.get("shortDisplayName") or ""
                sc = c.get("score")
                if c.get("homeAway") == "home":
                    home, hs = nm, sc
                else:
                    away, aw = nm, sc
            if not (home and away):
                continue
            try:
                hs_i = int(hs) if hs is not None else None
                aw_i = int(aw) if aw is not None else None
            except (ValueError, TypeError):
                hs_i = aw_i = None
            out.append({"home": home, "away": away, "hs": hs_i, "aw": aw_i,
                        "state": state, "completed": completed,
                        "detail": detail, "league": league_name})
    except Exception:
        return []
    return out


def _thesportsdb_day(yyyymmdd):
    """All soccer for a day from TheSportsDB (free, broad lower-league + friendly
    coverage, one call per day). Returns rich game dicts like _espn_scoreboard."""
    if _req is None:
        return []
    d = "{}-{}-{}".format(yyyymmdd[:4], yyyymmdd[4:6], yyyymmdd[6:8])
    url = "https://www.thesportsdb.com/api/v1/json/3/eventsday.php?d={}&s=Soccer".format(d)
    out = []
    try:
        r = _req.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return []
        for ev in (r.json().get("events") or []):
            home = ev.get("strHomeTeam")
            away = ev.get("strAwayTeam")
            if not (home and away):
                continue
            status = str(ev.get("strStatus") or "").lower().strip()
            hs, aw = ev.get("intHomeScore"), ev.get("intAwayScore")
            try:
                hs_i = int(hs) if hs not in (None, "") else None
                aw_i = int(aw) if aw not in (None, "") else None
            except (ValueError, TypeError):
                hs_i = aw_i = None
            completed = status in ("match finished", "ft", "finished", "aet", "pen",
                                   "after extra time", "after penalties")
            if status in ("1h", "2h", "ht", "live", "et", "bt", "p"):
                state = "in"
            elif completed:
                state = "post"
            else:
                state = "pre"
            out.append({"home": home, "away": away, "hs": hs_i, "aw": aw_i,
                        "state": state, "completed": completed,
                        "detail": ev.get("strStatus") or "",
                        "league": ev.get("strLeague") or ""})
    except Exception:
        return []
    return out


def _livescore_day(yyyymmdd):
    """All football for a day from Livescore.com's PUBLIC json endpoint (no key,
    not the encrypted CDN one) — broadest single-call coverage incl. lower
    leagues and friendlies. Endpoint pattern per the Simatwa/livescore-api repo.
    Returns rich game dicts like the other feeds."""
    if _req is None:
        return []
    url = ("https://prod-public-api.livescore.com/v1/api/app/date/soccer/{}/3?MD=1".format(yyyymmdd))
    out = []
    try:
        r = _req.get(url, timeout=12, headers={
            "User-Agent": "Mozilla/5.0", "Referer": "https://www.livescore.com/"})
        if r.status_code != 200:
            return []
        data = r.json()
        for stage in (data.get("Stages") or []):
            league = stage.get("Snm") or ""
            country = stage.get("Cnm") or ""
            if country and country.lower() not in league.lower():
                league_full = "{}: {}".format(country, league)
            else:
                league_full = league
            for ev in (stage.get("Events") or []):
                t1 = ev.get("T1") or [{}]
                t2 = ev.get("T2") or [{}]
                home = (t1[0].get("Nm") if t1 else "") or ""
                away = (t2[0].get("Nm") if t2 else "") or ""
                if not (home and away):
                    continue
                hs, aw = ev.get("Tr1"), ev.get("Tr2")
                try:
                    hs_i = int(hs) if hs not in (None, "") else None
                    aw_i = int(aw) if aw not in (None, "") else None
                except (ValueError, TypeError):
                    hs_i = aw_i = None
                eps = str(ev.get("Eps") or "").upper().strip()
                completed = eps in ("FT", "AET", "AP", "PEN", "FT_PEN", "AWD", "ABD")
                if eps in ("NS", "", "POSTP", "CANC", "TBD"):
                    state = "pre"
                elif completed:
                    state = "post"
                else:
                    state = "in"      # HT or a live minute like "63'"
                out.append({"home": home, "away": away, "hs": hs_i, "aw": aw_i,
                            "state": state, "completed": completed,
                            "detail": ("FT" if state == "post" else eps),
                            "league": league_full})
    except Exception:
        return []
    return out


def _fb_espn_index(dates):
    """Rich index of all games (pre/in/post) for given dates. Livescore.com is
    the primary feed (one broad call/day); TheSportsDB + ESPN fill any gaps."""
    index = []
    ls_n = tsdb_n = espn_n = 0
    for d in dates:
        ls = _livescore_day(d)
        if ls:
            index.extend(ls)
            ls_n += len(ls)
        time.sleep(0.1)
        rows = _thesportsdb_day(d)
        if rows:
            index.extend(rows)
            tsdb_n += len(rows)
        time.sleep(0.1)
        for slug in ESPN_SOCCER_LEAGUES:
            erows = _espn_scoreboard(slug, d)
            if erows:
                index.extend(erows)
                espn_n += len(erows)
            time.sleep(0.04)
    fin = sum(1 for r in index if r.get("completed"))
    live = sum(1 for r in index if r.get("state") == "in")
    print("[FB] score index: {} games ({} finished, {} live) "
          "[Livescore {}, TheSportsDB {}, ESPN {}] across {} dates".format(
              len(index), fin, live, ls_n, tsdb_n, espn_n, len(dates)))
    return index


# back-compat alias used by the settler
def _fb_build_score_index(dates):
    return _fb_espn_index(dates)


def _fb_norm_team(s):
    s = (s or "").lower().strip()
    s = s.replace("&", " and ")
    for junk in (" fc", " cf", " sc", " ac", " afc", " cd", " ud", " club",
                 " calcio", " 1929", " 1913", " 04", " 05", " 09", ".",
                 " and ", " de ", " of "):
        s = s.replace(junk, " ")
    s = (s.replace("á", "a").replace("é", "e").replace("í", "i")
           .replace("ó", "o").replace("ú", "u").replace("ñ", "n")
           .replace("ç", "c").replace("ü", "u").replace("ö", "o")
           .replace("ä", "a").replace("ø", "o").replace("å", "a"))
    return " ".join(s.split())


def _fb_teams_match(a, b):
    """Fuzzy team-name match tolerant of suffixes, accents, reserve sides."""
    a, b = _fb_norm_team(a), _fb_norm_team(b)
    if not a or not b:
        return False
    if a == b or a in b or b in a:
        return True
    wa, wb = set(a.split()), set(b.split())
    return any(len(w) >= 4 and w in wb for w in wa)


def _fb_closest_team(name, index):
    """Best-guess feed spelling of a team, for diagnostics. Returns name or ''."""
    target = set(_fb_norm_team(name).split())
    best, best_score = "", 0
    seen = set()
    for g in index:
        for t in (g.get("home", ""), g.get("away", "")):
            if t in seen:
                continue
            seen.add(t)
            toks = set(_fb_norm_team(t).split())
            overlap = len(target & toks)
            if overlap > best_score:
                best, best_score = t, overlap
    return best if best_score > 0 else ""


def _fb_find_game(index, home, away):
    """Find a game in the index matching these teams. Returns the game dict
    (with scores possibly home/away-swapped to the caller's orientation)."""
    for g in index:
        if _fb_teams_match(home, g["home"]) and _fb_teams_match(away, g["away"]):
            return g
        if _fb_teams_match(home, g["away"]) and _fb_teams_match(away, g["home"]):
            sw = dict(g)
            sw["hs"], sw["aw"] = g["aw"], g["hs"]
            sw["home"], sw["away"] = g["away"], g["home"]
            return sw
    return None


def _fb_lookup_score(index, home, away):
    """Final score for a FINISHED game matching these teams. -> (hs,aw) or None."""
    g = _fb_find_game(index, home, away)
    if g and g.get("completed") and g.get("hs") is not None and g.get("aw") is not None:
        return g["hs"], g["aw"]
    return None


# ── Live-status cache: refreshed in the background so the picks page can show
#    league + kickoff/LIVE/FT + current score without a slow API call per load.
_FB_LIVE = {"index": [], "ts": 0}


def _fb_live_status(home, away):
    """Return {league, state, detail, hs, aw} for a match, or None."""
    g = _fb_find_game(_FB_LIVE.get("index", []), home, away)
    return g


def _fb_live_refresh_thread():
    """Refresh the live match index (today ± 1) every ~3 minutes. Uses the cheap
    TheSportsDB feed (one call per day, all live/finished soccer) so the picks
    page can show league + LIVE/FT + scores without hammering ESPN."""
    def loop():
        time.sleep(45)
        while True:
            try:
                today = _dt.date.today()
                idx = []
                for i in (-1, 0, 1):
                    d = (today + _dt.timedelta(days=i)).strftime("%Y%m%d")
                    day = _livescore_day(d) or _thesportsdb_day(d)
                    idx.extend(day)
                    time.sleep(0.2)
                if idx:
                    _FB_LIVE["index"] = idx
                    _FB_LIVE["ts"] = int(time.time())
                    live = sum(1 for g in idx if g.get("state") == "in")
                    print("[FB] live index: {} games ({} live)".format(len(idx), live))
            except Exception as e:
                print("[FB] live refresh error: {}".format(e))
            time.sleep(180)
    threading.Thread(target=loop, daemon=True).start()
    print("[FB] live-status thread started (every 3min)")





def _fb_settle_accumulators(get_db):
    """Settle pending accumulators using ESPN final scores. Triggers off the
    slip's match_date (always stored) rather than per-leg kickoff timestamps."""
    today = _dt.date.today()
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT id, match_date, selections_json, result FROM sportybet_accumulators "
            "WHERE result IS NULL OR result = 'pending'")
        pend = [(r[0], r[1], r[2]) for r in rows]
        conn.close()
    except Exception as e:
        print("[FB] settle query error: {}".format(e))
        return

    if not pend:
        return

    # Which match-dates are due (game day is today or earlier)?
    def _as_date(v):
        if isinstance(v, _dt.date):
            return v
        try:
            return _dt.date.fromisoformat(str(v)[:10])
        except Exception:
            return None

    due_slips = []
    for acc_id, md, sj in pend:
        d = _as_date(md)
        if d and d <= today:
            due_slips.append((acc_id, d, sj))

    if not due_slips:
        return

    # Fetch a window that covers games played recently AND this weekend.
    # Slips store their creation date, but the games inside can be several days
    # later (e.g. Segunda fixtures created Thu but played Sat), so we scan a
    # band around today rather than the slip's creation date.
    dates = [(today + _dt.timedelta(days=i)).strftime("%Y%m%d")
             for i in (-3, -2, -1, 0, 1, 2, 3)]
    print("[FB] settle: {} pending slips due, fetching scores for {} dates".format(
        len(due_slips), len(dates)))
    index = _fb_build_score_index(dates)
    if not index:
        print("[FB] settle: no finished games available yet")
        return

    settled_count = 0
    for acc_id, md, sj in due_slips:
        try:
            sels = json.loads(sj) if sj else []
        except Exception:
            continue
        if not sels:
            continue

        any_lost = False
        all_known = True
        evaluable = 0
        upcoming = []   # in the feed but not finished yet (game is future/live)
        absent = []     # not found in the feed at all (coverage / name mismatch)
        for s in sels:
            home, away = s.get("home", ""), s.get("away", "")
            score = _fb_lookup_score(index, home, away)
            if score is None:
                all_known = False
                g = _fb_find_game(index, home, away)
                label = s.get("match", "{} vs {}".format(home, away))
                if g:
                    st = g.get("state")
                    upcoming.append("{}[{}]".format(label, "live" if st == "in" else "upcoming"))
                else:
                    ch = _fb_closest_team(home, index)
                    ca = _fb_closest_team(away, index)
                    hint = ""
                    if ch or ca:
                        hint = " (closest in feed: {} / {})".format(ch or "?", ca or "?")
                    absent.append(label + hint)
                continue
            hs, aw = score
            outcome = _fb_settle_pick(s.get("market_type", ""), s.get("pick", ""), hs, aw)
            if outcome is None:
                continue  # corners/cards — can't grade from final score
            evaluable += 1
            s["result"] = "won" if outcome else "lost"
            if not outcome:
                any_lost = True

        new_result = None
        if any_lost:
            new_result = "lost"
        elif all_known and evaluable > 0:
            new_result = "won"
        elif md < today - _dt.timedelta(days=7):
            # a week old and still unresolvable → stop showing pending
            new_result = "void"

        if new_result:
            try:
                conn = get_db()
                conn.run("UPDATE sportybet_accumulators SET result=:r, selections_json=:sj WHERE id=:i",
                         r=new_result, sj=json.dumps(sels), i=acc_id)
                conn.close()
                settled_count += 1
            except Exception as e:
                print("[FB] settle update error: {}".format(e))
        else:
            bits = []
            if upcoming:
                bits.append("not played yet: " + ", ".join(upcoming[:3]))
            if absent:
                bits.append("NOT IN FEED: " + ", ".join(absent[:3]))
            if bits:
                print("[FB] settle: slip {} pending — {}".format(acc_id, " | ".join(bits)))

    if settled_count:
        print("[FB] Settled {} accumulators".format(settled_count))


def fb_settle_thread(get_db):
    """Background thread: settle finished accumulators hourly."""
    def loop():
        _t = __import__("time")
        _t.sleep(180)
        while True:
            try:
                _fb_settle_accumulators(get_db)
            except Exception as e:
                print("[FB] settle thread error: {}".format(e))
            _t.sleep(3600)
    threading.Thread(target=loop, daemon=True).start()
    print("[FB] settle thread started (hourly)")


# ═══════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATION
# Assumes these are in namespace (inlined above in app.py):
#   build_fixture_dataset, analyze_fixture, build_all_accumulators,
#   top_picks_per_match, generate_code_for_accumulator,
#   tg_send (+ token/chat), fmt_codes, fmt_picks
# ═══════════════════════════════════════════════════════════════════

def _fb_enrich_and_filter_upcoming(fixtures, max_lookup=60):
    """
    Look each fixture up on SportyBet and keep ONLY upcoming, not-started
    games that still have markets. SportyBet removes a game the moment it
    kicks off, so 'present on SportyBet with markets' == 'bettable'.

    Attaches sb_event_id + kickoff_ts to each surviving fixture.
    Returns (upcoming_fixtures, match->event_id cache for code-gen reuse).
    """
    import time as _t
    now_ms = int(_t.time() * 1000)
    upcoming = []
    cache = {}
    looked = 0
    for fx in fixtures:
        if looked >= max_lookup:
            break
        looked += 1
        try:
            eid = sb_search_event(fx["home_team"], fx["away_team"])
            _t.sleep(0.35)
        except Exception:
            eid = None
        if not eid:
            continue
        info = _SB_EVENT_INFO.get(eid, {})
        ko = info.get("kickoff_ts") or 0
        status = str(info.get("match_status") or "").lower()
        has_markets = bool(_SB_MARKET_CACHE.get(eid))
        # Keep only games that haven't started and still have markets
        not_started = ("not start" in status) or (status == "" and (not ko or now_ms < ko))
        future_ok = (not ko) or (now_ms < ko)
        # Reject far-future fixtures (e.g. next-season openers SportyBet lists
        # months out) — only keep games within the next ~4 days.
        near_term = (not ko) or (ko < now_ms + 4 * 24 * 3600 * 1000)
        if not has_markets or not not_started or not future_ok or not near_term:
            continue
        fx["sb_event_id"] = eid
        fx["kickoff_ts"] = ko
        cache["{} vs {}".format(fx["home_team"], fx["away_team"])] = eid
        upcoming.append(fx)
    print("[FB] {} upcoming bettable fixtures (looked up {} on SportyBet)".format(
        len(upcoming), looked))
    return upcoming, cache


def run_football_engine(get_db, tg_token, tg_chat, send_telegram,
                        generate_codes=True, announce=True):
    """
    The full daily pipeline. Designed to never crash — every stage is
    wrapped, and a failure in one stage doesn't kill the others.
    """
    if _FB_CACHE["running"]:
        print("[FB] engine already running, skip")
        return
    _FB_CACHE["running"] = True
    try:
        date_human = _dt.date.today().strftime("%A, %B %d, %Y")
        print("[FB] ═══ Engine run starting ({}) ═══".format(date_human))

        # 1. Scrape — PRIMARY: prediction scrapers (proven working on Railway).
        #    Sofascore is Cloudflare-blocked from datacenter IPs, so it's only a fallback.
        try:
            fixtures = _fb_fixtures_from_predictions()
            if not fixtures:
                print("[FB] prediction scrapers empty — trying Sofascore fallback")
                fixtures = build_fixture_dataset()
        except Exception as e:
            print("[FB] scrape failed: {}".format(e))
            fixtures = []

        if not fixtures:
            print("[FB] No fixtures — engine run aborted")
            _FB_CACHE["running"] = False
            return

        # 1b. Reset SportyBet caches, then enrich + filter to UPCOMING bettable
        #     games only. SportyBet drops games once they start, so this both
        #     gets kickoff times AND guarantees codes can map (markets exist).
        try:
            _SB_DIAG_COUNT[0] = 0
            _SB_MARKET_CACHE.clear()
            _SB_STRUCT_LOGGED[0] = False
            _SB_EVENT_INFO.clear()
        except Exception:
            pass

        event_cache = {}
        if generate_codes:
            try:
                fixtures, event_cache = _fb_enrich_and_filter_upcoming(fixtures)
            except Exception as e:
                print("[FB] enrich/filter error: {}".format(e))

        if not fixtures:
            print("[FB] No upcoming bettable fixtures right now — nothing to post")
            _FB_CACHE["running"] = False
            return

        # 2. Analyze
        all_picks = []
        for fx in fixtures:
            try:
                all_picks.extend(analyze_fixture(fx))
            except Exception as e:
                print("[FB] analyze error for {}: {}".format(fx.get("home_team"), e))
        print("[FB] Scored {} picks across {} fixtures".format(len(all_picks), len(fixtures)))

        if not all_picks:
            _FB_CACHE["running"] = False
            return

        # 2b. Keep only picks that map to a real SportyBet market on that event,
        #     so every accumulator leg produces a valid code (no dead legs from
        #     corners/cards/combos SportyBet doesn't offer for the game).
        build_picks = all_picks
        if generate_codes:
            mappable = []
            for p in all_picks:
                eid = p.get("sb_event_id")
                if not eid:
                    continue
                mkts = _SB_MARKET_CACHE.get(eid, [])
                if mkts and sb_map_pick_to_selection(p, mkts):
                    mappable.append(p)
            print("[FB] {} of {} picks are SportyBet-bettable".format(
                len(mappable), len(all_picks)))
            if mappable:
                build_picks = mappable

        # 3. Build accumulators (from bettable picks)
        try:
            acca_dict = build_all_accumulators(build_picks)
        except Exception as e:
            print("[FB] accumulator build error: {}".format(e))
            acca_dict = {}

        accumulators = [acca_dict[k] for k in
                        ["2_odds", "3_odds", "5_odds", "10_odds", "1000_odds"]
                        if acca_dict.get(k)]

        # 4. Generate SportyBet codes (reusing the event cache from enrichment)
        if generate_codes:
            for acca in accumulators:
                try:
                    result = generate_code_for_accumulator(acca, event_cache)
                    acca["code"] = result.get("code")
                    acca["code_mapped"] = result.get("mapped", 0)
                    acca["code_total"] = result.get("total", 0)
                    if result.get("code"):
                        print("[FB] {} code: {} ({}/{} mapped)".format(
                            acca["tier"], result["code"], result["mapped"], result["total"]))
                    else:
                        print("[FB] {} code FAILED ({}/{} mapped, missing: {})".format(
                            acca["tier"], result.get("mapped", 0), result.get("total", 0),
                            result.get("missing", [])[:3]))
                except Exception as e:
                    print("[FB] code gen error for {}: {}".format(acca["tier"], e))
                    acca["code"] = None

        # 5. Cache + persist
        match_picks = top_picks_per_match(all_picks, 3)
        _FB_CACHE["date"] = date_human
        _FB_CACHE["match_picks"] = match_picks
        _FB_CACHE["accumulators"] = accumulators
        _FB_CACHE["last_run"] = _dt.datetime.now()

        try:
            fb_save_run(get_db, date_human, all_picks, accumulators)
        except Exception as e:
            print("[FB] persist error: {}".format(e))

        # 6. Telegram announce
        if announce:
            try:
                msg = fmt_codes(accumulators, date_human)
                send_telegram(msg)
            except Exception as e:
                print("[FB] announce error: {}".format(e))

        print("[FB] ═══ Engine run complete ═══")
    finally:
        _FB_CACHE["running"] = False


def fb_scanner_thread(get_db, tg_token, tg_chat, send_telegram, interval_hours=6):
    """Background thread: run the engine on a schedule."""
    def loop():
        time.sleep(30)  # let app boot first
        # Initial run
        try:
            run_football_engine(get_db, tg_token, tg_chat, send_telegram)
        except Exception as e:
            print("[FB] initial run error: {}".format(e))
        # Periodic
        while True:
            time.sleep(interval_hours * 3600)
            try:
                run_football_engine(get_db, tg_token, tg_chat, send_telegram)
            except Exception as e:
                print("[FB] scheduled run error: {}".format(e))
    threading.Thread(target=loop, daemon=True).start()
    print("[FB] scanner thread started (every {}h)".format(interval_hours))


# ═══════════════════════════════════════════════════════════════════
# TELEGRAM WEBHOOK HANDLER
# Assumes in namespace: tg_send, tg_answer_callback, kb_*, fmt_*
# Plus crypto/sports market accessors from existing app.py
# ═══════════════════════════════════════════════════════════════════

def fb_handle_telegram_update(update, tg_token,
                              get_crypto_signals=None, get_sports_markets=None,
                              get_live_bets=None, get_results=None):
    """
    Process one Telegram update (message or callback).
    Returns nothing — sends replies directly.
    """
    try:
        # ── Slash commands ──
        if "message" in update:
            msg = update["message"]
            chat_id = msg["chat"]["id"]
            text = (msg.get("text") or "").strip().lower()

            if text in ("/start", "/menu", "/help"):
                tg_send(tg_token, chat_id, fmt_welcome(), kb_main_menu())
            elif text.startswith("/picks"):
                tg_send(tg_token, chat_id,
                        fmt_picks(_FB_CACHE["match_picks"], _FB_CACHE["date"] or "today"))
            elif text.startswith("/codes"):
                tg_send(tg_token, chat_id,
                        fmt_codes(_FB_CACHE["accumulators"], _FB_CACHE["date"] or "today"))
            elif text.startswith("/sports"):
                tg_send(tg_token, chat_id, "📊 <b>Sports Markets</b>\nChoose a source:", kb_sports_menu())
            elif text.startswith("/crypto"):
                tg_send(tg_token, chat_id, "💰 <b>Crypto Signals</b>\nChoose a source:", kb_crypto_menu())
            elif text.startswith("/live"):
                _send_live(tg_token, chat_id, get_live_bets)
            elif text.startswith("/results"):
                _send_results(tg_token, chat_id, get_results)
            return

        # ── Button taps ──
        if "callback_query" in update:
            cq = update["callback_query"]
            chat_id = cq["message"]["chat"]["id"]
            data = cq.get("data", "")
            tg_answer_callback(tg_token, cq["id"])

            if data == "show_picks":
                tg_send(tg_token, chat_id,
                        fmt_picks(_FB_CACHE["match_picks"], _FB_CACHE["date"] or "today"))
            elif data == "show_codes":
                tg_send(tg_token, chat_id,
                        fmt_codes(_FB_CACHE["accumulators"], _FB_CACHE["date"] or "today"))
            elif data == "menu_sports":
                tg_send(tg_token, chat_id, "📊 <b>Sports Markets</b>\nChoose a source:", kb_sports_menu())
            elif data == "menu_crypto":
                tg_send(tg_token, chat_id, "💰 <b>Crypto Signals</b>\nChoose a source:", kb_crypto_menu())
            elif data == "show_results":
                _send_results(tg_token, chat_id, get_results)
            elif data == "show_live":
                _send_live(tg_token, chat_id, get_live_bets)
            elif data in ("crypto_poly", "crypto_limitless"):
                platform = "polymarket" if data == "crypto_poly" else "limitless"
                _send_crypto(tg_token, chat_id, platform, get_crypto_signals)
            elif data in ("sports_poly", "sports_limitless"):
                platform = "polymarket" if data == "sports_poly" else "limitless"
                _send_sports(tg_token, chat_id, platform, get_sports_markets)
            return
    except Exception as e:
        print("[TG] handler error: {}".format(e))


def _send_crypto(tg_token, chat_id, platform, getter):
    if getter is None:
        tg_send(tg_token, chat_id, "💰 Crypto signals not available right now.")
        return
    try:
        signals = getter(platform)
        if not signals:
            tg_send(tg_token, chat_id,
                    "💰 <b>{} Crypto</b>\nNo open signals right now.".format(platform.title()))
            return
        lines = ["💰 <b>{} Crypto Signals</b>".format(platform.title()), ""]
        for s in signals[:15]:
            lines.append("• {}".format(s))
        tg_send(tg_token, chat_id, "\n".join(lines))
    except Exception as e:
        tg_send(tg_token, chat_id, "💰 Error loading crypto signals.")
        print("[TG] crypto error: {}".format(e))


def _send_sports(tg_token, chat_id, platform, getter):
    if getter is None:
        tg_send(tg_token, chat_id, "📊 Sports markets not available right now.")
        return
    try:
        markets = getter(platform)
        if not markets:
            tg_send(tg_token, chat_id,
                    "📊 <b>{} Sports</b>\n\nNo sports picks cached yet. "
                    "The scanner runs every few hours — check back soon.".format(platform.title()))
            return
        lines = ["📊 <b>{} — SPORTS PICKS</b>".format(platform.title()),
                 "<i>Verified across prediction sites</i>", ""]
        for m in markets[:12]:
            if isinstance(m, dict):
                home = m.get("home", "")
                away = m.get("away", "")
                winner = m.get("winner", "")
                market = m.get("market", "")
                url = m.get("url", "")
                score = m.get("score", "")
                lines.append("⚽ <b>{} vs {}</b>".format(home, away))
                if winner and winner.upper() != "DRAW":
                    lines.append("🏆 Pick: <b>{}</b>".format(winner))
                elif winner:
                    lines.append("🏆 Pick: <b>Draw</b>")
                if market:
                    lines.append("📍 {}".format(market.replace("🏆 ", "").replace("⚽ ", "")))
                if score:
                    lines.append("⭐ Confidence: {}/100".format(score))
                if url:
                    lines.append('🔗 <a href="{}">Place this bet</a>'.format(url))
                lines.append("")
            else:
                lines.append("• {}".format(m))
        tg_send(tg_token, chat_id, "\n".join(lines))
    except Exception as e:
        tg_send(tg_token, chat_id, "📊 Error loading sports markets.")
        print("[TG] sports error: {}".format(e))


def _send_live(tg_token, chat_id, getter):
    if getter is None:
        tg_send(tg_token, chat_id, "🔴 Live bets not available right now.")
        return
    try:
        bets = getter()
        if not bets:
            tg_send(tg_token, chat_id, "🔴 <b>Live Bets</b>\nNo unresolved bets right now.")
            return
        lines = ["🔴 <b>Unresolved Bets</b>", ""]
        for b in bets[:25]:
            lines.append("• {}".format(b))
        tg_send(tg_token, chat_id, "\n".join(lines))
    except Exception as e:
        tg_send(tg_token, chat_id, "🔴 Error loading live bets.")
        print("[TG] live error: {}".format(e))


def _send_results(tg_token, chat_id, getter):
    if getter is None:
        tg_send(tg_token, chat_id, fmt_results([], _FB_CACHE["date"] or "today"))
        return
    try:
        stats = getter()
        tg_send(tg_token, chat_id, fmt_results(stats, _FB_CACHE["date"] or "today"))
    except Exception as e:
        tg_send(tg_token, chat_id, "📈 Error loading results.")
        print("[TG] results error: {}".format(e))



# ═══════════════════════════════════════════════════════════════════
# FOOTBALL v3 — BRIDGE: data getters for Telegram + Flask routes
# ═══════════════════════════════════════════════════════════════════

def _fb_parse_score(score_str):
    """Parse a predicted score like '2-1' or '2:1' -> (2, 1), else None."""
    try:
        m = _sports_re.search(r'(\d+)\s*[-:]\s*(\d+)', str(score_str))
        if m:
            return int(m.group(1)), int(m.group(2))
    except Exception:
        pass
    return None


_FB_LEAGUE_NAMES = {
    "epl": "EPL", "la_liga": "La Liga", "serie_a": "Serie A",
    "bundesliga": "Bundesliga", "ligue_1": "Ligue 1",
    "ucl": "Champions League", "uel": "Europa League",
}


def _fb_fixtures_from_predictions(max_fixtures=40):
    """
    Build engine fixtures from the prediction scrapers that are PROVEN to
    work on Railway (footballpredictions.com + forebet + fp.net).

    Each prediction carries a predicted score (e.g. "2-1"). We derive the
    engine's goal/xG inputs and a form bias from that score, then optionally
    overlay real Understat xG when available. Markets with no real data
    (corners/cards) fall back to league averages inside the engine.
    """
    all_predictions = []
    for scraper, name in [
        (_sports_scrape_footballpredictions_com, "fp.com"),
        (_sports_scrape_forebet, "forebet"),
        (_sports_scrape_footballpredictions_net, "fp.net"),
    ]:
        try:
            all_predictions.extend(scraper() or [])
        except Exception as e:
            print("[FB] {} scrape error: {}".format(name, e))

    if not all_predictions:
        print("[FB] No predictions from any scraper")
        return []

    # Group predictions by match (handle home/away swaps)
    matches = {}
    for p in all_predictions:
        h = _sports_normalize_team(p.get("home", ""))
        a = _sports_normalize_team(p.get("away", ""))
        if not h or not a:
            continue
        key, rev = (h, a), (a, h)
        if key in matches:
            matches[key]["preds"].append(p)
        elif rev in matches:
            matches[rev]["preds"].append(p)
        else:
            matches[key] = {"home": p.get("home"), "away": p.get("away"), "preds": [p]}

    # Merge fuzzy-duplicate matches (e.g. "Ireland" vs "Republic of Ireland").
    # Substring-only match — strict enough not to merge "Man Utd" with "Man City".
    def _same_team(n1, n2):
        a1 = (n1 or "").lower().replace(" fc", "").replace("afc ", "").strip()
        b1 = (n2 or "").lower().replace(" fc", "").replace("afc ", "").strip()
        if not a1 or not b1:
            return False
        if a1 == b1:
            return True
        short, lng = (a1, b1) if len(a1) <= len(b1) else (b1, a1)
        if len(short) >= 4 and short in lng:
            return True
        # Initials match: "psg" == initials of "paris saint germain"
        short_ns = short.replace(" ", "").replace(".", "")
        initials = "".join(w[0] for w in lng.split() if w)
        if len(short_ns) >= 3 and short_ns == initials:
            return True
        return False

    merged = []
    for md in matches.values():
        placed = False
        for m in merged:
            same = ((_same_team(md["home"], m["home"]) and _same_team(md["away"], m["away"])) or
                    (_same_team(md["home"], m["away"]) and _same_team(md["away"], m["home"])))
            if same:
                m["preds"].extend(md["preds"])
                # Keep the longer (more complete) team names
                if len(md["home"]) > len(m["home"]):
                    m["home"] = md["home"]
                if len(md["away"]) > len(m["away"]):
                    m["away"] = md["away"]
                placed = True
                break
        if not placed:
            merged.append(md)

    # Optional: real xG overlay from Understat (best-effort, never fatal)
    xg_cache = {}

    fixtures = []
    for md in merged:
        scores = [s for s in (_fb_parse_score(p.get("score")) for p in md["preds"]) if s]
        if not scores:
            continue  # need at least one score prediction to analyze
        avg_h = sum(s[0] for s in scores) / len(scores)
        avg_a = sum(s[1] for s in scores) / len(scores)

        # League from prediction type tags
        league = ""
        for p in md["preds"]:
            t = (p.get("type") or "").lower()
            if t in _FB_LEAGUE_NAMES:
                league = _FB_LEAGUE_NAMES[t]
                break

        # Derive a form bias from the predicted margin
        margin = avg_h - avg_a
        if margin >= 1.5:
            hf, af = "WWWWW", "LLLDL"
        elif margin >= 0.6:
            hf, af = "WWWDW", "LDLLL"
        elif margin <= -1.5:
            hf, af = "LLLDL", "WWWWW"
        elif margin <= -0.6:
            hf, af = "LDLLL", "WWWDW"
        else:
            hf, af = "WDWDW", "DWDWD"

        both_score = avg_h >= 0.5 and avg_a >= 0.5
        btts_pct = 65 if both_score else 35

        fx = {
            "home_team": md["home"], "away_team": md["away"], "league": league,
            "kickoff_time": "",
            "home_form": hf, "away_form": af,
            "home_xg_for": round(avg_h, 2), "home_xg_against": round(avg_a, 2),
            "away_xg_for": round(avg_a, 2), "away_xg_against": round(avg_h, 2),
            "home_goals_scored_avg": round(avg_h, 2), "home_goals_conceded_avg": round(avg_a, 2),
            "away_goals_scored_avg": round(avg_a, 2), "away_goals_conceded_avg": round(avg_h, 2),
            "home_btts_pct": btts_pct, "away_btts_pct": btts_pct,
            "home_clean_sheet_pct": 35 if not both_score else 22,
            "away_clean_sheet_pct": 35 if not both_score else 22,
            "home_position": 8, "away_position": 8,
            "home_key_injuries": 0, "away_key_injuries": 0,
            "_pred_score": "{:.0f}-{:.0f}".format(round(avg_h), round(avg_a)),
            "_n_sources": len(set(p.get("source") for p in md["preds"])),
        }

        # Overlay real xG from Understat where the league is supported
        try:
            if league in UNDERSTAT_LEAGUES:
                if league not in xg_cache:
                    xg_cache[league] = understat_team_xg(league)
                lg = xg_cache[league]
                hx = _match_team_xg(lg, md["home"])
                ax = _match_team_xg(lg, md["away"])
                if hx:
                    fx["home_xg_for"] = hx["xg_for"]
                    fx["home_xg_against"] = hx["xg_against"]
                if ax:
                    fx["away_xg_for"] = ax["xg_for"]
                    fx["away_xg_against"] = ax["xg_against"]
        except Exception:
            pass

        fixtures.append(fx)

    # Prefer matches confirmed by more sources
    fixtures.sort(key=lambda f: f.get("_n_sources", 0), reverse=True)
    print("[FB] Built {} fixtures from predictions ({} with multi-source)".format(
        len(fixtures), sum(1 for f in fixtures if f.get("_n_sources", 0) >= 2)))
    return fixtures[:max_fixtures]


def _fb_today_human():
    return _dt.date.today().strftime("%A, %B %d, %Y")


def _fb_get_crypto_signals(platform):
    """Open crypto signals for a platform (from v2_paper_trades)."""
    out = []
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT asset, direction, timeframe, entry_odds, confidence "
            "FROM v2_paper_trades WHERE platform=:p AND status='OPEN' "
            "ORDER BY id DESC LIMIT 20", p=platform)
        conn.close()
        for r in rows:
            asset, direction, tf, odds, conf = r
            odds_disp = ""
            if odds:
                cents = odds * 100 if odds <= 1 else odds
                odds_disp = " @ {:.0f}c".format(cents)
            conf_disp = " ({})".format(conf) if conf else ""
            out.append("{} {} · {}{}{}".format(asset, direction, tf, odds_disp, conf_disp))
    except Exception as e:
        print("[FB] crypto getter error: {}".format(e))
    return out


def _fb_get_sports_markets(platform):
    """Recent sports market alerts for a platform (from in-memory cache)."""
    try:
        return list(_sports_market_cache.get(platform, []))
    except Exception:
        return []


def _fb_get_live_bets():
    """All unresolved bets across platforms (crypto open + football codes)."""
    out = []
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT platform, asset, direction, timeframe FROM v2_paper_trades "
            "WHERE status='OPEN' ORDER BY id DESC LIMIT 30")
        conn.close()
        for r in rows:
            out.append("{} · {} {} {}".format(r[0], r[1], r[2], r[3]))
    except Exception as e:
        print("[FB] live getter error: {}".format(e))
    for acca in _FB_CACHE.get("accumulators", []):
        if acca.get("code"):
            out.append("{} {} — code {} @ {:.2f}".format(
                acca.get("emoji", "⚽"), acca["label"], acca["code"], acca["total_odds"]))
    return out


def _fb_get_results():
    """Win-rate stats per accumulator tier."""
    tiers = [("2_odds", "🟢 2 ODDS — BANKER"), ("3_odds", "🟢 3 ODDS — SAFE"),
             ("5_odds", "🟡 5 ODDS — VALUE"), ("10_odds", "🟠 10 ODDS — RISK"),
             ("1000_odds", "🔴 1000+ ODDS — MOONSHOT")]
    stats = []
    agg = {}
    try:
        conn = get_db()
        rows = conn.run("SELECT tier, result FROM sportybet_accumulators")
        conn.close()
        for r in rows:
            tier, result = r[0], r[1]
            a = agg.setdefault(tier, {"wins": 0, "settled": 0, "pending": 0})
            if result == "won":
                a["wins"] += 1; a["settled"] += 1
            elif result == "lost":
                a["settled"] += 1
            else:
                a["pending"] += 1
    except Exception as e:
        print("[FB] results getter error: {}".format(e))
    for tier, label in tiers:
        a = agg.get(tier, {"wins": 0, "settled": 0, "pending": 0})
        stats.append({"tier_label": label, "wins": a["wins"],
                      "settled": a["settled"], "pending": a["pending"]})
    return stats


# ── Flask routes ──

@app.route("/app/picks")
def fb_picks_page():
    return render_picks_page(_FB_CACHE.get("match_picks", {}),
                             _FB_CACHE.get("date") or _fb_today_human())


@app.route("/app/codes")
def fb_codes_page():
    return render_codes_page(_FB_CACHE.get("accumulators", []),
                             _FB_CACHE.get("date") or _fb_today_human())


@app.route("/app/results")
def fb_results_page():
    date_q = request.args.get("date")
    ym_q = request.args.get("ym")

    # Day-detail view
    if date_q:
        accas = []
        try:
            conn = get_db()
            rows = conn.run(
                "SELECT label, total_odds, selections_json, sportybet_code, result, tier, id "
                "FROM sportybet_accumulators WHERE match_date = :d ORDER BY id DESC",
                d=date_q)
            conn.close()
            order = {"2_odds": 0, "3_odds": 1, "5_odds": 2, "10_odds": 3, "1000_odds": 4}
            seen_tiers = set()
            for r in rows:
                tier = r[5]
                if tier in seen_tiers:   # keep only the latest run's slip per tier
                    continue
                seen_tiers.add(tier)
                try:
                    sels = json.loads(r[2]) if r[2] else []
                except Exception:
                    sels = []
                accas.append({"label": r[0], "total_odds": r[1] or 0,
                              "selections": sels, "sportybet_code": r[3],
                              "result": r[4], "tier": tier})
            accas.sort(key=lambda a: order.get(a.get("tier"), 9))
        except Exception as e:
            print("[FB] day results error: {}".format(e))
        try:
            human = _dt.date.fromisoformat(date_q).strftime("%A, %B %d, %Y")
        except Exception:
            human = date_q
        return render_results_day(date_q, human, accas)

    # Calendar view
    today = _dt.date.today()
    if ym_q:
        try:
            yy, mm = ym_q.split("-")
            year, month = int(yy), int(mm)
        except Exception:
            year, month = today.year, today.month
    else:
        year, month = today.year, today.month

    day_data = {}
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT EXTRACT(DAY FROM match_date)::int, result FROM sportybet_accumulators "
            "WHERE EXTRACT(YEAR FROM match_date) = :y AND EXTRACT(MONTH FROM match_date) = :m",
            y=year, m=month)
        conn.close()
        for r in rows:
            day = int(r[0]); result = (r[1] or "pending").lower()
            d = day_data.setdefault(day, {"slips": 0, "won": 0, "lost": 0, "pending": 0})
            d["slips"] += 1
            if result == "won":
                d["won"] += 1
            elif result == "lost":
                d["lost"] += 1
            else:
                d["pending"] += 1
    except Exception as e:
        print("[FB] calendar query error: {}".format(e))

    return render_results_calendar(year, month, day_data, _fb_get_results(), today.isoformat())


@app.route("/api/telegram-webhook", methods=["POST"])
def telegram_webhook():
    try:
        update = request.get_json(force=True, silent=True) or {}
        fb_handle_telegram_update(
            update, TELEGRAM_TOKEN,
            get_crypto_signals=_fb_get_crypto_signals,
            get_sports_markets=_fb_get_sports_markets,
            get_live_bets=_fb_get_live_bets,
            get_results=_fb_get_results)
    except Exception as e:
        print("[TG] webhook error: {}".format(e))
    return jsonify({"ok": True})


@app.route("/app/run-football")
def fb_manual_run():
    """Manual trigger to run the football engine now (for testing)."""
    threading.Thread(
        target=run_football_engine,
        args=(get_db, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, send_telegram),
        daemon=True).start()
    return jsonify({"ok": True, "msg": "Football engine started — check logs and /app/codes in a minute"})


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

# ── Football v3 engine ──
try:
    fb_init_db(get_db)
except Exception as e:
    print("[FB] DB init error: {}".format(e))

# Telegram commands + webhook
try:
    tg_set_commands(TELEGRAM_TOKEN)
    _webhook_base = os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if _webhook_base:
        if not _webhook_base.startswith("http"):
            _webhook_base = "https://" + _webhook_base
        _wh = tg_set_webhook(TELEGRAM_TOKEN, _webhook_base.rstrip("/") + "/api/telegram-webhook")
        print("[TG] Webhook set to {}/api/telegram-webhook -> {}".format(_webhook_base.rstrip("/"), _wh))
    else:
        print("[TG] No WEBHOOK_BASE_URL / RAILWAY_PUBLIC_DOMAIN set — set one so /commands work")
except Exception as e:
    print("[TG] Webhook setup error: {}".format(e))

# Football scanner thread (scrape -> analyze -> build -> codes -> telegram, every 6h)
try:
    fb_scanner_thread(get_db, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, send_telegram, interval_hours=6)
except Exception as e:
    print("[FB] scanner thread error: {}".format(e))

# Football settlement thread (marks accumulators won/lost from final scores, hourly)
try:
    fb_settle_thread(get_db)
except Exception as e:
    print("[FB] settle thread error: {}".format(e))

# Football live-status thread (league + LIVE/FT + scores for the picks page)
try:
    _fb_live_refresh_thread()
except Exception as e:
    print("[FB] live-status thread error: {}".format(e))

print("[V2] All threads launched — engine running")
print("=" * 60)

send_telegram("🚀 <b>CMVNG BOT v3 STARTED</b>\n\n"
              "💰 <b>Crypto:</b> Polymarket + Limitless (1H/15M/Daily)\n"
              "⚽ <b>Football:</b> analysis engine + SportyBet codes\n"
              "📊 <b>Sports markets:</b> live scanner\n\n"
              "Commands: /picks /codes /sports /crypto /live /results\n"
              "Tap /start for the menu.")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
