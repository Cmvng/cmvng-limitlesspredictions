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
    """Return session label and whether it's safe to trade.
    AVOID: London/US crossover 12-15 UTC, Peak US 13-17 UTC, Peak Asia 23-02 UTC.
    PREFER: Late US/early Asian 17-23 UTC, Early morning 4-11 UTC."""
    if 4 <= utc_hour <= 11:
        return "EARLY_MORNING", True
    elif 17 <= utc_hour <= 22:
        return "LATE_US_ASIA", True
    elif 11 <= utc_hour < 12:
        return "PRE_LONDON", True
    elif 12 <= utc_hour <= 15:
        return "LONDON_US_CROSS", False
    elif 15 < utc_hour < 17:
        return "LATE_US", False
    elif 23 <= utc_hour or utc_hour < 2:
        return "PEAK_ASIA", False
    else:
        return "TRANSITION", True


def _v2_analyze_structure(candles):
    """Analyze HH/HL structure from intra-period candles (1m or 5m).
    Returns dict with hh_count, hl_count, lh_count, ll_count,
    grind_type ('steady'|'spike'|'choppy'), direction ('UP'|'DOWN'|'FLAT')."""

    if not candles or len(candles) < 3:
        return None

    # Find swing points using 3-bar pivots
    swing_highs = []
    swing_lows = []
    for i in range(1, len(candles) - 1):
        if candles[i]["h"] > candles[i-1]["h"] and candles[i]["h"] > candles[i+1]["h"]:
            swing_highs.append((i, candles[i]["h"]))
        if candles[i]["l"] < candles[i-1]["l"] and candles[i]["l"] < candles[i+1]["l"]:
            swing_lows.append((i, candles[i]["l"]))

    # If not enough swings, use raw highs/lows in chunks
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        chunk_size = max(1, len(candles) // 5)
        swing_highs = []
        swing_lows = []
        for i in range(0, len(candles), chunk_size):
            chunk = candles[i:i+chunk_size]
            if chunk:
                max_h = max(c["h"] for c in chunk)
                min_l = min(c["l"] for c in chunk)
                swing_highs.append((i, max_h))
                swing_lows.append((i, min_l))

    # Count HH, HL, LH, LL
    hh_count = 0
    lh_count = 0
    for i in range(1, len(swing_highs)):
        if swing_highs[i][1] > swing_highs[i-1][1]:
            hh_count += 1
        elif swing_highs[i][1] < swing_highs[i-1][1]:
            lh_count += 1

    hl_count = 0
    ll_count = 0
    for i in range(1, len(swing_lows)):
        if swing_lows[i][1] > swing_lows[i-1][1]:
            hl_count += 1
        elif swing_lows[i][1] < swing_lows[i-1][1]:
            ll_count += 1

    # Grind analysis — steady vs spike
    # Calculate per-candle moves
    moves = []
    for i in range(1, len(candles)):
        move = abs(candles[i]["c"] - candles[i-1]["c"])
        moves.append(move)

    avg_move = sum(moves) / len(moves) if moves else 0
    max_move = max(moves) if moves else 0

    if avg_move > 0 and max_move > avg_move * 2.5:
        grind_type = "spike"
    elif avg_move > 0 and max_move < avg_move * 1.5:
        grind_type = "steady"
    else:
        grind_type = "choppy"

    # Direction determination
    if hh_count >= 2 and hl_count >= 2 and lh_count == 0 and ll_count == 0:
        direction = "UP"
    elif ll_count >= 2 and lh_count >= 2 and hh_count == 0 and hl_count == 0:
        direction = "DOWN"
    elif hh_count >= 2 and hl_count >= 1:
        direction = "UP"
    elif ll_count >= 2 and lh_count >= 1:
        direction = "DOWN"
    else:
        direction = "FLAT"

    return {
        "hh_count": hh_count, "hl_count": hl_count,
        "lh_count": lh_count, "ll_count": ll_count,
        "grind_type": grind_type, "direction": direction,
        "swing_highs": swing_highs, "swing_lows": swing_lows,
    }


def _v2_analyze_prev_candle(candle):
    """Analyze previous period's candle for strength/direction.
    Returns dict with color, body_pct, close_position, wick_ratios."""
    if not candle:
        return None
    o, h, l, c = candle["o"], candle["h"], candle["l"], candle["c"]
    rng = max(h - l, 0.0001)
    body = abs(c - o)
    body_pct = body / rng
    close_pos = (c - l) / rng  # 0 = closed at low, 1 = closed at high
    green = c > o
    upper_wick = (h - max(o, c)) / rng
    lower_wick = (min(o, c) - l) / rng

    # Strength assessment
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
    """Check if current volatility is normal or excessive.
    Returns (label, is_safe) — 'normal'|'high'|'extreme'."""
    if not candles or len(candles) < 5:
        return "unknown", True

    # ATR of recent candles
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
    elif ratio > 1.3:
        return "elevated", True
    else:
        return "normal", True


def _v2_ptb_distance(price, ptb, asset):
    """Calculate distance from PTB as percentage.
    Returns (pct_distance, direction, is_meaningful)."""
    if not price or not ptb or ptb <= 0:
        return 0, "NONE", False

    pct = ((price - ptb) / ptb) * 100
    direction = "ABOVE" if pct > 0 else "BELOW" if pct < 0 else "AT"

    # Minimum meaningful distance varies by asset
    min_dist = {
        "BTC": 0.05, "ETH": 0.08, "SOL": 0.10,
        "XRP": 0.15, "DOGE": 0.20, "BNB": 0.10,
    }.get(asset, 0.10)

    is_meaningful = abs(pct) > min_dist
    return round(pct, 4), direction, is_meaningful


def _v2_should_enter(structure, prev_candle, volatility_label, vol_safe,
                     session_safe, ptb_meaningful, ptb_direction, grind_type,
                     timeframe="1H"):
    """Master entry decision. Returns (should_trade, direction, confidence, reason)."""

    # Hard filters — SKIP if any fail
    if not session_safe:
        return False, None, 0, "Volatile session — skip"

    if not vol_safe:
        return False, None, 0, "Volatility too high — skip"

    if grind_type == "spike":
        return False, None, 0, "Spike detected — reversal risk"

    if not structure:
        return False, None, 0, "No structure data"

    if not prev_candle:
        return False, None, 0, "No previous candle data"

    # Direction from structure
    struct_dir = structure["direction"]
    if struct_dir == "FLAT":
        return False, None, 0, "No clear direction — choppy"

    # Validate structure + prev candle alignment
    hh = structure["hh_count"]
    hl = structure["hl_count"]
    lh = structure["lh_count"]
    ll = structure["ll_count"]

    if struct_dir == "UP":
        # Bullish: need HH+HL, previous candle green/strong, price above PTB
        if hh < 2 or hl < 1:
            return False, None, 0, "Weak bullish structure (HH={} HL={})".format(hh, hl)

        if prev_candle["strength"] in ("STRONG_BEAR", "MILD_BEAR"):
            # Previous red but structure up = possible reversal continuation — lower confidence
            if hh >= 3 and hl >= 3:
                confidence = 60
                reason = "Structure UP despite prev red — strong HH/HL overrides"
            else:
                return False, None, 0, "Prev candle bearish, structure not strong enough"
        elif prev_candle["strength"] == "STRONG_BULL":
            confidence = 90
            reason = "Prev strong green + clean HH/HL structure"
        elif prev_candle["strength"] == "MILD_BULL":
            confidence = 75
            reason = "Prev mild green + HH/HL structure"
        elif prev_candle["strength"] == "DOJI":
            confidence = 55
            reason = "Prev doji + structure UP — cautious"
        else:
            confidence = 70
            reason = "Structure UP + prev candle aligned"

        # PTB must be above for UP trades
        if ptb_meaningful and ptb_direction == "BELOW":
            confidence -= 15
            reason += " | Price below PTB — risky"
        elif ptb_meaningful and ptb_direction == "ABOVE":
            confidence += 5
            reason += " | Price above PTB — good"

        # Grind bonus
        if grind_type == "steady":
            confidence += 5
            reason += " | Steady grind"

        return confidence >= 60, "UP", confidence, reason

    elif struct_dir == "DOWN":
        if ll < 2 or lh < 1:
            return False, None, 0, "Weak bearish structure (LL={} LH={})".format(ll, lh)

        if prev_candle["strength"] in ("STRONG_BULL", "MILD_BULL"):
            if ll >= 3 and lh >= 3:
                confidence = 60
                reason = "Structure DOWN despite prev green — strong LL/LH overrides"
            else:
                return False, None, 0, "Prev candle bullish, structure not strong enough"
        elif prev_candle["strength"] == "STRONG_BEAR":
            confidence = 90
            reason = "Prev strong red + clean LL/LH structure"
        elif prev_candle["strength"] == "MILD_BEAR":
            confidence = 75
            reason = "Prev mild red + LL/LH structure"
        elif prev_candle["strength"] == "DOJI":
            confidence = 55
            reason = "Prev doji + structure DOWN — cautious"
        else:
            confidence = 70
            reason = "Structure DOWN + prev candle aligned"

        if ptb_meaningful and ptb_direction == "ABOVE":
            confidence -= 15
            reason += " | Price above PTB — risky for DOWN"
        elif ptb_meaningful and ptb_direction == "BELOW":
            confidence += 5
            reason += " | Price below PTB — good for DOWN"

        if grind_type == "steady":
            confidence += 5
            reason += " | Steady grind"

        return confidence >= 60, "DOWN", confidence, reason

    return False, None, 0, "No clear signal"


def _v2_build_entry_note(asset, timeframe, direction, prev_candle, structure,
                         ptb, price, session_label, volatility_label, confidence):
    """Build human-readable entry note for paper trade record."""
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

    now_utc = datetime.now(timezone.utc)
    return "{} {} {} | {} | {} | {} | Session: {} ({}h UTC) | Vol: {} | Conf: {}".format(
        timeframe, asset, direction,
        prev_str, struct_str, ptb_str,
        session_label, now_utc.hour, volatility_label, confidence)


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
        # Limitless uses market slugs like /markets/{slug}
        slug = market_data.get("slug", "") if market_data else ""
        if slug:
            return "https://limitless.exchange/markets/{}".format(slug)
        return "https://limitless.exchange"
    return ""


# ═══════════════════════════════════════════════════════════
# V2 HEDGE LOGIC
# ═══════════════════════════════════════════════════════════

def _v2_check_hedge(trade, current_structure):
    """Check if an open trade should be hedged.
    Hedge when structure breaks: trend was UP but now seeing LH+LL."""
    if not trade or not current_structure:
        return False, None

    direction = trade.get("direction")
    if direction == "UP":
        # Structure break: lower highs + lower lows forming
        if current_structure["lh_count"] >= 1 and current_structure["ll_count"] >= 1:
            return True, "Structure break — LH/LL forming against UP position"
    elif direction == "DOWN":
        if current_structure["hh_count"] >= 1 and current_structure["hl_count"] >= 1:
            return True, "Structure break — HH/HL forming against DOWN position"

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
    NOTE: get_order_book() is BROKEN (returns stale 0.01/0.99 — GitHub Issue #180).
    get_price(token_id, side="BUY") returns the correct live ask price.
    Returns odds as percentage (e.g. 72.0 for 72c) or None."""
    client = _get_poly_client()
    if not client or not market_data:
        return None

    try:
        token = market_data.get("up_token") if direction == "UP" else market_data.get("down_token")
        if not token:
            return None

        # get_price returns the best available price for the given side
        # BUY side = best ask (price to buy), SELL side = best bid (price to sell)
        buy_price = None
        sell_price = None

        try:
            buy_result = client.get_price(str(token), side="BUY")
            if buy_result:
                if isinstance(buy_result, dict):
                    buy_price = float(buy_result.get("price", 0))
                elif isinstance(buy_result, (int, float, str)):
                    buy_price = float(buy_result)
        except Exception as e:
            print("[V2] POLY get_price BUY error: {}".format(e))

        try:
            sell_result = client.get_price(str(token), side="SELL")
            if sell_result:
                if isinstance(sell_result, dict):
                    sell_price = float(sell_result.get("price", 0))
                elif isinstance(sell_result, (int, float, str)):
                    sell_price = float(sell_result)
        except:
            pass

        # Also get midpoint for reference
        mid = None
        try:
            mid_result = client.get_midpoint(str(token))
            if mid_result:
                if isinstance(mid_result, dict):
                    mid = float(mid_result.get("mid", 0))
                elif isinstance(mid_result, (int, float, str)):
                    mid = float(mid_result)
        except:
            pass

        # Log for debugging
        asset = market_data.get("asset", "?")
        tf = market_data.get("timeframe", "?")
        slug = market_data.get("slug", "?")[:40]
        tok_snippet = str(token)[:12] + "..." + str(token)[-6:]
        print("[V2] POLY PRICE {} {} {} | slug={} | tok={} | buy={} sell={} mid={}".format(
            asset, tf, direction, slug, tok_snippet,
            "{:.4f}".format(buy_price) if buy_price else "None",
            "{:.4f}".format(sell_price) if sell_price else "None",
            "{:.4f}".format(mid) if mid else "None"))

        # Return the BUY price (best ask) as percentage
        if buy_price and 0.01 <= buy_price <= 0.99:
            return round(buy_price * 100, 1)

        # Fallback to midpoint
        if mid and 0.01 <= mid <= 0.99:
            return round(mid * 100, 1)

        # Fallback to sell price (best bid)
        if sell_price and 0.01 <= sell_price <= 0.99:
            return round(sell_price * 100, 1)

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
    """Calculate limit order price based on book state and confidence.
    We place a limit slightly below the ask — any small dip fills us.

    At high odds (90c+): limit just 0.5-1.5c below ask — queue for any dip
    At medium odds (70-90c): limit 2-5c below ask — want some discount
    At low odds (<70c): limit 5-8c below — need real edge

    Returns (limit_price_cents, should_place) or (None, False)."""

    if not book_ask or book_ask <= 0:
        return None, False

    # Below 50c means the market favors the other side — skip
    if book_ask < 50:
        return None, False

    # High odds (90c+): practically confirmed, just queue slightly below
    if book_ask >= 90:
        # At 99c → limit 97.5c. At 93c → limit 91.5c. At 90c → limit 88.5c.
        limit = book_ask - 1.5
        if confidence >= 85:
            limit = book_ask - 0.5  # Very confident, barely undercut
        elif confidence >= 75:
            limit = book_ask - 1.0

    # Medium odds (70-90c): want a small discount
    elif book_ask >= 70:
        limit = book_ask - 3
        if confidence >= 85:
            limit = book_ask - 2
        elif confidence >= 75:
            limit = book_ask - 2.5

    # Lower odds (50-70c): want meaningful discount
    else:
        limit = book_ask - 5
        if confidence >= 85:
            limit = book_ask - 3
        elif confidence >= 75:
            limit = book_ask - 4

    # Floor at 50c — below that there's no real edge
    limit = max(50, limit)

    return round(limit, 1), True


# ═══════════════════════════════════════════════════════════
# V2 RESOLUTION — Check outcomes of paper trades
# ═══════════════════════════════════════════════════════════

def _v2_resolve_trades():
    """Check all OPEN paper trades and resolve them if the market has closed."""
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

            # Get close price from Chainlink or Binance
            asset = t["asset"]
            close_price = _get_binance_price(asset)
            if not close_price:
                continue

            ptb = t.get("ptb")
            if not ptb or ptb <= 0:
                continue

            # Determine actual result
            if close_price > ptb:
                actual = "UP"
            elif close_price < ptb:
                actual = "DOWN"
            else:
                actual = "FLAT"

            direction = t["direction"]
            entry_odds = t.get("entry_odds", 50) or 50
            stake = t.get("stake", 2.50) or 2.50

            # Calculate P&L
            if actual == direction:
                # Won: payout = stake / (odds/100) - stake
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
                hedge_stake = stake * 0.5  # hedge at half stake
                if actual == hedge_dir:
                    hedge_pnl = (hedge_stake / (hedge_odds / 100)) - hedge_stake
                else:
                    hedge_pnl = -hedge_stake
                pnl += hedge_pnl
            else:
                hedge_pnl = 0

            # Update balance
            platform = t["platform"]
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
                """, cp=close_price, ar=actual, oc=outcome,
                    pnl=round(pnl, 4), bal=round(bal["balance"], 2),
                    hpnl=round(hedge_pnl, 4) if hedge_pnl else None,
                    st="RESOLVED", tid=t["id"])
                conn2.close()
                resolved += 1
            except Exception as e:
                print("[V2] Resolve update error: {}".format(e))

            # Telegram notification
            emoji = "✅" if outcome == "WIN" else "❌"
            send_telegram("{} V2 {} {} {} {} @ {:.0f}c → {} | P&L ${:+.2f} | Bal ${:.2f}".format(
                emoji, t["timeframe"], asset, direction,
                t["platform"][:4].upper(), entry_odds, outcome,
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
FLAT_STAKE = 3.00  # $3 flat per confirmed entry

def _v2_scan_timeframe(timeframe):
    """Core scanning logic shared by 1H, 15M, and DAILY watchers.
    Scans BOTH Polymarket and Limitless. Enters whenever confidence is high, at ANY odds."""

    ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB"]
    tf_label = timeframe

    # Timeframe-specific config
    if tf_label == "1H":
        intra_interval = "5m"
        prev_interval = "1h"
        min_intra_candles = 3
        min_confidence = 60
        boundary_secs = 3600
        poly_tf_filter = "1H"
        scan_sleep = 120
    elif tf_label == "15M":
        intra_interval = "1m"
        prev_interval = "15m"
        min_intra_candles = 3
        min_confidence = 75
        boundary_secs = 900
        poly_tf_filter = "15M"
        scan_sleep = 60
    else:  # DAILY
        intra_interval = "1h"
        prev_interval = "1d"
        min_intra_candles = 3
        min_confidence = 60
        boundary_secs = 86400
        poly_tf_filter = "DAILY"
        scan_sleep = 600  # Check every 10 minutes

    while True:
        try:
            now = datetime.now(timezone.utc)
            now_ts = int(now.timestamp())

            if tf_label == "DAILY":
                # Daily boundary = midnight UTC
                boundary_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                secs_into_period = now_ts - boundary_ts
                # Need at least 3 hours of data for daily
                if secs_into_period < 10800:
                    time.sleep(max(60, 10800 - secs_into_period))
                    continue
            else:
                boundary_ts = (now_ts // boundary_secs) * boundary_secs
                secs_into_period = now_ts - boundary_ts
                min_secs = 300 if tf_label == "1H" else 120
                if secs_into_period < min_secs:
                    time.sleep(min_secs - secs_into_period + 5)
                    continue
                remaining = boundary_secs - secs_into_period
                if remaining < 60:
                    time.sleep(remaining + 5)
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
                        continue

                    # Filter to THIS period only
                    period_candles = [c for c in intra_candles if c["t"] >= boundary_ts * 1000]
                    if len(period_candles) < min_intra_candles:
                        continue

                    # Previous completed candle
                    prev_candles = _fetch_binance_candles(asset, interval=prev_interval, limit=5)
                    if not prev_candles or len(prev_candles) < 2:
                        continue
                    prev_candle = _v2_analyze_prev_candle(prev_candles[-2])

                    # Structure analysis
                    structure = _v2_analyze_structure(period_candles)

                    # Volatility
                    current_range = max(c["h"] for c in period_candles) - min(c["l"] for c in period_candles)
                    vol_label, vol_safe = _v2_volatility_check(prev_candles[:-1], current_range)

                    # PTB distance
                    price = _get_binance_price(asset)
                    ptb = None
                    if market_data and market_data.get("baseline"):
                        ptb = market_data["baseline"]
                    elif market_data and platform == "polymarket":
                        ptb = _poly_get_baseline(market_data, price)
                    elif platform == "limitless" and market_data:
                        ptb = market_data.get("baseline")
                    # Fallback: use period open price
                    if not ptb and period_candles:
                        ptb = period_candles[0]["o"]

                    ptb_pct, ptb_dir, ptb_meaningful = _v2_ptb_distance(price, ptb, asset) if ptb else (0, "NONE", False)

                    # Entry decision
                    should, direction, confidence, reason = _v2_should_enter(
                        structure, prev_candle, vol_label, vol_safe,
                        session_safe if tf_label != "DAILY" else True,  # Daily ignores session
                        ptb_meaningful, ptb_dir,
                        structure["grind_type"] if structure else "unknown",
                        timeframe=tf_label
                    )

                    if not should or not confidence or confidence < min_confidence:
                        continue

                    # Get REAL book ask from order book
                    book_ask = _v2_get_odds(platform, market_data, direction)

                    # Calculate limit price — we don't buy at the ask
                    if book_ask:
                        limit_price, should_place = _v2_calc_limit_price(book_ask, confidence)
                        if not should_place:
                            print("[V2] {} {} {} — book_ask={:.0f}c, no edge at conf {}".format(
                                tf_label, asset, direction, book_ask, confidence))
                            continue
                    else:
                        # No book data — place at confidence-based estimate
                        limit_price = max(65, min(85, 60 + confidence * 0.25))
                        book_ask = None

                    # Record as PENDING limit order — the fill checker will
                    # check each cycle if the ask has come down to our limit
                    entry_odds = limit_price  # Our limit IS our entry price

                    # Build entry note
                    prev_str = "{} body={:.0f}%".format(
                        prev_candle["strength"], prev_candle["body_pct"] * 100) if prev_candle else ""
                    note = _v2_build_entry_note(
                        asset, tf_label, direction, prev_candle, structure,
                        ptb, price, session_label, vol_label, confidence)
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

                    url_str = "\n🔗 {}".format(market_url) if market_url else ""
                    send_telegram(
                        "📋 V2 LIMIT {} {} {} {} @ {:.0f}c (book {:.0f}c)\n"
                        "Conf {} | ${:.2f} | PENDING fill{}".format(
                            platform[:4].upper(), tf_label, asset, direction,
                            limit_price, book_ask or 0, confidence,
                            FLAT_STAKE, url_str))

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
                should_hedge, hedge_reason = _v2_check_hedge(t, structure)

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
                    continue

                limit = o.get("limit_price", 0) or 0

                # FILL if current ask <= our limit price
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
# STARTUP
# ═══════════════════════════════════════════════════════════

print("=" * 60)
print("CMVNG BOT v2 — CONFIRMATION TRADING ENGINE")
print("=" * 60)

try:
    init_db()
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
threading.Thread(target=_v2_monitor_thread, daemon=True, name="v2-monitor").start()
threading.Thread(target=_v2_resolve_loop, daemon=True, name="v2-resolve").start()
threading.Thread(target=_v2_fill_checker, daemon=True, name="v2-fills").start()
threading.Thread(target=_v2_cleanup_loop, daemon=True, name="v2-cleanup").start()

print("[V2] All threads launched — engine running")
print("=" * 60)

send_telegram("🚀 <b>CMVNG BOT v2 STARTED</b>\nConfirmation Trading Engine\nMode: PAPER | $50/platform | $3/trade\nWatchers: 1H + 15M + DAILY\nPlatforms: Polymarket + Limitless")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
