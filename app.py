from flask import Flask, request, jsonify, render_template_string
import pg8000.native
import os
import re
import threading
import time
import json
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
ANTHROPIC_KEY    = os.environ.get("ANTHROPIC_API_KEY", "")
FOOTBALL_DATA_KEY = os.environ.get("FOOTBALL_DATA_KEY", "")

# Auto-trading credentials (HMAC auth + EIP-712 signing)
LIMITLESS_TOKEN_ID     = os.environ.get("LIMITLESS_TOKEN_ID", "")      # from Derive Token
LIMITLESS_TOKEN_SECRET = os.environ.get("LIMITLESS_TOKEN_SECRET", "")  # from Derive Token (one-time)
LIMITLESS_PRIV_KEY     = os.environ.get("LIMITLESS_PRIVATE_KEY", "")   # MetaMask private key 0x...

LAGOS_TZ      = timezone(timedelta(hours=1))
LIMITLESS_API = "https://api.limitless.exchange"

# Global BTC trend cache
_btc_trend_cache = {"trend": None, "price": None, "sma10": None, "updated": None}
# Debug log for last scan
_last_scan_log = {"time": None, "total": 0, "qualified": 0, "filtered": []}

# ═══════════════════════════════════════════════════════════
# AUTO-TRADING STATE
# ═══════════════════════════════════════════════════════════
_trading_state = {
    "enabled": True,             # Kill switch — set False to stop all trades
    "daily_loss": 0.0,           # Accumulated losses today (USDC)
    "daily_profit": 0.0,         # Accumulated profits today
    "trades_today": 0,           # Number of trades placed today
    "last_reset": None,          # When daily counters last reset
    "last_balance": None,        # Cached balance
    "high_pct": 0.15,            # 15% of balance on HIGH confidence
    "medium_pct": 0.08,          # 8% of balance on MEDIUM confidence
    "daily_loss_limit_pct": 0.25,# Stop after 25% daily loss
    "min_stake": 1.0,            # Limitless minimum $1
    "starting_balance": 20.0,    # Manual fallback balance
}

FAVOURITE_HOURLY = ["ADA", "BNB", "HYPE"]

YAHOO_MAP = {
    "BTC":"BTC-USD",  "ETH":"ETH-USD",  "SOL":"SOL-USD",
    "ADA":"ADA-USD",  "BNB":"BNB-USD",  "DOGE":"DOGE-USD",
    "XRP":"XRP-USD",  "AVAX":"AVAX-USD","LINK":"LINK-USD",
    "LTC":"LTC-USD",  "BCH":"BCH-USD",  "XLM":"XLM-USD",
    "HYPE":"HYPE-USD","SUI":"SUI-USD",  "ZEC":"ZEC-USD",
    "XMR":"XMR-USD",  "ONDO":"ONDO-USD","MNT":"MNT-USD",
    "DOT":"DOT-USD",  "UNI":"UNI-USD",  "ATOM":"ATOM-USD",
    "TRX":"TRX-USD",  "APT":"APT-USD",  "ARB":"ARB-USD",
    "OP":"OP-USD",    "NEAR":"NEAR-USD","TON":"TON-USD",
}

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
    conn.run("""
        CREATE TABLE IF NOT EXISTS limitless_predictions (
            id            SERIAL PRIMARY KEY,
            market_id     TEXT,
            title         TEXT,
            asset         TEXT,
            direction     TEXT,
            baseline      REAL,
            bet_odds      REAL,
            confidence    TEXT,
            size_rec      TEXT,
            current_price REAL,
            hours_left    REAL,
            market_type   TEXT,
            status        TEXT DEFAULT 'Pending',
            outcome       TEXT,
            fired_at      TEXT,
            resolved_at   TEXT,
            slug          TEXT,
            bet_side      TEXT DEFAULT 'YES'
        )
    """)
    conn.run("""
        CREATE TABLE IF NOT EXISTS football_picks (
            id              SERIAL PRIMARY KEY,
            match_id        TEXT,
            home_team       TEXT,
            away_team       TEXT,
            competition     TEXT,
            kickoff_time    TEXT,
            pick_type       TEXT,
            pick_value      TEXT,
            confidence      REAL,
            reasoning       TEXT,
            implied_odds    REAL,
            accumulator_tier TEXT,
            status          TEXT DEFAULT 'Pending',
            outcome         TEXT,
            fired_at        TEXT,
            resolved_at     TEXT
        )
    """)
    # Add resolved_at if table already exists without it
    try:
        conn.run("ALTER TABLE football_picks ADD COLUMN IF NOT EXISTS resolved_at TEXT")
    except:
        pass
    # Add bet_side column to existing limitless_predictions table
    try:
        conn.run("ALTER TABLE limitless_predictions ADD COLUMN IF NOT EXISTS bet_side TEXT DEFAULT 'YES'")
    except:
        pass
    conn.close()
    print("DB initialized OK")

# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        import requests
        requests.post(
            "https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_TOKEN),
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print("Telegram error: {}".format(e))

# ═══════════════════════════════════════════════════════════
# YAHOO FINANCE
# ═══════════════════════════════════════════════════════════

def get_price(asset):
    import yfinance as yf
    symbol = YAHOO_MAP.get(asset.upper())
    if not symbol:
        return None
    try:
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
        return None
    except Exception as e:
        print("yfinance error {}: {}".format(asset, e))
        return None

def get_btc_trend():
    import yfinance as yf
    try:
        btc = yf.Ticker("BTC-USD")
        hist = btc.history(period="2d", interval="1h")
        if hist.empty or len(hist) < 10:
            return _btc_trend_cache.get("trend")
        closes = hist["Close"].tolist()
        current = closes[-1]
        sma10 = sum(closes[-10:]) / 10
        trend = "BUY" if current > sma10 else "SELL"
        _btc_trend_cache["trend"] = trend
        _btc_trend_cache["price"] = current
        _btc_trend_cache["sma10"] = sma10
        _btc_trend_cache["updated"] = datetime.now(timezone.utc).isoformat()
        print("BTC: {} price={:.0f} sma10={:.0f}".format(trend, current, sma10))
        return trend
    except Exception as e:
        print("BTC trend error: {}".format(e))
        return _btc_trend_cache.get("trend")

# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def is_lagos_window():
    hour = datetime.now(LAGOS_TZ).hour
    return (5 <= hour < 12) or (18 <= hour < 24)

def fmt_price(v):
    if v is None:
        return "-"
    try:
        v = float(v)
        return "${:,.4f}".format(v) if v < 100 else "${:,.2f}".format(v)
    except:
        return "-"

# ═══════════════════════════════════════════════════════════
# PARSE LIMITLESS MARKET
# ═══════════════════════════════════════════════════════════

def parse_market(market):
    title = market.get("title", "")
    m = re.search(r'(?:\$)?([A-Z]+)\s+(above|below)\s+\$([\d,]+\.?\d*)', title)
    if not m:
        return None

    asset     = m.group(1)
    direction = m.group(2)
    baseline  = float(m.group(3).replace(",", ""))

    exp_ts = market.get("expirationTimestamp", 0)
    if not exp_ts:
        return None

    expiry_dt  = datetime.fromtimestamp(exp_ts / 1000, tz=timezone.utc)
    now        = datetime.now(timezone.utc)
    mins_left  = (expiry_dt - now).total_seconds() / 60
    hours_left = mins_left / 60

    if mins_left <= 0:
        return None

    # ── FIX: Limitless returns prices as 0-1 decimals, convert to % ──
    prices = market.get("prices", [0.5, 0.5])
    yes_raw = float(prices[0]) if prices else 0.5
    # Auto-detect: if value > 1, already in %, else multiply by 100
    if yes_raw > 1:
        yes_odds = yes_raw
    else:
        yes_odds = yes_raw * 100

    tags = market.get("tags", [])
    cats = market.get("categories", [])
    # ── FIX: detect hourly vs daily via tags/categories ──
    # "Minutely", "Minutes 15", "Hourly" = short-term; else daily
    is_short = any(t in tags or t in cats for t in
                   ["Minutely", "Minutes 15", "Minutes 30", "Minutes 5", "Hourly", "15 min", "30 min"])
    is_daily = not is_short

    return {
        "market_id":  str(market.get("id", "")),
        "title":      title,
        "asset":      asset,
        "direction":  direction,
        "baseline":   baseline,
        "expiry_dt":  expiry_dt,
        "mins_left":  mins_left,
        "hours_left": hours_left,
        "yes_odds":   yes_odds,
        "is_short":   is_short,
        "is_daily":   is_daily,
        "slug":       market.get("slug", ""),
    }

# ═══════════════════════════════════════════════════════════
# SCORE MARKET
# ═══════════════════════════════════════════════════════════

def score_market(p, btc_trend, price, debug_log=None):
    def reject(reason):
        if debug_log is not None:
            debug_log.append({
                "asset": p["asset"], "title": p["title"][:60],
                "odds": p["yes_odds"], "hrs": p["hours_left"],
                "reason": reason
            })
        return None

    is_fav = p["asset"] in FAVOURITE_HOURLY

    # 1. Time window — skip if auto-trading is off AND outside Lagos window
    #    When auto-trading is on, scan 24/7
    auto_trading_on = bool(_has_trading_keys() and _trading_state.get("enabled"))
    if not auto_trading_on and not is_lagos_window() and not is_fav:
        return reject("outside Lagos window (auto-trade off)")

    # 2. Expiry filter
    if p["is_short"]:
        if not is_fav and not (5 <= p["mins_left"] <= 30):
            return reject("short-term not in 5-30 min window (got {:.0f} mins)".format(p["mins_left"]))
    else:
        if p["hours_left"] < 0.5:
            return reject("daily too close to expiry ({:.1f}h)".format(p["hours_left"]))
        if p["hours_left"] > 10 and not is_fav:
            return reject("daily too far out ({:.1f}h)".format(p["hours_left"]))

    # 3. Price must exist
    if price is None:
        return reject("no Yahoo price for {}".format(p["asset"]))

    # 4. Calculate margin
    margin = abs(price - p["baseline"])
    margin_pct = (margin / p["baseline"] * 100) if p["baseline"] > 0 else 0

    # 5. Determine margin thresholds based on timeframe
    #    Shorter timeframes need less margin because price has less time to move
    if p["is_short"] and p["mins_left"] <= 30:
        margin_thresh_aligned = 0.05   # 0.05% when trend helps (~$42 on BTC)
        margin_thresh_against = 0.15   # 0.15% when trend fights (~$126 on BTC)
    elif p["hours_left"] <= 2:
        margin_thresh_aligned = 0.15   # 0.15% (~$2.43 on ETH)
        margin_thresh_against = 0.4    # 0.4% (~$6.48 on ETH)
    else:
        margin_thresh_aligned = 0.5
        margin_thresh_against = 2.0

    # 6. Determine bet side: YES or NO
    yes_odds = p["yes_odds"]
    no_odds = 100 - yes_odds

    if p["direction"] == "above":
        price_is_above = price > p["baseline"]
        price_is_below = price < p["baseline"]
    else:
        # "below" market: price_is_above means price IS below baseline (winning side for YES)
        price_is_above = price < p["baseline"]
        price_is_below = price > p["baseline"]

    bet_side = None
    effective_odds = None

    if price_is_above:
        # Price is on YES side — check YES odds
        if 73 <= yes_odds <= 99:
            bet_side = "YES"
            effective_odds = yes_odds
            # BTC alignment for YES
            if p["direction"] == "above":
                btc_aligned = (btc_trend == "BUY") if btc_trend else True
            else:
                btc_aligned = (btc_trend == "SELL") if btc_trend else True
        else:
            # YES odds out of range — maybe NO qualifies?
            pass

    if bet_side is None and price_is_below:
        # Price is on NO side — check NO odds
        if 73 <= no_odds <= 99:
            bet_side = "NO"
            effective_odds = no_odds
            # BTC alignment for NO (opposite direction)
            if p["direction"] == "above":
                btc_aligned = (btc_trend == "SELL") if btc_trend else True
            else:
                btc_aligned = (btc_trend == "BUY") if btc_trend else True
        else:
            return reject("NO odds {:.1f}% outside 73-99% range".format(no_odds))

    if bet_side is None:
        if price_is_above:
            return reject("YES odds {:.1f}% outside 73-99% range".format(yes_odds))
        else:
            return reject("price on wrong side and NO odds {:.1f}% outside range".format(no_odds))

    # 7. Margin safety check
    if not btc_aligned and btc_trend:
        if margin_pct < margin_thresh_against:
            return reject("{} margin {:.2f}% < {:.1f}% threshold (trend against)".format(
                bet_side, margin_pct, margin_thresh_against))
    else:
        if margin_pct < margin_thresh_aligned:
            return reject("{} margin {:.2f}% < {:.1f}% threshold (even aligned)".format(
                bet_side, margin_pct, margin_thresh_aligned))

    # 8. Confidence
    if not btc_aligned and btc_trend:
        confidence = "MEDIUM"
    elif effective_odds >= 90 or (effective_odds >= 80 and btc_aligned):
        confidence = "HIGH"
    else:
        confidence = "MEDIUM"

    # 9. Size recommendation
    if effective_odds >= 94:
        size_rec = "$20-50 (high odds — go with size)"
    elif effective_odds >= 85:
        size_rec = "$10-20 (normal size)"
    else:
        size_rec = "$5-10 (cautious)"

    # 10. Reversal warning for short-term tight margins
    reversal = ""
    if p["is_short"] and p["mins_left"] <= 60 and 78 <= effective_odds <= 88:
        reversal = "⚠️ Reversal risk — watch carefully"

    return {
        "bet_side":    bet_side,
        "bet_odds":    effective_odds,
        "confidence":  confidence,
        "size_rec":    size_rec,
        "margin":      margin,
        "margin_pct":  margin_pct,
        "reversal":    reversal,
        "btc_aligned": btc_aligned,
    }

# ═══════════════════════════════════════════════════════════
# SAVE AND ALERT
# ═══════════════════════════════════════════════════════════

def save_and_alert(p, score, price, btc_trend):
    try:
        now  = datetime.now(timezone.utc).isoformat()
        bet_side = score.get("bet_side", "YES")
        conn = get_db()
        rows = conn.run(
            """INSERT INTO limitless_predictions
            (market_id,title,asset,direction,baseline,bet_odds,confidence,
             size_rec,current_price,hours_left,market_type,status,fired_at,slug,bet_side)
            VALUES (:mid,:ttl,:ast,:dir,:base,:odds,:conf,:sz,:pr,:hrs,:mt,'Pending',:now,:slg,:bs)
            RETURNING id""",
            mid=p["market_id"], ttl=p["title"], ast=p["asset"],
            dir=p["direction"], base=p["baseline"],
            odds=score["bet_odds"], conf=score["confidence"], sz=score["size_rec"],
            pr=price, hrs=round(p["hours_left"], 2),
            mt="Short" if p["is_short"] else "Daily",
            now=now, slg=p["slug"], bs=bet_side
        )
        pid = rows[0][0]
        conn.close()

        trend_str  = "🟢 Bullish" if btc_trend == "BUY" else "🔴 Bearish" if btc_trend == "SELL" else "⚪ Unknown"
        conf_emoji = "🔥" if score["confidence"] == "HIGH" else "🟡"
        hrs_str    = "{:.1f} hrs".format(p["hours_left"]) if p["hours_left"] >= 1 else "{:.0f} mins".format(p["mins_left"])
        exp_str    = p["expiry_dt"].strftime("%d %b %H:%M UTC")

        # Bet side display
        if bet_side == "YES":
            side_str = "YES ✅"
            margin_dir = p["direction"]
        else:
            side_str = "NO 🔻"
            margin_dir = "below" if p["direction"] == "above" else "above"

        msg = (
            "🎯 <b>PREDICTION #{}</b>\n"
            "──────────────────────────\n"
            "📌 {}\n"
            "──────────────────────────\n"
            "<b>Bet:</b> {}\n"
            "<b>Odds:</b> {:.1f}% chance\n"
            "<b>Current Price:</b> {}\n"
            "<b>Baseline:</b> {}\n"
            "<b>Margin {} baseline:</b> {} ({:.2f}%)\n"
            "<b>Time Left:</b> {}\n"
            "<b>Expires:</b> {}\n"
            "<b>Type:</b> {}\n"
            "──────────────────────────\n"
            "{} <b>Confidence:</b> {}\n"
            "💰 <b>Size:</b> {}\n"
            "📊 <b>BTC:</b> {}\n"
            "{}"
            "🔗 limitless.exchange/markets/{}"
        ).format(
            pid, p["title"],
            side_str,
            score["bet_odds"],
            fmt_price(price), fmt_price(p["baseline"]),
            margin_dir, fmt_price(score["margin"]), score.get("margin_pct", 0),
            hrs_str, exp_str,
            "Short ⏱" if p["is_short"] else "Daily 📅",
            conf_emoji, score["confidence"],
            score["size_rec"], trend_str,
            score["reversal"] + "\n" if score["reversal"] else "",
            p["slug"]
        )
        send_telegram(msg)
        print("ALERT #{}: {} {} at {:.1f}%".format(pid, bet_side, p["title"][:50], score["bet_odds"]))

        # Auto-trade if enabled
        if _has_trading_keys():
            try:
                execute_trade(p, score, pid)
            except Exception as te:
                print("Auto-trade error for #{}: {}".format(pid, te))

    except Exception as e:
        print("Alert error: {}".format(e))

# ═══════════════════════════════════════════════════════════
# HMAC AUTHENTICATION FOR LIMITLESS API
# ═══════════════════════════════════════════════════════════

def _hmac_headers(method, path, body=""):
    """Build HMAC-signed headers for Limitless API requests."""
    import hmac as hmac_mod, hashlib, base64
    timestamp = datetime.now(timezone.utc).isoformat()
    message = "{}\n{}\n{}\n{}".format(timestamp, method, path, body)
    signature = base64.b64encode(
        hmac_mod.new(
            base64.b64decode(LIMITLESS_TOKEN_SECRET),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")
    return {
        "lmts-api-key": LIMITLESS_TOKEN_ID,
        "lmts-timestamp": timestamp,
        "lmts-signature": signature,
        "Content-Type": "application/json",
    }

def _has_trading_keys():
    """Check if all 3 trading credentials are set."""
    return bool(LIMITLESS_TOKEN_ID and LIMITLESS_TOKEN_SECRET and LIMITLESS_PRIV_KEY)

# ═══════════════════════════════════════════════════════════
# AUTO-TRADING ENGINE
# ═══════════════════════════════════════════════════════════

def _reset_daily_counters():
    """Reset daily P&L counters at midnight Lagos time."""
    today = datetime.now(LAGOS_TZ).strftime("%Y-%m-%d")
    if _trading_state["last_reset"] != today:
        _trading_state["daily_loss"] = 0.0
        _trading_state["daily_profit"] = 0.0
        _trading_state["trades_today"] = 0
        _trading_state["last_reset"] = today
        print("Trading: daily counters reset for {}".format(today))

def _get_limitless_balance():
    """Fetch USDC balance from Limitless.
    Tries multiple endpoints since the API structure varies."""
    import requests as req
    try:
        # First get our wallet address from the private key
        try:
            from eth_account import Account
            account = Account.from_key(LIMITLESS_PRIV_KEY)
            wallet_addr = account.address
        except Exception:
            print("Balance: can't derive wallet address from private key")
            return _trading_state.get("last_balance")

        # Try 1: Get profile which may include balance info
        path = "/profiles/{}".format(wallet_addr)
        headers = _hmac_headers("GET", path)
        r = req.get("{}{}".format(LIMITLESS_API, path), headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            # Store profile ID for later use
            _trading_state["profile_id"] = data.get("id")
            _trading_state["wallet_addr"] = wallet_addr

        # Try 2: Trading allowance endpoint
        for try_path in ["/trading/allowance", "/portfolio/trading-allowance", "/allowance"]:
            try:
                h = _hmac_headers("GET", try_path)
                r2 = req.get("{}{}".format(LIMITLESS_API, try_path), headers=h, timeout=10)
                if r2.status_code == 200:
                    data2 = r2.json()
                    bal = data2.get("balance") or data2.get("allowance") or data2.get("available")
                    if bal is not None:
                        balance = float(bal) / 1e6 if float(bal) > 1000 else float(bal)
                        _trading_state["last_balance"] = balance
                        print("Balance: ${:.2f} (from {})".format(balance, try_path))
                        return balance
            except:
                continue

        # Try 3: Locked balance endpoint (shows how much is in open orders)
        path3 = "/trading/locked-balance"
        h3 = _hmac_headers("GET", path3)
        r3 = req.get("{}{}".format(LIMITLESS_API, path3), headers=h3, timeout=10)
        if r3.status_code == 200:
            print("Locked balance response: {}".format(r3.text[:200]))

        # If we can't get balance from API, use a manual fallback
        # The user sets their starting balance, and we track P&L from there
        if _trading_state.get("last_balance") is None:
            # Default starting balance — user should update via /trading/set?balance=20
            _trading_state["last_balance"] = _trading_state.get("starting_balance", 20.0)
            print("Balance: using manual balance ${:.2f} (API endpoints not found)".format(
                _trading_state["last_balance"]))

        return _trading_state.get("last_balance")
    except Exception as e:
        print("Balance error: {}".format(e))
        return _trading_state.get("last_balance") or _trading_state.get("starting_balance", 20.0)

def _fetch_market_details(slug):
    """Fetch full market details including venue and positionIds."""
    import requests as req
    try:
        path = "/markets/{}".format(slug)
        headers = _hmac_headers("GET", path)
        r = req.get(
            "{}{}".format(LIMITLESS_API, path),
            headers=headers,
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            # Log the keys we got so we can debug field names
            print("Market {} keys: {}".format(slug[:40], list(data.keys())[:15]))

            # Handle different possible field names for venue
            venue = data.get("venue") or data.get("clob") or data.get("exchange") or {}
            if not venue and data.get("exchangeAddress"):
                venue = {"exchange": data["exchangeAddress"]}

            # Handle different possible field names for position IDs
            pos_ids = data.get("positionIds") or data.get("tokenIds") or data.get("tokens") or []
            if not pos_ids:
                # Some responses have tokens as a dict {yes: "id", no: "id"}
                tokens = data.get("tokens", {})
                if isinstance(tokens, dict):
                    yes_id = tokens.get("yes") or tokens.get("YES") or tokens.get("0")
                    no_id = tokens.get("no") or tokens.get("NO") or tokens.get("1")
                    if yes_id and no_id:
                        pos_ids = [yes_id, no_id]
                elif isinstance(tokens, list) and len(tokens) >= 2:
                    pos_ids = tokens

            # Store back into data for the caller
            data["venue"] = venue
            data["positionIds"] = pos_ids

            if not venue.get("exchange") and not pos_ids:
                print("Market details WARNING: no venue or positionIds found. Full response keys: {}".format(
                    list(data.keys())))
                # Print a sample of the data to help debug
                for k in list(data.keys())[:10]:
                    v = data[k]
                    if isinstance(v, (str, int, float, bool)):
                        print("  {}: {}".format(k, str(v)[:80]))
                    elif isinstance(v, dict):
                        print("  {}: dict with keys {}".format(k, list(v.keys())[:5]))
                    elif isinstance(v, list):
                        print("  {}: list len {}".format(k, len(v)))

            return data
        print("Market details failed for {}: {} {}".format(slug[:40], r.status_code, r.text[:100]))
        return None
    except Exception as e:
        print("Market details error: {}".format(e))
        return None

def _sign_order(order_data, verifying_contract):
    """Sign order with EIP-712."""
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data
        from web3 import Web3
    except ImportError:
        print("Auto-trade ERROR: eth-account or web3 not installed. Add to requirements.txt: eth-account web3")
        return None
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data
        from web3 import Web3

        CHAIN_ID = 8453  # Base

        domain = {
            "name": "Limitless CTF Exchange",
            "version": "1",
            "chainId": CHAIN_ID,
            "verifyingContract": Web3.to_checksum_address(verifying_contract),
        }
        types = {
            "Order": [
                {"name": "salt", "type": "uint256"},
                {"name": "maker", "type": "address"},
                {"name": "signer", "type": "address"},
                {"name": "taker", "type": "address"},
                {"name": "tokenId", "type": "uint256"},
                {"name": "makerAmount", "type": "uint256"},
                {"name": "takerAmount", "type": "uint256"},
                {"name": "expiration", "type": "uint256"},
                {"name": "nonce", "type": "uint256"},
                {"name": "feeRateBps", "type": "uint256"},
                {"name": "side", "type": "uint8"},
                {"name": "signatureType", "type": "uint8"},
            ],
        }

        signable = encode_typed_data(domain, types, order_data)
        account = Account.from_key(LIMITLESS_PRIV_KEY)
        signed = account.sign_message(signable)
        return signed.signature.hex()
    except Exception as e:
        print("Signing error: {}".format(e))
        return None

def _is_safe_trading_window():
    """Check if current time is safe for auto-trading.
    Pauses during US session (1pm-6pm Lagos / 12:00-17:00 UTC) when
    major economic news can cause sudden price spikes."""
    hour = datetime.now(LAGOS_TZ).hour
    # 1pm-6pm Lagos = danger zone (US session opens, news drops)
    if 13 <= hour < 18:
        return False
    return True

def execute_trade(parsed_market, score, prediction_id):
    """Execute a trade on Limitless Exchange.
    Returns True if trade was placed, False otherwise."""
    import requests as req

    # Pre-checks
    if not _has_trading_keys():
        print("Auto-trade skipped: missing trading credentials")
        return False

    if not _trading_state["enabled"]:
        print("Auto-trade skipped: kill switch active")
        return False

    # US session protection — no auto-trades during 1pm-6pm Lagos
    if not _is_safe_trading_window():
        print("Auto-trade paused: US session window (1pm-6pm Lagos) — signal-only mode")
        return False

    _reset_daily_counters()

    # Check daily loss limit
    balance = _get_limitless_balance()
    if balance is None:
        print("Auto-trade skipped: cannot fetch balance")
        return False

    daily_limit = balance * _trading_state["daily_loss_limit_pct"]
    if _trading_state["daily_loss"] >= daily_limit:
        print("Auto-trade STOPPED: daily loss ${:.2f} >= limit ${:.2f}".format(
            _trading_state["daily_loss"], daily_limit))
        send_telegram(
            "⚠️ <b>Daily loss limit reached</b>\n"
            "Lost: ${:.2f} | Limit: ${:.2f}\n"
            "Auto-trading paused until tomorrow.\n"
            "Use /trading/start to override.".format(
                _trading_state["daily_loss"], daily_limit))
        _trading_state["enabled"] = False
        return False

    # Calculate stake
    confidence = score.get("confidence", "MEDIUM")
    if confidence == "HIGH":
        stake = max(_trading_state["min_stake"], round(balance * _trading_state["high_pct"], 2))
    else:
        stake = max(_trading_state["min_stake"], round(balance * _trading_state["medium_pct"], 2))

    # Don't bet more than we have
    if stake > balance:
        stake = max(_trading_state["min_stake"], round(balance * 0.5, 2))
    if stake > balance:
        print("Auto-trade skipped: balance ${:.2f} too low for min stake".format(balance))
        return False

    bet_side = score.get("bet_side", "YES")
    slug = parsed_market.get("slug", "")

    if not slug:
        print("Auto-trade skipped: no slug for market")
        return False

    try:
        # 1. Fetch market details (venue + positionIds)
        market_data = _fetch_market_details(slug)
        if not market_data:
            print("Auto-trade skipped: couldn't fetch market details for {}".format(slug))
            return False

        venue = market_data.get("venue", {})
        exchange_addr = venue.get("exchange", "")
        position_ids = market_data.get("positionIds", [])

        if not exchange_addr or len(position_ids) < 2:
            print("Auto-trade skipped: missing venue/positionIds for {}".format(slug))
            return False

        # positionIds[0] = YES token, positionIds[1] = NO token
        token_id = position_ids[0] if bet_side == "YES" else position_ids[1]

        # 2. Build order
        # For a BUY order: makerAmount = USDC we pay, takerAmount = shares we receive
        # Price = odds as decimal (e.g. 85% = 0.85)
        odds_decimal = score["bet_odds"] / 100.0
        if bet_side == "NO":
            price = 1.0 - odds_decimal  # NO price = 1 - YES price
        else:
            price = odds_decimal

        # Shares = stake / price (how many shares we get for our USDC)
        num_shares = stake / price
        maker_amount = int(stake * 1e6)        # USDC in 6 decimals
        taker_amount = int(num_shares * 1e6)   # Shares in 6 decimals

        from web3 import Web3
        from eth_account import Account

        account = Account.from_key(LIMITLESS_PRIV_KEY)
        wallet_addr = account.address

        # Generate unique salt
        import random
        salt = int(time.time() * 1000) * 1000 + random.randint(0, 999)

        ZERO_ADDR = "0x0000000000000000000000000000000000000000"

        order_data = {
            "salt": salt,
            "maker": Web3.to_checksum_address(wallet_addr),
            "signer": Web3.to_checksum_address(wallet_addr),
            "taker": Web3.to_checksum_address(ZERO_ADDR),
            "tokenId": int(token_id),
            "makerAmount": maker_amount,
            "takerAmount": taker_amount,
            "expiration": 0,  # no expiration
            "nonce": 0,
            "feeRateBps": 0,
            "side": 0,  # BUY
            "signatureType": 0,  # EOA
        }

        # 3. Sign
        signature = _sign_order(order_data, exchange_addr)
        if not signature:
            print("Auto-trade skipped: signing failed")
            return False

        # 4. Submit FOK order (Fill or Kill = instant market buy)
        order_payload = {
            "salt": str(salt),
            "maker": wallet_addr,
            "signer": wallet_addr,
            "taker": ZERO_ADDR,
            "tokenId": str(token_id),
            "makerAmount": str(maker_amount),
            "takerAmount": str(taker_amount),
            "expiration": "0",
            "nonce": "0",
            "feeRateBps": "0",
            "side": "0",
            "signatureType": "0",
            "signature": "0x" + signature if not signature.startswith("0x") else signature,
            "orderType": "FOK",
        }

        order_body = json.dumps(order_payload)
        path = "/orders"
        headers = _hmac_headers("POST", path, order_body)

        r = req.post(
            "{}{}".format(LIMITLESS_API, path),
            headers=headers,
            data=order_body,
            timeout=15,
        )

        if r.status_code in (200, 201):
            _trading_state["trades_today"] += 1
            result_data = r.json() if r.text else {}

            # Update DB with trade info
            try:
                conn = get_db()
                conn.run(
                    "UPDATE limitless_predictions SET size_rec=:s WHERE id=:i",
                    s="AUTO ${:.2f} | {}".format(stake, bet_side), i=prediction_id
                )
                conn.close()
            except:
                pass

            trade_msg = (
                "🤖 <b>AUTO-TRADE PLACED</b>\n"
                "──────────────────────────\n"
                "📌 {}\n"
                "<b>Side:</b> BUY {} shares\n"
                "<b>Stake:</b> ${:.2f}\n"
                "<b>Price:</b> {:.3f}\n"
                "<b>Shares:</b> {:.1f}\n"
                "<b>Balance:</b> ${:.2f}\n"
                "<b>Trade #:</b> {} today\n"
                "──────────────────────────\n"
                "📊 Daily P&L: +${:.2f} / -${:.2f}"
            ).format(
                parsed_market["title"],
                bet_side, stake, price, num_shares,
                balance - stake,
                _trading_state["trades_today"],
                _trading_state["daily_profit"],
                _trading_state["daily_loss"],
            )
            # Update tracked balance (subtract stake — shares are pending)
            _trading_state["last_balance"] = round(balance - stake, 2)
            send_telegram(trade_msg)
            print("AUTO-TRADE #{}: {} {} ${:.2f} on {}".format(
                prediction_id, bet_side, slug[:30], stake, parsed_market["title"][:40]))
            return True
        else:
            error_msg = r.text[:200] if r.text else "unknown error"
            print("Auto-trade FAILED: {} - {}".format(r.status_code, error_msg))
            send_telegram("❌ <b>Trade failed</b>\n{}\nError: {}".format(
                parsed_market["title"][:50], error_msg[:100]))
            return False

    except Exception as e:
        print("Auto-trade error: {}".format(e))
        send_telegram("❌ <b>Trade error</b>\n{}".format(str(e)[:100]))
        return False

def record_trade_outcome(prediction_id, won, stake_amount):
    """Record win/loss for daily P&L tracking and update balance."""
    if won:
        # Winning: shares pay $1 each. Profit = payout - stake
        # At avg 85% odds: bought at $0.85, pays $1 → profit = $0.15/share
        # But we track by stake: if we staked $3 at 85%, we get back $3.53
        # For simplicity: add back the full stake + estimated profit
        payout = stake_amount / 0.85  # approximate — shares bought at ~85% pay $1
        profit = payout - stake_amount
        _trading_state["daily_profit"] += profit
        _trading_state["last_balance"] = round(
            (_trading_state.get("last_balance") or 0) + payout, 2)
        print("Trade #{} WON: stake ${:.2f}, payout ${:.2f}, profit ${:.2f}, balance ${:.2f}".format(
            prediction_id, stake_amount, payout, profit, _trading_state["last_balance"]))
    else:
        # Losing: shares are worthless. Stake already subtracted when trade was placed.
        _trading_state["daily_loss"] += stake_amount
        print("Trade #{} LOST: stake ${:.2f}, balance ${:.2f}".format(
            prediction_id, stake_amount, _trading_state["last_balance"]))

# ═══════════════════════════════════════════════════════════
# SCANNER
# ═══════════════════════════════════════════════════════════

def run_scan():
    import requests as req
    global _last_scan_log
    debug_log = []
    try:
        btc_trend = get_btc_trend()
        r = req.get("{}/markets/active".format(LIMITLESS_API), timeout=15)
        if r.status_code != 200:
            print("Limitless API error: {}".format(r.status_code))
            return 0
        markets = r.json().get("data", [])
        print("Scan: {} markets total | BTC={} | Lagos={}".format(
            len(markets), btc_trend, datetime.now(LAGOS_TZ).strftime("%H:%M")))

        conn = get_db()
        alerted_rows = conn.run(
            "SELECT market_id FROM limitless_predictions WHERE fired_at::timestamptz > NOW() - INTERVAL '6 hours'"
        )
        alerted_ids = set(str(row[0]) for row in alerted_rows)
        conn.close()

        count = 0
        # Cache prices so we don't hit Yahoo repeatedly for same asset
        price_cache = {}
        for market in markets:
            try:
                parsed = parse_market(market)
                if not parsed:
                    continue
                if parsed["market_id"] in alerted_ids:
                    continue
                asset = parsed["asset"]
                if asset not in price_cache:
                    price_cache[asset] = get_price(asset)
                price = price_cache[asset]
                scored = score_market(parsed, btc_trend, price, debug_log)
                if not scored:
                    continue
                save_and_alert(parsed, scored, price, btc_trend)
                alerted_ids.add(parsed["market_id"])
                count += 1
                time.sleep(1)
            except Exception as e:
                print("Market error: {}".format(e))

        _last_scan_log = {
            "time": datetime.now(timezone.utc).isoformat(),
            "total": len(markets),
            "qualified": count,
            "filtered": debug_log[:30]
        }
        print("Scan done: {}/{} qualified".format(count, len(markets)))
        return count
    except Exception as e:
        print("Scanner error: {}".format(e))
        return 0

def scan_loop():
    time.sleep(30)
    while True:
        run_scan()
        time.sleep(300)

# ═══════════════════════════════════════════════════════════
# OUTCOME CHECKER
# ═══════════════════════════════════════════════════════════

def _fetch_match_result(match_name):
    """Try to fetch final score from available football APIs"""
    import requests as req
    try:
        # Try API-Football first
        key = os.environ.get("API_FOOTBALL_KEY", "")
        if key:
            # Search for the match in finished fixtures (last 3 days)
            for days_back in range(4):
                date = (datetime.now(LAGOS_TZ) - timedelta(days=days_back)).strftime("%Y-%m-%d")
                r = req.get(
                    "https://v3.football.api-sports.io/fixtures?date={}&status=FT".format(date),
                    headers={"x-apisports-key": key},
                    timeout=15
                )
                if r.status_code == 200:
                    for fx in r.json().get("response", []):
                        home = fx.get("teams", {}).get("home", {}).get("name", "")
                        away = fx.get("teams", {}).get("away", {}).get("name", "")
                        if home and away and (home in match_name or away in match_name):
                            if home in match_name and away in match_name:
                                return {
                                    "home": home, "away": away,
                                    "home_goals": fx.get("goals", {}).get("home"),
                                    "away_goals": fx.get("goals", {}).get("away"),
                                    "status": "finished",
                                }
        return None
    except Exception as e:
        print("Match result fetch error: {}".format(e))
        return None

def _evaluate_pick_result(pick, result):
    """Given a pick and match result, return True (won), False (lost), or None (can't tell)"""
    if not result or result.get("home_goals") is None:
        return None
    hg = int(result["home_goals"])
    ag = int(result["away_goals"])
    total = hg + ag
    pick_type = (pick.get("pick_type") or "").lower()
    pick_value = (pick.get("pick_value") or "").lower().strip()

    try:
        if "over_0.5" in pick_type:
            return total > 0 if pick_value in ("yes", "over") else total == 0
        elif "over_1.5" in pick_type:
            return total > 1 if pick_value in ("yes", "over") else total <= 1
        elif "over_2.5" in pick_type:
            return total > 2 if pick_value in ("yes", "over") else total <= 2
        elif "over_3.5" in pick_type:
            return total > 3 if pick_value in ("yes", "over") else total <= 3
        elif "both_teams_score" in pick_type or "btts" in pick_type:
            btts = hg > 0 and ag > 0
            return btts if pick_value in ("yes",) else not btts
        elif "match_winner" in pick_type or "winner" in pick_type:
            if pick_value in ("home",):
                return hg > ag
            elif pick_value in ("away",):
                return ag > hg
            elif pick_value in ("draw",):
                return hg == ag
        elif "draw_no_bet" in pick_type:
            if pick_value in ("home",):
                if hg == ag: return None  # draw = refund, treat as not lost
                return hg > ag
            elif pick_value in ("away",):
                if hg == ag: return None
                return ag > hg
        elif "double_chance" in pick_type:
            if pick_value in ("home",) or "home_or_draw" in pick_value:
                return hg >= ag
            elif pick_value in ("away",) or "away_or_draw" in pick_value:
                return ag >= hg
    except Exception as e:
        print("Evaluate error: {}".format(e))
    return None

def check_football_outcomes():
    """Auto-resolve football picks by fetching match results."""
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT id, match_id, pick_type, pick_value, kickoff_time, fired_at "
            "FROM football_picks "
            "WHERE status='Pending' AND accumulator_tier IN ('safe_2x','medium_3x','value_10x','value_100x')"
        )
        cols = [c['name'] for c in conn.columns]
        picks = [dict(zip(cols, r)) for r in rows]
        conn.close()
        now = datetime.now(timezone.utc)
        resolved_count = 0
        for p in picks:
            try:
                ko = p.get("kickoff_time", "")
                # Only check matches that are at least 2 hours past kickoff
                if ko:
                    try:
                        ko_dt = datetime.fromisoformat(ko.replace("Z", "+00:00"))
                        if ko_dt.tzinfo is None:
                            ko_dt = ko_dt.replace(tzinfo=timezone.utc)
                        if now < ko_dt + timedelta(hours=2):
                            continue  # Too early
                    except:
                        pass

                result = _fetch_match_result(p.get("match_id", ""))
                if not result:
                    # Mark as needs check only if > 24h past kickoff
                    if ko:
                        try:
                            ko_dt = datetime.fromisoformat(ko.replace("Z", "+00:00"))
                            if ko_dt.tzinfo is None:
                                ko_dt = ko_dt.replace(tzinfo=timezone.utc)
                            if now > ko_dt + timedelta(hours=24):
                                conn2 = get_db()
                                conn2.run(
                                    "UPDATE football_picks SET status='Needs Check' WHERE id=:i",
                                    i=p["id"]
                                )
                                conn2.close()
                        except:
                            pass
                    continue

                won = _evaluate_pick_result(p, result)
                if won is None:
                    continue

                status = "✅ Won" if won else "❌ Lost"
                outcome = "WIN" if won else "LOSS"
                conn2 = get_db()
                conn2.run(
                    "UPDATE football_picks SET status=:s, outcome=:o, resolved_at=:r WHERE id=:i",
                    s=status, o=outcome, r=now.isoformat(), i=p["id"]
                )
                conn2.close()
                resolved_count += 1
                print("Football #{} -> {} (score: {}-{})".format(
                    p["id"], outcome, result.get("home_goals"), result.get("away_goals")))
                # Notify on Telegram
                try:
                    emoji = "✅" if won else "❌"
                    send_telegram(
                        "{} <b>FOOTBALL {} — #{}</b>\n"
                        "📌 {}\n"
                        "⚽ Final: {}-{}\n"
                        "🎯 Pick: {} = {}".format(
                            emoji, outcome, p["id"],
                            p.get("match_id", ""),
                            result.get("home_goals", "?"), result.get("away_goals", "?"),
                            p.get("pick_type", ""), p.get("pick_value", "")
                        )
                    )
                except:
                    pass
                time.sleep(0.5)  # pace API calls
            except Exception as e:
                print("Football outcome #{}: {}".format(p["id"], e))
        if resolved_count > 0:
            print("Auto-resolved {} football picks".format(resolved_count))
    except Exception as e:
        print("Football outcome check error: {}".format(e))

def outcome_loop():
    while True:
        try:
            conn = get_db()
            rows = conn.run("SELECT * FROM limitless_predictions WHERE status='Pending'")
            cols = [c['name'] for c in conn.columns]
            items = [dict(zip(cols, r)) for r in rows]
            conn.close()

            now = datetime.now(timezone.utc)
            for p in items:
                try:
                    fired = datetime.fromisoformat(p["fired_at"])
                    if fired.tzinfo is None:
                        fired = fired.replace(tzinfo=timezone.utc)
                    expiry = fired + timedelta(hours=float(p["hours_left"] or 0))
                    if now < expiry:
                        continue
                    price = get_price(p["asset"])
                    if price is None:
                        continue
                    # Determine if the market question resolved true or false
                    market_resolved_true = (price > p["baseline"]) if p["direction"] == "above" else (price < p["baseline"])
                    # Check if our bet side won
                    bet_side = p.get("bet_side") or "YES"
                    if bet_side == "YES":
                        won = market_resolved_true
                    else:
                        won = not market_resolved_true
                    outcome = "WIN" if won else "LOSS"
                    status = "✅ Won" if won else "❌ Lost"
                    conn2 = get_db()
                    conn2.run(
                        "UPDATE limitless_predictions SET status=:s,outcome=:o,resolved_at=:r WHERE id=:i",
                        s=status, o=outcome, r=now.isoformat(), i=p["id"]
                    )
                    conn2.close()

                    # Update auto-trading balance if this was an auto-trade
                    size_rec = p.get("size_rec") or ""
                    if "AUTO $" in size_rec:
                        try:
                            stake_str = size_rec.split("AUTO $")[1].split(" |")[0]
                            stake_amt = float(stake_str)
                            record_trade_outcome(p["id"], won, stake_amt)
                        except:
                            pass

                    emoji = "✅" if won else "❌"
                    bal_str = " | Balance: ${:.2f}".format(_trading_state.get("last_balance", 0)) if _has_trading_keys() else ""
                    send_telegram(
                        "{} <b>PREDICTION {} — #{}</b>\n"
                        "──────────────────────────\n"
                        "📌 {}\n"
                        "<b>Closed:</b> {}\n"
                        "<b>Baseline:</b> {}{}".format(
                            emoji, outcome, p["id"], p["title"],
                            fmt_price(price), fmt_price(p["baseline"]),
                            bal_str
                        )
                    )
                except Exception as e:
                    print("Outcome #{}: {}".format(p["id"], e))
        except Exception as e:
            print("Outcome loop: {}".format(e))
        try:
            check_football_outcomes()
        except Exception as e:
            print("FB outcome error: {}".format(e))
        time.sleep(300)

# ═══════════════════════════════════════════════════════════
# FOOTBALL MODULE (Week 2)
# ═══════════════════════════════════════════════════════════

def _normalize_fixture(match, source):
    """Normalize fixture format across different APIs"""
    if source == "api-football":
        return {
            "id": match.get("fixture", {}).get("id"),
            "homeTeam": {"name": match.get("teams", {}).get("home", {}).get("name", "")},
            "awayTeam": {"name": match.get("teams", {}).get("away", {}).get("name", "")},
            "competition": {"name": match.get("league", {}).get("name", "")},
            "utcDate": match.get("fixture", {}).get("date", ""),
            "source": "api-football",
        }
    elif source == "football-data":
        return {
            "id": match.get("id"),
            "homeTeam": match.get("homeTeam", {}),
            "awayTeam": match.get("awayTeam", {}),
            "competition": match.get("competition", {}),
            "utcDate": match.get("utcDate", ""),
            "source": "football-data",
        }
    elif source == "thesportsdb":
        return {
            "id": match.get("idEvent"),
            "homeTeam": {"name": match.get("strHomeTeam", "")},
            "awayTeam": {"name": match.get("strAwayTeam", "")},
            "competition": {"name": match.get("strLeague", "")},
            "utcDate": "{}T{}".format(match.get("dateEvent", ""), match.get("strTime", "00:00:00")),
            "source": "thesportsdb",
        }
    return None

def _fetch_limitless_football_matches():
    """Pull football matches directly from Limitless category 49.
    These are the actual matches available as markets — the ones we can bet on."""
    raw_markets = _fetch_limitless_category(49, limit=25, pages=10)
    fixtures = []
    seen_match_ids = set()
    for m in raw_markets:
        title = m.get("title", "") or ""
        # Titles look like: "⚽ EPL, Brentford vs Fulham, Apr 18, 2026"
        # Parse: emoji + league, home vs away, date
        import re
        match = re.match(r'^[⚽\s]*([^,]+),\s*(.+?)\s+vs\s+(.+?),\s*(.+)$', title)
        if not match:
            continue
        league = match.group(1).strip()
        home = match.group(2).strip()
        away = match.group(3).strip()
        date_str = match.group(4).strip()

        # Get kickoff from expirationTimestamp
        exp_ts = m.get("expirationTimestamp", 0)
        kickoff = ""
        if exp_ts:
            try:
                kickoff = datetime.fromtimestamp(exp_ts / 1000, tz=timezone.utc).isoformat()
            except:
                pass

        match_key = "{}|{}|{}".format(home, away, date_str)
        if match_key in seen_match_ids:
            continue
        seen_match_ids.add(match_key)

        fixtures.append({
            "id": m.get("id"),
            "homeTeam": {"name": home},
            "awayTeam": {"name": away},
            "competition": {"name": league},
            "utcDate": kickoff,
            "source": "limitless",
            "slug": m.get("slug", ""),
        })
    return fixtures

def _fetch_api_football():
    """Fetch from API-Football via RapidAPI (100/day free tier)"""
    key = os.environ.get("API_FOOTBALL_KEY", "")
    if not key:
        return []
    import requests as req
    try:
        tomorrow = (datetime.now(LAGOS_TZ) + timedelta(days=1)).strftime("%Y-%m-%d")
        r = req.get(
            "https://v3.football.api-sports.io/fixtures?date={}".format(tomorrow),
            headers={"x-apisports-key": key},
            timeout=15
        )
        if r.status_code != 200:
            print("API-Football error: {}".format(r.status_code))
            return []
        matches = r.json().get("response", [])
        print("API-Football: {} fixtures (tomorrow)".format(len(matches)))
        return [_normalize_fixture(m, "api-football") for m in matches]
    except Exception as e:
        print("API-Football error: {}".format(e))
        return []

def _fetch_football_data():
    """Fetch from football-data.org (10/min free tier)"""
    if not FOOTBALL_DATA_KEY:
        return []
    import requests as req
    try:
        tomorrow = (datetime.now(LAGOS_TZ) + timedelta(days=1)).strftime("%Y-%m-%d")
        r = req.get(
            "https://api.football-data.org/v4/matches?dateFrom={}&dateTo={}".format(tomorrow, tomorrow),
            headers={"X-Auth-Token": FOOTBALL_DATA_KEY},
            timeout=15
        )
        if r.status_code != 200:
            print("football-data.org error: {}".format(r.status_code))
            return []
        matches = r.json().get("matches", [])
        print("football-data.org: {} fixtures (tomorrow)".format(len(matches)))
        return [_normalize_fixture(m, "football-data") for m in matches]
    except Exception as e:
        print("football-data.org error: {}".format(e))
        return []

def _fetch_thesportsdb():
    """Fetch from TheSportsDB (free, no key needed)"""
    import requests as req
    try:
        tomorrow = (datetime.now(LAGOS_TZ) + timedelta(days=1)).strftime("%Y-%m-%d")
        # TheSportsDB free endpoint - uses "1" as public key
        r = req.get(
            "https://www.thesportsdb.com/api/v1/json/3/eventsday.php?d={}&s=Soccer".format(tomorrow),
            timeout=15
        )
        if r.status_code != 200:
            print("TheSportsDB error: {}".format(r.status_code))
            return []
        events = r.json().get("events") or []
        print("TheSportsDB: {} fixtures (tomorrow)".format(len(events)))
        return [_normalize_fixture(e, "thesportsdb") for e in events]
    except Exception as e:
        print("TheSportsDB error: {}".format(e))
        return []

def get_todays_fixtures():
    """Primary: Limitless own matches (so we bet on markets that exist).
    Fallbacks: API-Football → football-data.org → TheSportsDB."""
    # Limitless first — these are the actual betting markets
    fixtures = _fetch_limitless_football_matches()
    if fixtures:
        print("Using Limitless native matches: {} fixtures".format(len(fixtures)))
        return fixtures
    # External APIs as fallback (only useful if you have their keys)
    fixtures = _fetch_api_football()
    if fixtures:
        return fixtures
    fixtures = _fetch_football_data()
    if fixtures:
        return fixtures
    return _fetch_thesportsdb()

def analyze_match_with_claude(match):
    """Use Claude Haiku to analyze a match like a seasoned punter — worst case scenario thinking"""
    if not ANTHROPIC_KEY:
        return None
    import requests as req
    try:
        home = match.get("homeTeam", {}).get("name", "")
        away = match.get("awayTeam", {}).get("name", "")
        comp = match.get("competition", {}).get("name", "")
        kickoff = match.get("utcDate", "")
        prompt = (
            "Football match: {} vs {}\n"
            "League: {}\n"
            "Kickoff: {}\n\n"
            "You are a SEASONED PUNTER with 20 years experience. You think in WORST-CASE SCENARIOS.\n\n"
            "YOUR PHILOSOPHY:\n"
            "- Never trust media hype. If bookies tip Chelsea to win 3-0, check their actual form first.\n"
            "- Check recent form (last 5 games), head-to-head records, home/away performance.\n"
            "- A team that hasnt scored in 4 games wont suddenly score 3 goals.\n"
            "- Big game pressure often means FEWER goals, not more.\n"
            "- New managers usually start with defensive tactics.\n"
            "- Injured key players massively affect team output.\n"
            "- The SAFEST bet is always the one where it takes an UNEXPECTED event to lose.\n\n"
            "FOR THE 2x SAFE TIER — think: what is ALMOST CERTAIN to happen?\n"
            "- Over 0.5 goals (a goalless draw takes an unexpected event)\n"
            "- Over 1.5 goals (most matches have 2+ goals)\n"
            "- Over 7.5 corners (most competitive matches hit 8+)\n"
            "- Both teams to get a card (its the Premier League / Serie A / etc)\n"
            "- Over 0.5 first half goals (if both teams are attacking)\n"
            "These should have implied odds 1.10-1.45 — safe, boring, almost certain.\n\n"
            "FOR THE 3x TIER — same philosophy but more picks stacked:\n"
            "- Similar safety level to 2x picks, just more of them\n"
            "- Over 2.5 goals only if BOTH teams have been scoring recently\n"
            "- BTTS only if both teams have scored in 4+ of last 5\n"
            "- Implied odds 1.15-1.50\n\n"
            "FOR THE 10x TIER — calculated risk based on data:\n"
            "- BTTS when both teams average 1.2+ goals\n"
            "- Over 2.5 goals in attacking matchups\n"
            "- Match winner when one team dominates head-to-head\n"
            "- Double chance for the team with better recent form\n"
            "- Implied odds 1.60-3.00\n\n"
            "FOR THE 100x TIER — long shots with genuine reasoning:\n"
            "- Both teams to score AND over 2.5 (needs attacking match)\n"
            "- Correct score 1-1 or 2-1 (most common scorelines)\n"
            "- Over 3.5 goals (only in genuinely open matches)\n"
            "- First half over 1.5 goals (in fast-starting teams)\n"
            "- Home/away win to nil (strong defence vs weak attack)\n"
            "- Implied odds 3.00-12.00\n\n"
            "Return a JSON array of 10-14 picks:\n"
            "- 4 SAFE picks: confidence 82-95, implied_odds 1.10-1.45\n"
            "- 3 MEDIUM picks: confidence 75-85, implied_odds 1.15-1.50\n"
            "- 4 VALUE picks: confidence 60-78, implied_odds 1.60-3.00\n"
            "- 3 MEGA picks: confidence 40-62, implied_odds 3.00-12.00\n\n"
            "STRICT RULES:\n"
            "- pick_value MUST be ONE OF: Yes, No, Home, Away, Draw, Over, Under\n"
            "- pick_type options: match_winner, both_teams_score, over_0.5_goals, over_1.5_goals, "
            "over_2.5_goals, over_3.5_goals, draw_no_bet, double_chance, over_8.5_corners, "
            "over_9.5_corners, over_2.5_cards, over_3.5_cards, over_4.5_cards, "
            "first_half_over_0.5, first_half_over_1.5, clean_sheet_home, clean_sheet_away, "
            "win_to_nil_home, win_to_nil_away, home_or_draw, away_or_draw, "
            "btts_and_over_2.5, handicap_home_minus1, handicap_away_minus1\n"
            "- reasoning: cite actual data (form, h2h, home/away stats). Max 100 chars.\n"
            "- NEVER pick a team to win 3+ goals if they havent scored in recent matches\n"
            "- NEVER pick BTTS if either team has kept 3+ clean sheets in last 5\n\n"
            "Output ONLY the JSON array. No markdown."
        ).format(home, away, comp, kickoff)

        r = req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 4000,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        if r.status_code != 200:
            print("Claude error: {}".format(r.text[:200]))
            return None
        data = r.json()
        text = data["content"][0]["text"].strip()
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        return json.loads(text)
    except Exception as e:
        print("Claude analyze error: {}".format(e))
        return None

def build_accumulators(picks):
    """Build multiple slips per tier (2x / 3x / 10x / 100x).
    Each MATCH appears in at most ONE tier.
    Pre-assigns matches to tiers fairly, then builds slips within each."""
    if not picks:
        return {}

    match_groups = {}
    for p in picks:
        m = (p.get("match") or "").strip()
        if not m:
            continue
        pv = (p.get("pick_value") or "").strip()
        if not pv or pv == "—":
            continue
        match_groups.setdefault(m, []).append(p)

    for m in match_groups:
        match_groups[m].sort(key=lambda p: p.get("confidence", 0), reverse=True)

    # PRE-ASSIGN: for each match, find its BEST pick for each tier
    # Then assign the match to the tier where it's most useful
    match_tier_scores = {}  # match -> {tier: (best_pick, odds, conf)}
    for match, match_picks in match_groups.items():
        match_tier_scores[match] = {}
        for p in match_picks:
            pi = float(p.get("implied_odds") or 1.0)
            pc = float(p.get("confidence") or 0)
            # Check which tiers this pick qualifies for
            if pc >= 80 and 1.10 <= pi <= 1.45 and "safe" not in match_tier_scores[match]:
                match_tier_scores[match]["safe"] = (p, pi, pc)
            if pc >= 75 and 1.15 <= pi <= 1.50 and "medium" not in match_tier_scores[match]:
                match_tier_scores[match]["medium"] = (p, pi, pc)
            if 60 <= pc < 80 and 1.60 <= pi <= 3.00 and "value" not in match_tier_scores[match]:
                match_tier_scores[match]["value"] = (p, pi, pc)
            if pc >= 40 and pi >= 3.00 and "mega" not in match_tier_scores[match]:
                match_tier_scores[match]["mega"] = (p, pi, pc)

    # Assign matches to tiers: prioritize underrepresented tiers
    tier_matches = {"safe": [], "medium": [], "value": [], "mega": []}
    used_matches = set()
    total = len(match_groups)

    # Target distribution: split matches roughly equally, with safe getting slightly more
    target_per_tier = max(3, total // 4)

    # First pass: assign matches that ONLY qualify for one tier
    for match in match_tier_scores:
        qualifying_tiers = [t for t in match_tier_scores[match]]
        if len(qualifying_tiers) == 1:
            tier = qualifying_tiers[0]
            tier_matches[tier].append((match, *match_tier_scores[match][tier]))
            used_matches.add(match)

    # Second pass: assign remaining matches to smallest tier
    remaining = [m for m in match_tier_scores if m not in used_matches]
    import random
    random.shuffle(remaining)  # randomize so it's not always alphabetical
    for match in remaining:
        # Find which qualifying tier is most underrepresented
        best_tier = None
        best_gap = -999
        for tier in match_tier_scores[match]:
            gap = target_per_tier - len(tier_matches[tier])
            if gap > best_gap:
                best_gap = gap
                best_tier = tier
        if best_tier:
            tier_matches[best_tier].append((match, *match_tier_scores[match][best_tier]))
            used_matches.add(match)

    # Now build slips for each tier
    def build_slips(candidates, target_odds, hard_max_picks=10):
        slips = []
        remaining = list(candidates)
        if not remaining:
            return slips
        # Sort: safe/medium by confidence desc, value/mega by odds desc
        while remaining:
            slip_picks = []
            cumulative = 1.0
            i = 0
            while i < len(remaining):
                if len(slip_picks) >= hard_max_picks:
                    break
                if cumulative >= target_odds and len(slip_picks) >= 2:
                    break
                match, pick, implied, conf = remaining[i]
                slip_picks.append(remaining.pop(i))
                cumulative *= implied
            if not slip_picks:
                break
            if cumulative >= target_odds or (cumulative >= target_odds * 0.6 and len(slip_picks) >= 3):
                slips.append({
                    "picks": [s[1] for s in slip_picks],
                    "total_odds": round(cumulative, 2)
                })
            else:
                break
        return slips

    # Sort candidates within each tier
    tier_matches["safe"].sort(key=lambda x: -x[3])      # by confidence
    tier_matches["medium"].sort(key=lambda x: -x[3])     # by confidence
    tier_matches["value"].sort(key=lambda x: -x[2])      # by odds
    tier_matches["mega"].sort(key=lambda x: -x[2])       # by odds

    return {
        "safe_2x":   build_slips(tier_matches["safe"],   target_odds=2.0,   hard_max_picks=8),
        "medium_3x": build_slips(tier_matches["medium"], target_odds=3.0,   hard_max_picks=8),
        "value_10x": build_slips(tier_matches["value"],  target_odds=10.0,  hard_max_picks=10),
        "mega_100x": build_slips(tier_matches["mega"],   target_odds=100.0, hard_max_picks=12),
    }



# ═══════════════════════════════════════════════════════════
# MARKET CLASSIFIER — team-level vs player-prop
# ═══════════════════════════════════════════════════════════

def classify_market_type(title):
    """Return 'team' if market is predictable from team stats,
    'player' if it needs real-time player data, 'complex' if too unpredictable."""
    t = (title or "").lower()

    # Player-specific props (NEED real-time player data — hard for Claude)
    player_patterns = [
        "to record more", "to make more", "to score more than",
        "to outscore", "to play more minutes",
        "donnarumma", "raya", "haaland", "martinelli", "doku",
        "to record", "to score", "minutes than",
        "successful dribbles", "more saves", "more touches",
        "more tackles", "big chances", "key passes",
        "to start", "on bench", "to commit more",
    ]
    # Individual named player = player prop
    player_names = [
        "haaland", "salah", "son", "saka", "rodri", "bruno fernandes",
        "casemiro", "caicedo", "gordon", "solanke", "watkins",
        "van dijk", "bellingham", "vinicius", "mbappe", "rashford",
        "martinelli", "saliba", "doku", "de bruyne", "isak",
        "welbeck", "tanaka", "hwang", "gyokeres",
    ]
    if any(p in t for p in player_patterns):
        return "player"
    if any(name in t for name in player_names):
        return "player"

    # Complex/unpredictable markets
    complex_patterns = [
        "goal in added time", "goal in first 5 minutes",
        "goal in first", "two goals to be scored within",
        "substitution before", "substitute",
        "red card in", "any player",
        "specific minute", "exact minute",
    ]
    if any(p in t for p in complex_patterns):
        return "complex"

    # Team-level markets (predictable from team stats)
    team_patterns = [
        "total goals", "total corners", "total cards",
        "both teams score", "both teams to score", "btts",
        "clean sheet", "to win", "to draw",
        "more goals than", "more corners than",
        "higher possession", "possession",
        "over", "under", "winner",
    ]
    if any(p in t for p in team_patterns):
        return "team"

    return "team"  # default — try to analyze


# ═══════════════════════════════════════════════════════════
# OFF THE PITCH SCANNER — football prop markets on Limitless
# ═══════════════════════════════════════════════════════════

def is_otp_market(market):
    """Detect football/sports prop markets vs crypto/stock price markets.
    Multi-strategy: category ID, automationType, title patterns."""
    title = market.get("title", "") or ""
    cats = market.get("categories", []) or []
    tags = market.get("tags", []) or []
    automation = (market.get("automationType") or "").lower()

    title_lower = title.lower()

    # EXCLUDE: crypto/stock price markets (strongest signal)
    is_price_market = (
        "above $" in title_lower or
        "below $" in title_lower or
        automation == "lumy"  # lumy is price oracle markets
    )
    if is_price_market:
        return False

    # INCLUDE signals for sports/OTP
    # Strategy 1: automationType
    if automation in ("sports", "sport"):
        return True

    # Strategy 2: category hints
    sport_cats = ["Football", "Soccer", "Sports", "Basketball", "Tennis",
                  "NBA", "NFL", "EPL", "UCL", "Premier League",
                  "Off the Pitch", "Props", "Matches"]
    if any(s.lower() in [c.lower() for c in cats] or s.lower() in [t.lower() for t in tags]
           for s in sport_cats):
        return True

    # Strategy 3: title patterns (team vs team, prop language)
    # "Team A vs Team B" pattern
    if " vs " in title_lower or " vs." in title_lower:
        return True

    # OTP-specific language
    otp_patterns = [
        "to record", "to score", "to win", "to make",
        "more goals", "more assists", "more shots",
        "more fouls", "more corners", "more tackles",
        "first to score", "clean sheet", "to commit",
        "yellow card", "red card", "penalty", "substitut"
    ]
    if any(p in title_lower for p in otp_patterns):
        return True

    return False


def _fetch_team_context_for_match(home_team, away_team):
    """Fetch recent team stats to give Claude real data to analyze with.
    Uses API-Football if key present. Returns formatted string or empty."""
    key = os.environ.get("API_FOOTBALL_KEY", "")
    if not key:
        return ""
    import requests as req
    context_lines = []
    try:
        # Search for home and away team IDs
        for team_name in [home_team, away_team]:
            if not team_name:
                continue
            r = req.get(
                "https://v3.football.api-sports.io/teams?search={}".format(team_name.replace(" ", "%20")),
                headers={"x-apisports-key": key},
                timeout=10
            )
            if r.status_code != 200:
                continue
            results = r.json().get("response", [])
            if not results:
                continue
            team_id = results[0].get("team", {}).get("id")
            if not team_id:
                continue
            # Get last 5 fixtures
            r2 = req.get(
                "https://v3.football.api-sports.io/fixtures?team={}&last=5".format(team_id),
                headers={"x-apisports-key": key},
                timeout=10
            )
            if r2.status_code != 200:
                continue
            fixtures = r2.json().get("response", [])
            form = []
            goals_scored = []
            goals_conceded = []
            for fx in fixtures:
                teams = fx.get("teams", {})
                goals = fx.get("goals", {})
                is_home = teams.get("home", {}).get("id") == team_id
                my_goals = goals.get("home") if is_home else goals.get("away")
                op_goals = goals.get("away") if is_home else goals.get("home")
                if my_goals is None or op_goals is None:
                    continue
                goals_scored.append(int(my_goals))
                goals_conceded.append(int(op_goals))
                if my_goals > op_goals:
                    form.append("W")
                elif my_goals == op_goals:
                    form.append("D")
                else:
                    form.append("L")
            if form:
                avg_scored = sum(goals_scored) / len(goals_scored)
                avg_conceded = sum(goals_conceded) / len(goals_conceded)
                context_lines.append(
                    "{}: last 5 = {} | scored {:.1f}/game | conceded {:.1f}/game".format(
                        team_name, "".join(form), avg_scored, avg_conceded
                    )
                )
        return "\n".join(context_lines)
    except Exception as e:
        print("Team context fetch error: {}".format(e))
        return ""

def _extract_teams_from_title(title):
    """Try to pull home/away teams from market title."""
    import re
    # Pattern: "X vs Y" or "Home vs Away"
    m = re.search(r'(?:against|vs\.?)\s+([A-Z][a-zA-Z\s]+?)(?:\s+on\b|\s*\?|\s*$|,)', title)
    if m:
        # Try to extract both - look backwards for home team
        parts = title.split(" vs ")
        if len(parts) == 2:
            home = parts[0].strip()
            # Home often has prefix like "Arsenal to commit more fouls than Man City"
            # Extract first capitalized noun
            hm = re.search(r'([A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)?)', home)
            away_str = parts[1].split(" on ")[0].split("?")[0].split(",")[0].strip()
            if hm:
                return hm.group(1), away_str
    # Pattern "Team A vs Team B: ..."
    m2 = re.search(r'^([A-Z][a-zA-Z\s]+?)\s+vs\s+([A-Z][a-zA-Z\s]+?):', title)
    if m2:
        return m2.group(1).strip(), m2.group(2).strip()
    return None, None


# ═══════════════════════════════════════════════════════════
# HEURISTIC ENGINE — pattern-match markets to real football stats
# ═══════════════════════════════════════════════════════════

# Hit rates based on aggregate football analytics (OPTA/bookmaker data)
# Format: (regex_pattern, pick_side, confidence, reasoning)
# Pattern matches the market title case-insensitively.

HEURISTIC_RULES = [
    # ─── Goal over/under markets ───────────────────────────────
    (r"over\s*0\.5\s*goals?|0\.5\+\s*total\s*goals?|1\+\s*(total\s*)?goals?",
        "YES", 88, "Over 0.5 goals happens in ~95% of matches"),
    (r"over\s*1\.5\s*goals?|2\+\s*(total\s*)?goals?",
        "YES", 75, "Over 1.5 goals happens in ~78% of matches"),
    (r"over\s*2\.5\s*goals?|3\+\s*(total\s*)?goals?",
        "YES", 55, "Over 2.5 goals ~55% in attacking leagues"),
    (r"over\s*3\.5\s*goals?|4\+\s*(total\s*)?goals?",
        "NO",  68, "Over 3.5 goals only ~30% of matches"),
    (r"over\s*4\.5\s*goals?|5\+\s*(total\s*)?goals?",
        "NO",  82, "Over 4.5 goals only ~14% of matches"),

    # ─── Corner markets ────────────────────────────────────────
    (r"over\s*7\.5\s*(total\s*)?corners?|8\+\s*(total\s*)?corners?",
        "YES", 72, "Over 7.5 corners in ~74% of matches"),
    (r"over\s*8\.5\s*(total\s*)?corners?|9\+\s*(total\s*)?corners?",
        "YES", 62, "Over 8.5 corners in ~64% of matches"),
    (r"over\s*9\.5\s*(total\s*)?corners?|10\+\s*(total\s*)?corners?",
        "YES", 55, "Over 9.5 corners ~56% — slight lean"),
    (r"over\s*10\.5\s*(total\s*)?corners?|11\+\s*(total\s*)?corners?",
        "NO",  60, "Over 10.5 corners only ~44%"),
    (r"over\s*11\.5\s*(total\s*)?corners?|12\+\s*(total\s*)?corners?",
        "NO",  70, "Over 11.5 corners only ~35%"),

    # ─── Card markets ──────────────────────────────────────────
    (r"over\s*1\.5\s*(total\s*)?cards?|2\+\s*(total\s*)?cards?",
        "YES", 88, "Over 1.5 cards in ~92% of matches"),
    (r"over\s*2\.5\s*(total\s*)?cards?|3\+\s*(total\s*)?cards?",
        "YES", 78, "Over 2.5 cards in ~82% of matches"),
    (r"over\s*3\.5\s*(total\s*)?cards?|4\+\s*(total\s*)?cards?",
        "YES", 62, "Over 3.5 cards in ~68% of matches"),
    (r"over\s*4\.5\s*(total\s*)?cards?|5\+\s*(total\s*)?cards?",
        "NO",  58, "Over 4.5 cards only ~43%"),
    (r"over\s*5\.5\s*(total\s*)?cards?|6\+\s*(total\s*)?cards?",
        "NO",  72, "Over 5.5 cards only ~25%"),

    # ─── BTTS markets ──────────────────────────────────────────
    (r"both\s+.+?\s+and\s+.+?\s+score|both\s*teams?\s*(to\s*)?score|\bbtts\b",
        "YES", 58, "BTTS ~55% avg, higher in EPL/Bundesliga"),

    # ─── Clean sheet markets ───────────────────────────────────
    (r"clean\s*sheet",
        "NO",  65, "Clean sheets rare — only ~30% of matches"),
    (r"to\s*keep\s*a?\s*clean\s*sheet",
        "NO",  65, "Keeping clean sheet rare (~30%)"),

    # ─── Early/late goal timing ────────────────────────────────
    (r"concede\s*before\s*(the\s*)?(\d+)\s*minute|goal\s*before\s*(the\s*)?(\d+)\s*minute",
        "NO",  72, "Early goals rare — only ~15-25%"),
    (r"goal\s*in\s*added\s*time|added\s*time\s*goal",
        "NO",  70, "Added time goals only ~18% of matches"),
    (r"goal\s*in\s*first\s*\d+\s*minutes?",
        "NO",  68, "Goals in specific short windows rare"),

    # ─── Penalties ─────────────────────────────────────────────
    (r"take\s*a?\s*penalty|penalty\s*to\s*be\s*awarded|penalty\s*awarded",
        "NO",  65, "Penalty awarded in only ~25% of matches"),
    (r"penalty\s*scored",
        "NO",  70, "Penalty scored even rarer (~20%)"),

    # ─── Substitution markets ─────────────────────────────────
    (r"substitut.*before\s*(the\s*)?60",
        "YES", 75, "Sub before 60min in ~85% of modern matches"),
    (r"substitut.*before\s*(the\s*)?70",
        "YES", 85, "Sub before 70min in ~95% of modern matches"),

    # ─── Possession ────────────────────────────────────────────
    (r"higher\s*possession|more\s*possession",
        "YES", 60, "Home team wins possession ~60% of the time"),
    (r"(\d+)%\+?\s*possession|over\s*(\d+)%?\s*possession",
        "YES", 55, "Teams usually hit 45%+ possession"),

    # ─── Shots ─────────────────────────────────────────────────
    (r"over\s*\d+\.5\s*shots\s*on\s*target|\d+\+\s*shots\s*on\s*target",
        "YES", 60, "Total SoT typically high in competitive matches"),

    # ─── Result markets (home win default in doubt) ───────────
    (r"to\s*win\s*to\s*nil",
        "NO",  65, "Win-to-nil uncommon — teams usually score"),
    (r"draw\s*no\s*bet\s*home|1x",
        "YES", 58, "Home team wins or draws in ~60% of matches"),
    (r"double\s*chance.*home",
        "YES", 65, "Home team wins or draws in ~60% of matches"),
]

def heuristic_pick(title):
    """Match market title against known football heuristics.
    Returns dict with action/confidence/reasoning or None if no rule matches."""
    import re
    t = (title or "").lower()
    for pattern, action, conf, reason in HEURISTIC_RULES:
        if re.search(pattern, t, flags=re.IGNORECASE):
            return {
                "action": action,
                "confidence": conf,
                "reasoning": reason,
                "source": "heuristic",
            }
    return None

def analyze_otp_market_with_claude(market, parsed_odds):
    """Use Claude Haiku to analyze a football prop market WITH team context data."""
    if not ANTHROPIC_KEY:
        return None
    import requests as req
    try:
        title = market.get("title", "")
        yes_odds = parsed_odds["yes_odds"]
        no_odds  = 100 - yes_odds
        hours    = parsed_odds["hours_left"]

        # Fetch real team stats to feed Claude
        home, away = _extract_teams_from_title(title)
        team_context = ""
        if home and away:
            team_context = _fetch_team_context_for_match(home, away)
        
        prompt = (
            "You are analyzing a prediction market on Limitless Exchange. "
            "Your job is to identify mispriced markets where the current odds don't reflect reality.\n\n"
            "MARKET: {}\n"
            "Current odds: YES {:.1f}% / NO {:.1f}%\n"
            "Time to expiry: {:.1f} hours\n\n"
            "Based on publicly known information (team form, player stats, recent news), "
            "should a bettor take YES, NO, or SKIP this market?\n\n"
            "Only recommend YES or NO if you have HIGH confidence (75%+) and the market odds "
            "offer value. Most markets should be SKIP.\n\n"
            "Respond ONLY in this JSON format (no other text):\n"
            '{{"action": "YES"|"NO"|"SKIP", "confidence": 0-100, "reasoning": "brief explanation (max 120 chars)"}}'
        ).format(title, yes_odds, no_odds, hours)
        
        # Prefill assistant response to force YES or NO output
        r = req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 300,
                "messages": [
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": '{"action": "'}
                ]
            },
            timeout=30
        )
        if r.status_code != 200:
            print("OTP Claude error: {}".format(r.status_code))
            return None
        data = r.json()
        raw_text = data["content"][0]["text"].strip()
        # Reconstruct full JSON since we prefilled the start
        full_text = '{"action": "' + raw_text
        # Trim anything after the closing brace
        close_idx = full_text.rfind("}")
        if close_idx > 0:
            full_text = full_text[:close_idx + 1]
        try:
            parsed = json.loads(full_text)
            # Reject SKIP (enforce YES or NO)
            if parsed.get("action") not in ("YES", "NO"):
                print("  [Claude returned non-YES/NO: {}]".format(parsed.get("action")))
                return None
            return parsed
        except Exception as e:
            print("  [Claude JSON parse failed: {}]".format(full_text[:100]))
            return None
    except Exception as e:
        print("OTP analyze error: {}".format(e))
        return None

def save_and_alert_otp(market, parsed, analysis):
    """Save OTP pick to DB and send Telegram alert"""
    try:
        action = analysis["action"]
        if action == "SKIP":
            return
        conf = analysis.get("confidence", 0)
        reasoning = analysis.get("reasoning", "")
        source = analysis.get("source", "unknown")
        
        now = datetime.now(timezone.utc).isoformat()
        conn = get_db()
        rows = conn.run(
            """INSERT INTO football_picks
            (match_id, home_team, away_team, competition, kickoff_time,
             pick_type, pick_value, confidence, reasoning, implied_odds,
             accumulator_tier, status, fired_at)
            VALUES (:mid, :h, :a, :c, :k, 'limitless_otp', :pv, :conf, :r, :o, 'single', 'Pending', :now)
            RETURNING id""",
            mid=parsed["title"][:200],  # Save the market TITLE, not the ID
            h=str(parsed.get("market_id", ""))[:50],  # Store ID in home_team field for reference
            a=parsed.get("slug", "")[:100],  # Slug in away_team for link reconstruction
            c="Limitless OTP",
            k=parsed["expiry_dt"].isoformat(), pv=action,
            conf=conf, r=reasoning[:200],
            o=parsed["yes_odds"] if action == "YES" else (100 - parsed["yes_odds"]),
            now=now
        )
        pid = rows[0][0]
        conn.close()
        
        odds_val = parsed["yes_odds"] if action == "YES" else (100 - parsed["yes_odds"])
        hrs_str = "{:.1f} hrs".format(parsed["hours_left"]) if parsed["hours_left"] >= 1 else "{:.0f} mins".format(parsed["mins_left"])
        conf_emoji = "🔥" if conf >= 80 else "🟡" if conf >= 65 else "⚪"
        source_label = {"heuristic": "📊 Stats-based", "claude": "🤖 AI-analyzed"}.get(source, "")

        msg = (
            "⚽ <b>OFF THE PITCH #{}</b>\n"
            "──────────────────────────\n"
            "📌 {}\n"
            "──────────────────────────\n"
            "<b>Pick:</b> {} ✅\n"
            "<b>Market Odds:</b> {:.1f}%\n"
            "<b>Time Left:</b> {}\n"
            "──────────────────────────\n"
            "{} <b>Confidence:</b> {}%  {}\n"
            "💭 <b>Reasoning:</b> {}\n"
            "🔗 limitless.exchange/markets/{}"
        ).format(
            pid, parsed["title"],
            action, odds_val, hrs_str,
            conf_emoji, conf, source_label,
            reasoning,
            parsed["slug"]
        )
        send_telegram(msg)
        print("OTP alert #{} [{}]: {} -> {} ({}%)".format(pid, source, parsed["title"][:50], action, conf))
    except Exception as e:
        print("OTP alert error: {}".format(e))

# Limitless category IDs (discovered via /debug/otp):
LIMITLESS_CAT_MATCHES = 49   # 217 football matches
LIMITLESS_CAT_OTP     = 50   # 66 "Off The Pitch" prop markets
LIMITLESS_CAT_PROPS   = 66   # 316 generic props

def _fetch_limitless_category(category_id, limit=25, pages=4):
    """Fetch markets from a specific Limitless category. API caps limit at 25."""
    import requests as req
    markets = []
    try:
        for page in range(1, pages + 1):
            r = req.get(
                "{}/markets/active/{}?limit={}&page={}".format(LIMITLESS_API, category_id, limit, page),
                timeout=15
            )
            if r.status_code != 200:
                break
            data = r.json().get("data", [])
            if not data:
                break
            markets.extend(data)
            if len(data) < limit:
                break
    except Exception as e:
        print("Fetch category {} error: {}".format(category_id, e))
    return markets

def run_otp_scan():
    """Scan Limitless for sports/OTP markets using proven category endpoints."""
    import requests as req
    if not ANTHROPIC_KEY:
        print("OTP scan skipped — no ANTHROPIC_API_KEY")
        return 0
    try:
        otp_markets = []

        # Strategy 1: direct category fetch — Off The Pitch category
        otp_only = _fetch_limitless_category(LIMITLESS_CAT_OTP, limit=25, pages=4)
        print("OTP scan: category 50 (OTP) returned {} markets".format(len(otp_only)))
        otp_markets.extend(otp_only)

        # Strategy 2: also include Props category (these are football prop markets)
        props = _fetch_limitless_category(LIMITLESS_CAT_PROPS, limit=25, pages=6)
        print("OTP scan: category 66 (Props) returned {} markets".format(len(props)))
        # Deduplicate by market ID
        seen_ids = set(m.get("id") for m in otp_markets)
        for m in props:
            if m.get("id") not in seen_ids:
                otp_markets.append(m)
                seen_ids.add(m.get("id"))

        if not otp_markets:
            print("OTP scan: categories returned 0 markets — API may be having issues")
            return 0

        print("OTP scan: {} total prop/OTP markets to analyze".format(len(otp_markets)))
        
        # Get already-alerted (we now store market ID in home_team field for OTP rows)
        conn = get_db()
        alerted = conn.run(
            "SELECT home_team FROM football_picks WHERE fired_at::timestamptz > NOW() - INTERVAL '6 hours' AND pick_type='limitless_otp'"
        )
        alerted_ids = set(str(r[0]) for r in alerted if r[0])
        conn.close()
        
        count = 0
        for market in otp_markets[:100]:  # Cap at 100 (heuristics are free)
            try:
                mid = str(market.get("id", ""))
                if mid in alerted_ids:
                    continue
                
                # Parse basic timing/odds
                exp_ts = market.get("expirationTimestamp", 0)
                if not exp_ts:
                    continue
                expiry_dt = datetime.fromtimestamp(exp_ts / 1000, tz=timezone.utc)
                now = datetime.now(timezone.utc)
                mins_left = (expiry_dt - now).total_seconds() / 60
                # Allow any future market — from 15 mins to 7 days ahead
                if mins_left <= 15 or mins_left > 10080:
                    continue
                
                prices = market.get("prices", [0.5, 0.5])
                yes_raw = float(prices[0])
                yes_odds = yes_raw if yes_raw > 1 else yes_raw * 100
                
                # Skip extreme-odds markets (not worth analyzing)
                if yes_odds < 15 or yes_odds > 92:
                    continue

                # CLASSIFY: skip player props and complex markets (Claude can't analyze them well)
                market_class = classify_market_type(market.get("title", ""))
                if market_class != "team":
                    print("  OTP skip ({}): {}".format(market_class, market.get("title", "")[:60]))
                    continue
                
                parsed = {
                    "market_id": mid,
                    "title": market.get("title", ""),
                    "yes_odds": yes_odds,
                    "hours_left": mins_left / 60,
                    "mins_left": mins_left,
                    "expiry_dt": expiry_dt,
                    "slug": market.get("slug", ""),
                }
                
                # HYBRID: try heuristic first (free, instant)
                analysis = heuristic_pick(market.get("title", ""))

                # Only call Claude if heuristic couldn't match
                if not analysis:
                    analysis = analyze_otp_market_with_claude(market, parsed)
                    if analysis:
                        analysis["source"] = "claude"

                if analysis and analysis.get("action") in ("YES", "NO"):
                    save_and_alert_otp(market, parsed, analysis)
                    count += 1
                    # Only sleep when we actually called Claude
                    if analysis.get("source") == "claude":
                        time.sleep(2)
                else:
                    print("  OTP unmatched: {}".format(parsed["title"][:60]))
            except Exception as e:
                print("OTP market error: {}".format(e))
        
        print("OTP scan done: {} picks sent".format(count))
        return count
    except Exception as e:
        print("OTP scan error: {}".format(e))
        return 0

def otp_loop():
    """Run OTP scan every 30 minutes"""
    time.sleep(90)
    while True:
        try:
            if is_lagos_window():
                run_otp_scan()
        except Exception as e:
            print("OTP loop: {}".format(e))
        time.sleep(1800)  # 30 min

def save_accumulator_picks(accas):
    """Save accumulator picks to DB — wipes ALL old pending when new batch arrives.
    Now handles multiple slips per tier."""
    try:
        conn = get_db()
        now = datetime.now(timezone.utc).isoformat()
        conn.run(
            "UPDATE football_picks SET status='Replaced' "
            "WHERE status='Pending' AND pick_type != 'limitless_otp' "
            "AND accumulator_tier IN ('safe_2x','medium_3x','value_10x','mega_100x')"
        )
        total_saved = 0
        for tier_name, slips in accas.items():
            if not slips:
                continue
            for slip_idx, slip in enumerate(slips):
                slip_num = slip_idx + 1
                for p in slip.get("picks", []):
                    match_str = p.get("match", "")
                    home, away = "", ""
                    if " vs " in match_str:
                        parts = match_str.split(" vs ", 1)
                        home, away = parts[0].strip(), parts[1].strip()
                    # Build a readable pick description
                    raw_type = p.get("pick_type", "")
                    raw_value = str(p.get("pick_value", ""))
                    # Create human-readable pick string
                    readable = _format_pick_readable(raw_type, raw_value)
                    conn.run(
                        """INSERT INTO football_picks
                        (match_id, home_team, away_team, competition, kickoff_time,
                         pick_type, pick_value, confidence, reasoning, implied_odds,
                         accumulator_tier, status, fired_at)
                        VALUES (:m, :h, :a, :c, :k, :pt, :pv, :conf, :r, :o, :tier, 'Pending', :now)""",
                        m=match_str[:100], h=home[:50], a=away[:50],
                        c=p.get("competition", "")[:50],
                        k=p.get("kickoff", ""),
                        pt=raw_type,
                        pv=readable,
                        conf=float(p.get("confidence", 0)),
                        r="Slip {} | {}".format(slip_num, str(p.get("reasoning", ""))[:180]),
                        o=float(p.get("implied_odds", 1.0)),
                        tier=tier_name,
                        now=now
                    )
                    total_saved += 1
        conn.close()
        print("Accumulators saved to DB: {} picks across {} tiers".format(total_saved, len(accas)))
    except Exception as e:
        print("Save accumulator error: {}".format(e))

def _format_pick_readable(pick_type, pick_value):
    """Convert pick_type + pick_value into human-readable text.
    e.g. 'over_2.5_goals' + 'Over' -> 'Over 2.5 Goals'
         'both_teams_score' + 'Yes' -> 'Both Teams Score: Yes'
         'match_winner' + 'Home' -> 'Home Win'"""
    t = (pick_type or "").lower()
    v = (pick_value or "").strip()

    mappings = {
        "over_0.5_goals": "Over 0.5 Goals",
        "over_1.5_goals": "Over 1.5 Goals",
        "over_2.5_goals": "Over 2.5 Goals",
        "over_3.5_goals": "Over 3.5 Goals",
        "over_4.5_goals": "Over 4.5 Goals",
        "both_teams_score": "Both Teams Score: {}".format(v),
        "btts": "Both Teams Score: {}".format(v),
        "btts_and_over_2.5": "BTTS & Over 2.5 Goals",
        "match_winner": "{} Win".format(v) if v in ("Home", "Away") else "Draw" if v == "Draw" else "Winner: {}".format(v),
        "draw_no_bet": "Draw No Bet: {}".format(v),
        "double_chance": "Double Chance: {}".format(v),
        "home_or_draw": "Home or Draw",
        "away_or_draw": "Away or Draw",
        "over_8.5_corners": "Over 8.5 Corners",
        "over_9.5_corners": "Over 9.5 Corners",
        "over_2.5_cards": "Over 2.5 Cards",
        "over_3.5_cards": "Over 3.5 Cards",
        "over_4.5_cards": "Over 4.5 Cards",
        "first_half_over_0.5": "1st Half Over 0.5 Goals",
        "first_half_over_1.5": "1st Half Over 1.5 Goals",
        "clean_sheet_home": "Home Clean Sheet: {}".format(v),
        "clean_sheet_away": "Away Clean Sheet: {}".format(v),
        "win_to_nil_home": "Home Win to Nil",
        "win_to_nil_away": "Away Win to Nil",
        "handicap_home_minus1": "Home -1 Handicap",
        "handicap_away_minus1": "Away -1 Handicap",
    }

    if t in mappings:
        return mappings[t]

    # Fallback: format the type nicely and append value
    formatted = t.replace("_", " ").title()
    if v and v.lower() not in formatted.lower():
        formatted = "{}: {}".format(formatted, v)
    return formatted

def football_loop():
    """Run football analysis every 6 hours — scans multiple days ahead"""
    time.sleep(60)
    while True:
        try:
            if not ANTHROPIC_KEY:
                print("Football: missing ANTHROPIC_API_KEY — skipping")
                time.sleep(21600)
                continue
            fixtures = get_todays_fixtures()
            print("Football: {} fixtures found".format(len(fixtures)))
            all_picks = []
            for match in fixtures[:20]:  # analyze up to 20 matches per run
                picks = analyze_match_with_claude(match)
                if picks:
                    for p in picks:
                        p["match"] = "{} vs {}".format(
                            match.get("homeTeam", {}).get("name", ""),
                            match.get("awayTeam", {}).get("name", "")
                        )
                        p["kickoff"] = match.get("utcDate", "")
                        p["competition"] = match.get("competition", {}).get("name", "")
                        all_picks.append(p)
                time.sleep(2)
            if all_picks:
                accas = build_accumulators(all_picks)
                save_accumulator_picks(accas)

                # Format kickoff as readable time
                def fmt_kickoff(kickoff_str):
                    if not kickoff_str:
                        return ""
                    try:
                        dt = datetime.fromisoformat(kickoff_str.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        dt_lagos = dt.astimezone(LAGOS_TZ)
                        return dt_lagos.strftime("%a %d %b, %H:%M")
                    except:
                        return kickoff_str[:10]

                # Clean pick value — strip weird formatting
                def clean_value(val):
                    if not val:
                        return "—"
                    s = str(val).strip()
                    if len(s) > 40:
                        s = s[:40]
                    return s or "—"

                msg = "⚽ <b>DAILY FOOTBALL ACCUMULATORS</b>\n"
                msg += "<i>{}</i>\n\n".format(datetime.now(LAGOS_TZ).strftime("%A, %d %B"))

                tier_labels = {
                    "safe_2x": "🟢 2x Safe",
                    "medium_3x": "🟡 3x Medium",
                    "value_10x": "🔥 10x Value",
                    "mega_100x": "🚀 100x Mega"
                }

                for tier_name, slips in accas.items():
                    if not slips:
                        continue
                    label = tier_labels.get(tier_name, tier_name)
                    for slip_idx, slip in enumerate(slips):
                        if not slip.get("picks"):
                            continue
                        msg += "<b>{} Slip #{}</b> — {:.2f}x\n".format(label, slip_idx + 1, slip["total_odds"])
                        msg += "─────────────────\n"
                        for p in slip["picks"]:
                            match = p.get("match", "Unknown")
                            comp = p.get("competition", "")
                            ko = fmt_kickoff(p.get("kickoff", ""))
                            pick_type = p.get("pick_type", "").replace("_", " ").title()
                            pick_val = clean_value(p.get("pick_value", ""))
                            conf = int(p.get("confidence", 0))
                            odds = float(p.get("implied_odds", 1.0))
                            reasoning = p.get("reasoning", "")

                            msg += "⚡ <b>{}</b>\n".format(match)
                            if comp or ko:
                                line = []
                                if comp:
                                    line.append(comp)
                                if ko:
                                    line.append(ko)
                                msg += "   🏆 <i>{}</i>\n".format(" · ".join(line))
                            msg += "   → {}: <b>{}</b> @ {:.2f} ({}%)\n".format(pick_type, pick_val, odds, conf)
                            if reasoning:
                                msg += "   💭 <i>{}</i>\n".format(reasoning[:80])
                            msg += "\n"
                        msg += "\n"

                # Summary
                total_slips = sum(len(s) for s in accas.values())
                total_picks = sum(len(p.get("picks", [])) for s in accas.values() for p in s)
                msg += "📊 <i>{} slips · {} picks · Each match in one tier only</i>".format(total_slips, total_picks)
                send_telegram(msg)
        except Exception as e:
            print("Football loop: {}".format(e))
        time.sleep(21600)  # 6 hours

# ═══════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════

@app.route("/limitless/update/<int:pred_id>/<status>", methods=["POST"])
def update_prediction(pred_id, status):
    if status not in ["✅ Won", "❌ Lost", "Pending"]:
        return {"error": "Invalid status"}, 400
    outcome = "WIN" if status == "✅ Won" else "LOSS" if status == "❌ Lost" else ""
    conn = get_db()
    conn.run(
        "UPDATE limitless_predictions SET status=:s,outcome=:o,resolved_at=:r WHERE id=:i",
        s=status, o=outcome, r=datetime.now(timezone.utc).isoformat(), i=pred_id
    )
    conn.close()
    return {"status": "updated"}, 200

@app.route("/scan", methods=["GET"])
def manual_scan():
    threading.Thread(target=run_scan, daemon=True).start()
    return {"status": "scan triggered"}, 200

@app.route("/trading/stop", methods=["GET"])
def trading_stop():
    """Kill switch — immediately stop all auto-trading."""
    _trading_state["enabled"] = False
    send_telegram("🛑 <b>Auto-trading STOPPED</b>\nKill switch activated. Signals still fire but no trades placed.\nUse /trading/start to resume.")
    return {"status": "auto-trading stopped", "enabled": False}, 200

@app.route("/trading/start", methods=["GET"])
def trading_start():
    """Resume auto-trading."""
    _trading_state["enabled"] = True
    _reset_daily_counters()
    balance = _get_limitless_balance()
    send_telegram("✅ <b>Auto-trading RESUMED</b>\nBalance: ${:.2f}\nHIGH stake: {:.0f}% | MEDIUM stake: {:.0f}%\nDaily loss limit: {:.0f}%".format(
        balance or 0,
        _trading_state["high_pct"] * 100,
        _trading_state["medium_pct"] * 100,
        _trading_state["daily_loss_limit_pct"] * 100,
    ))
    return {"status": "auto-trading started", "enabled": True, "balance": balance}, 200

@app.route("/trading/status", methods=["GET"])
def trading_status():
    """Check current auto-trading status."""
    balance = _get_limitless_balance()
    return {
        "enabled": _trading_state["enabled"],
        "balance": balance,
        "safe_window": _is_safe_trading_window(),
        "current_hour_lagos": datetime.now(LAGOS_TZ).hour,
        "mode": "AUTO-TRADING" if _trading_state["enabled"] and _is_safe_trading_window() else "SIGNALS ONLY (US session)" if _trading_state["enabled"] and not _is_safe_trading_window() else "STOPPED (kill switch)",
        "daily_loss": _trading_state["daily_loss"],
        "daily_profit": _trading_state["daily_profit"],
        "trades_today": _trading_state["trades_today"],
        "high_pct": _trading_state["high_pct"],
        "medium_pct": _trading_state["medium_pct"],
        "daily_loss_limit_pct": _trading_state["daily_loss_limit_pct"],
        "has_keys": _has_trading_keys(),
        "schedule": "AUTO: 6am-1pm & 6pm-6am Lagos | SIGNALS ONLY: 1pm-6pm Lagos",
    }, 200

@app.route("/trading/set", methods=["GET"])
def trading_set():
    """Adjust trading parameters. Usage: /trading/set?high=0.15&medium=0.08&loss_limit=0.25&balance=20"""
    if request.args.get("high"):
        _trading_state["high_pct"] = float(request.args["high"])
    if request.args.get("medium"):
        _trading_state["medium_pct"] = float(request.args["medium"])
    if request.args.get("loss_limit"):
        _trading_state["daily_loss_limit_pct"] = float(request.args["loss_limit"])
    if request.args.get("balance"):
        bal = float(request.args["balance"])
        _trading_state["last_balance"] = bal
        _trading_state["starting_balance"] = bal
    return {
        "high_pct": _trading_state["high_pct"],
        "medium_pct": _trading_state["medium_pct"],
        "daily_loss_limit_pct": _trading_state["daily_loss_limit_pct"],
        "balance": _trading_state.get("last_balance"),
    }, 200

@app.route("/football/clear", methods=["GET"])
def clear_football_picks():
    """Wipe old accumulator picks with broken formatting. Run once, then /football/scan."""
    try:
        conn = get_db()
        conn.run(
            "DELETE FROM football_picks WHERE pick_type != 'limitless_otp' "
            "AND accumulator_tier IN ('safe_2x','medium_3x','value_10x','mega_100x')"
        )
        conn.close()
        return {"status": "cleared all accumulator picks — now hit /football/scan to regenerate"}, 200
    except Exception as e:
        return {"error": str(e)}, 500

@app.route("/football/scan", methods=["GET"])
def manual_football_scan():
    """Manually trigger the football accumulator builder (instead of waiting 6 hours)."""
    def run_once():
        try:
            if not ANTHROPIC_KEY:
                print("Football scan skipped — no ANTHROPIC_API_KEY")
                return
            fixtures = get_todays_fixtures()
            print("Manual football scan: {} fixtures".format(len(fixtures)))
            all_picks = []
            for match in fixtures[:20]:
                picks = analyze_match_with_claude(match)
                if picks:
                    for p in picks:
                        p["match"] = "{} vs {}".format(
                            match.get("homeTeam", {}).get("name", ""),
                            match.get("awayTeam", {}).get("name", "")
                        )
                        p["kickoff"] = match.get("utcDate", "")
                        p["competition"] = match.get("competition", {}).get("name", "")
                        all_picks.append(p)
                time.sleep(2)
            if all_picks:
                accas = build_accumulators(all_picks)
                save_accumulator_picks(accas)
                print("Manual football scan: done — {} picks saved".format(len(all_picks)))
        except Exception as e:
            print("Manual football scan error: {}".format(e))
    threading.Thread(target=run_once, daemon=True).start()
    return {"status": "football scan triggered — wait 60-90 seconds, then refresh /app/football"}, 200


@app.route("/debug/otp")
def debug_otp():
    """Diagnostic endpoint — shows what the OTP scanner is seeing."""
    import requests as req
    report = {"strategies": {}, "sample_markets": []}

    # Strategy 1: automationType=sports
    try:
        r = req.get("{}/markets/active?automationType=sports&limit=100".format(LIMITLESS_API), timeout=15)
        if r.status_code == 200:
            markets = r.json().get("data", [])
            report["strategies"]["automation_sports"] = {
                "status": r.status_code,
                "count": len(markets),
                "sample_titles": [m.get("title", "")[:80] for m in markets[:5]],
            }
        else:
            report["strategies"]["automation_sports"] = {"status": r.status_code, "error": r.text[:200]}
    except Exception as e:
        report["strategies"]["automation_sports"] = {"error": str(e)}

    # Strategy 2: fetch category counts
    try:
        r = req.get("{}/markets/categories/count".format(LIMITLESS_API), timeout=10)
        if r.status_code == 200:
            report["strategies"]["category_counts"] = r.json()
    except Exception as e:
        report["strategies"]["category_counts"] = {"error": str(e)}

    # Strategy 3: pull pages and categorize
    try:
        category_breakdown = {}
        automation_breakdown = {}
        total_pulled = 0
        all_sample_titles = []
        for page in range(1, 6):
            r = req.get("{}/markets/active?page={}&limit=100".format(LIMITLESS_API, page), timeout=15)
            if r.status_code != 200:
                break
            markets = r.json().get("data", [])
            if not markets:
                break
            total_pulled += len(markets)
            for m in markets:
                for c in (m.get("categories") or []):
                    category_breakdown[c] = category_breakdown.get(c, 0) + 1
                auto = m.get("automationType") or "none"
                automation_breakdown[auto] = automation_breakdown.get(auto, 0) + 1
                if len(all_sample_titles) < 20 and "above $" not in m.get("title", "").lower():
                    all_sample_titles.append(m.get("title", "")[:80])
            if len(markets) < 100:
                break
        report["strategies"]["paginated_analysis"] = {
            "total_pulled": total_pulled,
            "categories_found": category_breakdown,
            "automation_types": automation_breakdown,
            "non_crypto_sample_titles": all_sample_titles,
        }
    except Exception as e:
        report["strategies"]["paginated_analysis"] = {"error": str(e)}

    # Strategy 4: search endpoint
    try:
        r = req.get("{}/markets/search?query=goals&limit=10".format(LIMITLESS_API), timeout=10)
        if r.status_code == 200:
            results = r.json().get("data", [])
            report["strategies"]["search_goals"] = {
                "count": len(results),
                "sample_titles": [m.get("title", "")[:80] for m in results[:5]],
            }
    except Exception as e:
        report["strategies"]["search_goals"] = {"error": str(e)}

    # Strategy 5: try category 49 and 50 directly (football categories likely)
    for cat_id in [49, 50, 43]:
        try:
            r = req.get("{}/markets/active/{}?limit=5".format(LIMITLESS_API, cat_id), timeout=10)
            if r.status_code == 200:
                markets = r.json().get("data", [])
                report["strategies"]["category_{}".format(cat_id)] = {
                    "status": r.status_code,
                    "count": len(markets),
                    "sample_titles": [m.get("title", "")[:80] for m in markets[:5]],
                }
            else:
                report["strategies"]["category_{}".format(cat_id)] = {"status": r.status_code}
        except Exception as e:
            report["strategies"]["category_{}".format(cat_id)] = {"error": str(e)}

    return jsonify(report)

@app.route("/otp/clear", methods=["GET"])
def clear_otp_picks():
    """One-time cleanup: wipe old OTP picks that have market IDs instead of titles."""
    try:
        conn = get_db()
        # Delete all existing OTP picks so next scan generates fresh ones with titles
        conn.run("DELETE FROM football_picks WHERE pick_type='limitless_otp'")
        conn.close()
        return {"status": "cleared all OTP picks — run /otp/scan to regenerate"}, 200
    except Exception as e:
        return {"error": str(e)}, 500

@app.route("/otp/scan", methods=["GET"])
def manual_otp_scan():
    threading.Thread(target=run_otp_scan, daemon=True).start()
    return {"status": "OTP scan triggered"}, 200

@app.route("/debug", methods=["GET"])
def debug():
    """Show why markets were filtered out in the last scan"""
    return jsonify({
        "last_scan": _last_scan_log,
        "btc": _btc_trend_cache,
        "in_window": is_lagos_window(),
        "lagos_time": datetime.now(LAGOS_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    })

@app.route("/test")
def test():
    btc = get_btc_trend()
    win = is_lagos_window()
    send_telegram(
        "✅ <b>Limitless Bot v3 — LIVE</b>\n\n"
        "✅ Scanner active (5 min)\n"
        "✅ Outcome tracker active\n"
        "✅ Football module: {}\n"
        "✅ PostgreSQL connected\n\n"
        "<b>BTC:</b> {}\n"
        "<b>Window:</b> {}".format(
            "ready" if (ANTHROPIC_KEY and FOOTBALL_DATA_KEY) else "needs keys",
            btc or "Calculating...",
            "🟢 OPEN" if win else "🔴 CLOSED"
        )
    )
    return {"status": "ok", "btc_trend": btc, "in_window": win}, 200


# ═══════════════════════════════════════════════════════════
# DASHBOARD HTML
# ═══════════════════════════════════════════════════════════

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Limitless — CMVNG</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,500;9..144,600&family=Inter+Tight:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #fafaf7;
  --bg-subtle: #f4f3ed;
  --surface: #ffffff;
  --surface-hover: #fbfaf5;
  --border: #ececea;
  --border-strong: #dcdbd7;
  --accent: #1a3d2e;
  --accent-muted: #2d5a42;
  --accent-soft: #e8efe9;
  --positive: #1a7046;
  --positive-bg: #e8f3ed;
  --negative: #b4322e;
  --negative-bg: #f7e7e5;
  --warning: #8a6a2f;
  --warning-bg: #f5eedb;
  --info: #2d4a7a;
  --info-bg: #e5ecf5;
  --ink: #1a1a17;
  --ink-2: #3a3a35;
  --ink-3: #6b6b64;
  --ink-4: #9c9c94;
  --display: 'Fraunces', Georgia, serif;
  --sans: 'Inter Tight', -apple-system, sans-serif;
  --mono: 'JetBrains Mono', monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
::selection{background:var(--accent);color:var(--bg)}
html{scroll-behavior:smooth}
body{font-family:var(--sans);background:var(--bg);color:var(--ink);-webkit-font-smoothing:antialiased;min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;background-image:radial-gradient(circle at 20% 30%,rgba(26,61,46,.015) 0%,transparent 40%),radial-gradient(circle at 80% 70%,rgba(26,61,46,.015) 0%,transparent 40%);pointer-events:none;z-index:0}
.app{position:relative;z-index:1;max-width:1380px;margin:0 auto}
.hdr{padding:24px 40px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:20px;border-bottom:1px solid var(--border)}
.brand{display:flex;align-items:center;gap:14px}
.brand-mark{width:38px;height:38px;border-radius:10px;background:var(--accent);display:flex;align-items:center;justify-content:center;position:relative}
.brand-mark::before{content:'';width:14px;height:14px;border:2px solid var(--bg);border-radius:50%}
.brand-mark::after{content:'';position:absolute;width:4px;height:4px;background:var(--bg);border-radius:50%}
.brand-text h1{font-family:var(--display);font-weight:500;font-size:19px;letter-spacing:-.02em;font-variation-settings:"opsz" 14;line-height:1.1}
.brand-text small{font-size:10px;font-family:var(--mono);color:var(--ink-4);text-transform:uppercase;letter-spacing:.12em}
.hdr-right{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.nav-tabs{display:flex;gap:4px;background:var(--bg-subtle);border-radius:10px;padding:3px}
.nav-tab{padding:7px 14px;font-size:12px;font-weight:500;cursor:pointer;color:var(--ink-3);border-radius:8px;transition:all .15s;font-family:var(--sans);letter-spacing:-.005em;text-decoration:none}
.nav-tab.active{background:var(--surface);color:var(--ink);box-shadow:0 1px 2px rgba(0,0,0,.04)}
.pills{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.pill{font-size:11px;font-weight:500;padding:6px 11px;border-radius:100px;display:inline-flex;align-items:center;gap:6px;font-family:var(--sans);letter-spacing:-.005em;background:var(--surface);border:1px solid var(--border);color:var(--ink-2);transition:border-color .15s}
.pill-active{background:var(--positive-bg);color:var(--positive);border-color:transparent}
.pill-inactive{background:var(--warning-bg);color:var(--warning);border-color:transparent}
.pill-btc-up{border-color:var(--positive);color:var(--positive);font-family:var(--mono)}
.pill-btc-down{border-color:var(--negative);color:var(--negative);font-family:var(--mono)}
.dot{width:6px;height:6px;border-radius:50%;background:currentColor;position:relative}
.dot.live::after{content:'';position:absolute;inset:-3px;border-radius:50%;border:1.5px solid currentColor;opacity:0;animation:ring 2s ease-out infinite}
@keyframes ring{0%{opacity:1;transform:scale(.8)}80%,100%{opacity:0;transform:scale(2)}}
.hero{padding:48px 40px 32px;border-bottom:1px solid var(--border)}
.hero-label{font-size:10px;font-family:var(--mono);color:var(--ink-4);text-transform:uppercase;letter-spacing:.15em;margin-bottom:12px;display:flex;align-items:center;gap:10px}
.hero-label::before{content:'';width:24px;height:1px;background:var(--ink-4)}
.hero-title{font-family:var(--display);font-weight:400;font-size:clamp(34px,4.8vw,52px);line-height:1.03;letter-spacing:-.035em;font-variation-settings:"opsz" 80,"SOFT" 30;max-width:900px;margin-bottom:14px}
.hero-title em{font-style:italic;color:var(--accent);font-weight:400;font-variation-settings:"opsz" 144}
.hero-sub{font-size:15px;color:var(--ink-3);max-width:560px;line-height:1.55}
.stats{padding:32px 40px;display:grid;grid-template-columns:repeat(6,1fr);gap:0;border-bottom:1px solid var(--border)}
.stat{padding:0 24px;position:relative}
.stat+.stat{border-left:1px solid var(--border)}
.stat:first-child{padding-left:0}
.stat:last-child{padding-right:0}
.stat-label{font-size:10px;font-family:var(--mono);color:var(--ink-4);text-transform:uppercase;letter-spacing:.14em;margin-bottom:10px;font-weight:500}
.stat-value{font-family:var(--display);font-weight:400;font-size:40px;line-height:1;letter-spacing:-.04em;font-variation-settings:"opsz" 80;margin-bottom:6px}
.stat-value.is-positive{color:var(--positive)}.stat-value.is-negative{color:var(--negative)}
.stat-value.is-warning{color:var(--warning)}.stat-value.is-accent{color:var(--accent)}
.stat-meta{font-size:11px;font-family:var(--mono);color:var(--ink-4)}
@media(max-width:900px){.stats{grid-template-columns:repeat(3,1fr);gap:24px 0}.stat:nth-child(3n+1){padding-left:0}.stat:nth-child(3n){padding-right:0}.stat:nth-child(n+4){border-top:1px solid var(--border);padding-top:24px}}
@media(max-width:600px){.stats{grid-template-columns:repeat(2,1fr)}.stat{border-left:none!important;padding:0}.stat:nth-child(n+3){border-top:1px solid var(--border);padding-top:20px;margin-top:4px}}
.action-bar{padding:24px 40px;display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}
.section-head{display:flex;align-items:baseline;gap:14px}
.section-title{font-family:var(--display);font-weight:500;font-size:22px;letter-spacing:-.02em;font-variation-settings:"opsz" 24}
.section-count{font-size:11px;font-family:var(--mono);color:var(--ink-4);background:var(--bg-subtle);padding:3px 8px;border-radius:100px}
.actions{display:flex;gap:8px;align-items:center}
.btn{font-family:var(--sans);font-size:13px;font-weight:500;padding:9px 16px;border-radius:8px;border:1px solid var(--border);background:var(--surface);color:var(--ink);cursor:pointer;display:inline-flex;align-items:center;gap:7px;transition:all .15s;box-shadow:0 1px 2px rgba(0,0,0,.02);text-decoration:none}
.btn:hover{border-color:var(--border-strong);background:var(--surface-hover);transform:translateY(-.5px)}
.btn-primary{background:var(--accent);color:var(--bg);border-color:var(--accent);box-shadow:0 1px 2px rgba(26,61,46,.15)}
.btn-primary:hover{background:var(--accent-muted);border-color:var(--accent-muted)}
.table-wrap{margin:0 40px 32px;background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden}
.table-scroll{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px;min-width:900px}
thead{background:var(--bg-subtle);border-bottom:1px solid var(--border)}
thead th{text-align:left;padding:14px 16px;font-size:10px;font-family:var(--mono);font-weight:500;color:var(--ink-3);text-transform:uppercase;letter-spacing:.1em;white-space:nowrap}
thead th:first-child{padding-left:24px}
thead th:last-child{padding-right:24px}
tbody td{padding:16px;border-bottom:1px solid var(--border);color:var(--ink-2)}
tbody td:first-child{padding-left:24px}
tbody td:last-child{padding-right:24px}
tbody tr:last-child td{border-bottom:none}
tbody tr{transition:background .1s}
tbody tr:hover{background:var(--bg)}
.cell-id{font-family:var(--mono);color:var(--ink-4);font-size:12px}
.cell-market{font-weight:500;color:var(--ink);max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.cell-asset{font-family:var(--mono);font-weight:600;font-size:12px;color:var(--accent);letter-spacing:.02em}
.cell-type{font-family:var(--mono);font-size:11px;color:var(--ink-4);text-transform:uppercase;letter-spacing:.08em}
.cell-odds{font-family:var(--mono);font-weight:600;font-size:13px;color:var(--ink)}
.cell-price{font-family:var(--mono);font-size:12px}
.cell-time{font-family:var(--mono);font-size:11px;color:var(--ink-4)}
.tag{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:100px;font-size:11px;font-weight:500}
.tag-pending{background:var(--info-bg);color:var(--info)}
.tag-won{background:var(--positive-bg);color:var(--positive)}
.tag-lost{background:var(--negative-bg);color:var(--negative)}
.tag-high{background:var(--accent-soft);color:var(--accent)}
.tag-med{background:var(--warning-bg);color:var(--warning)}
.tag-yes{background:#dcfce7;color:#16a34a;font-weight:700}
.tag-no{background:#fef2f2;color:#dc2626;font-weight:700}
.cell-margin{font-family:var(--mono);font-size:12px;color:var(--ink-3)}
.act{font-family:var(--sans);font-size:11px;font-weight:500;padding:5px 10px;border-radius:6px;border:1px solid transparent;cursor:pointer;margin-right:4px;transition:all .15s}
.act-won{background:var(--positive-bg);color:var(--positive)}
.act-won:hover{background:var(--positive);color:var(--bg)}
.act-lost{background:var(--negative-bg);color:var(--negative)}
.act-lost:hover{background:var(--negative);color:var(--bg)}
.empty-state{padding:64px 24px;text-align:center}
.empty-mark{width:56px;height:56px;border-radius:14px;background:var(--bg-subtle);display:inline-flex;align-items:center;justify-content:center;font-size:22px;margin-bottom:16px;border:1px solid var(--border)}
.empty-state h3{font-family:var(--display);font-weight:500;font-size:18px;color:var(--ink);margin-bottom:6px}
.empty-state p{font-size:13px;color:var(--ink-3);max-width:340px;margin:0 auto;line-height:1.55}
.footer{padding:24px 40px 40px;border-top:1px solid var(--border);text-align:center;font-size:11px;font-family:var(--mono);color:var(--ink-4);letter-spacing:.04em}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--ink);color:var(--bg);padding:12px 20px;border-radius:100px;font-size:13px;font-weight:500;box-shadow:0 10px 40px rgba(0,0,0,.15);opacity:0;transition:all .3s cubic-bezier(.34,1.56,.64,1);z-index:1000}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
@keyframes fade-up{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}
.hero-label,.hero-title,.hero-sub{animation:fade-up .6s ease both}
.hero-title{animation-delay:.1s}
.hero-sub{animation-delay:.2s}
.stat{animation:fade-up .5s ease both}
.stat:nth-child(1){animation-delay:.3s}.stat:nth-child(2){animation-delay:.35s}
.stat:nth-child(3){animation-delay:.4s}.stat:nth-child(4){animation-delay:.45s}
.stat:nth-child(5){animation-delay:.5s}.stat:nth-child(6){animation-delay:.55s}
@media(max-width:720px){.hdr,.hero,.stats,.action-bar,.footer{padding-left:20px;padding-right:20px}.table-wrap{margin-left:20px;margin-right:20px}.hero{padding-top:32px}.hero-title{font-size:30px}.stat-value{font-size:28px}}
</style></head><body>
<div class="app">
<header class="hdr">
  <div class="brand">
    <div class="brand-mark"></div>
    <div class="brand-text">
      <h1>Limitless</h1>
      <small>CMVNG · Prediction Platform</small>
    </div>
  </div>
  <div class="hdr-right">
    <nav class="nav-tabs">
      <a href="/" class="nav-tab">Home</a>
      <a href="/app" class="nav-tab active">Crypto</a>
      <a href="/app/football" class="nav-tab">Football</a>
    </nav>
    <div class="pills">
      <span class="pill {{ 'pill-active' if in_window else 'pill-inactive' }}">
        <span class="dot live"></span>
        {{ 'Window Open' if in_window else 'Window Closed' }}
      </span>
      <span class="pill {{ 'pill-btc-up' if btc_trend == 'BUY' else 'pill-btc-down' if btc_trend == 'SELL' else '' }}">
        BTC {{ '↗ BUY' if btc_trend == 'BUY' else '↘ SELL' if btc_trend == 'SELL' else '— N/A' }}
      </span>
    </div>
  </div>
</header>

<section class="hero">
  <div class="hero-label">Prediction Intelligence</div>
  <h2 class="hero-title">Precision scanning,<br><em>effortless compounding.</em></h2>
  <p class="hero-sub">Automated scanner monitoring Limitless markets in real-time, surfacing only opportunities that match your edge across price, timing and trend.</p>
</section>

<section class="stats">
  <div class="stat"><div class="stat-label">Total Sent</div><div class="stat-value">{{ stats.total }}</div><div class="stat-meta">all time</div></div>
  <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value {{ 'is-positive' if stats.wr >= 65 else 'is-warning' if stats.wr >= 50 else 'is-negative' if stats.total > 0 else '' }}">{{ stats.wr }}<span style="font-size:.5em;color:var(--ink-4)">%</span></div><div class="stat-meta">{{ stats.wins }}W · {{ stats.losses }}L</div></div>
  <div class="stat"><div class="stat-label">Wins</div><div class="stat-value is-positive">{{ stats.wins }}</div><div class="stat-meta">resolved</div></div>
  <div class="stat"><div class="stat-label">Losses</div><div class="stat-value is-negative">{{ stats.losses }}</div><div class="stat-meta">resolved</div></div>
  <div class="stat"><div class="stat-label">Pending</div><div class="stat-value is-warning">{{ stats.pending }}</div><div class="stat-meta">in play</div></div>
  <div class="stat"><div class="stat-label">Today</div><div class="stat-value is-accent">{{ stats.today }}</div><div class="stat-meta">Lagos time</div></div>
</section>

<div class="action-bar">
  <div class="section-head">
    <h3 class="section-title">Predictions</h3>
    <span class="section-count">{{ stats.total }} total</span>
  </div>
  <div class="actions">
    <a class="btn" href="/debug" target="_blank">⚙ Debug</a>
    <button class="btn" onclick="location.reload()">↻ Refresh</button>
    <button class="btn btn-primary" onclick="triggerScan()">◎ Scan Now</button>
  </div>
</div>

<div class="table-wrap">
  <div class="table-scroll">
    <table>
      <thead><tr>
        <th>#</th><th>Market</th><th>Asset</th><th>Side</th><th>Type</th><th>Odds</th>
        <th>Price @ Alert</th><th>Baseline</th><th>Margin</th><th>Time Left</th>
        <th>Confidence</th><th>Status</th><th>Logged</th><th>Action</th>
      </tr></thead>
      <tbody>
        {% if not preds %}
        <tr><td colspan="14">
          <div class="empty-state">
            <div class="empty-mark">◎</div>
            <h3>Awaiting first signal</h3>
            <p>Scanner runs every 5 minutes during your Lagos trading window. Click <b>Debug</b> to see why markets were filtered.</p>
          </div>
        </td></tr>
        {% endif %}
        {% for p in preds %}
        <tr>
          <td class="cell-id">{{ p.id }}</td>
          <td><div class="cell-market" title="{{ p.title }}">{{ p.title }}</div></td>
          <td><span class="cell-asset">{{ p.asset }}</span></td>
          <td><span class="tag {{ 'tag-yes' if (p.bet_side or 'YES') == 'YES' else 'tag-no' }}">{{ p.bet_side or 'YES' }}</span></td>
          <td><span class="cell-type">{{ p.market_type }}</span></td>
          <td><span class="cell-odds">{{ "%.1f"|format(p.bet_odds) }}%</span></td>
          <td><span class="cell-price">{{ "$%.4f"|format(p.current_price) if p.current_price and p.current_price < 100 else "$%.2f"|format(p.current_price) if p.current_price else "—" }}</span></td>
          <td><span class="cell-price">{{ "$%.4f"|format(p.baseline) if p.baseline < 100 else "$%.2f"|format(p.baseline) }}</span></td>
          <td><span class="cell-margin">{% if p.current_price and p.baseline %}{{ "%.2f"|format(((p.current_price - p.baseline)|abs / p.baseline * 100)) }}%{% else %}—{% endif %}</span></td>
          <td><span class="cell-time">{{ "%.1fh"|format(p.hours_left) if p.hours_left else "—" }}</span></td>
          <td><span class="tag {{ 'tag-high' if p.confidence == 'HIGH' else 'tag-med' }}">{{ 'High' if p.confidence == 'HIGH' else 'Medium' }}</span></td>
          <td><span class="tag {{ 'tag-pending' if p.status == 'Pending' else 'tag-won' if '✅' in (p.status or '') else 'tag-lost' }}">{{ 'Pending' if p.status == 'Pending' else 'Won' if '✅' in (p.status or '') else 'Lost' }}</span></td>
          <td><span class="cell-time">{{ p.fired_at[:16].replace("T"," ") if p.fired_at else "—" }}</span></td>
          <td>
            {% if p.status == "Pending" %}
            <button class="act act-won" onclick="updL({{ p.id }},'✅ Won')">Won</button>
            <button class="act act-lost" onclick="updL({{ p.id }},'❌ Lost')">Lost</button>
            {% endif %}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>

<footer class="footer">Scanner · 5min intervals · Auto-resolving · Auto-refresh 60s</footer>
</div>

<div class="toast" id="toast"><span id="toast-msg">Scan triggered</span></div>

<script>
function updL(id, s){ fetch('/limitless/update/'+id+'/'+encodeURIComponent(s),{method:'POST'}).then(()=>location.reload()); }
function triggerScan(){ fetch('/scan').then(()=>showToast('Scan running — check Telegram shortly')); }
function showToast(msg){ const t=document.getElementById('toast'); document.getElementById('toast-msg').textContent=msg; t.classList.add('show'); setTimeout(()=>t.classList.remove('show'),3000); }
setTimeout(()=>location.reload(),60000);
</script>
</body></html>"""

FOOTBALL_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Football — Limitless CMVNG</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,500;9..144,600&family=Inter+Tight:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{--bg:#fafaf7;--bg-subtle:#f4f3ed;--surface:#fff;--border:#ececea;--border-strong:#dcdbd7;--accent:#1a3d2e;--accent-muted:#2d5a42;--accent-soft:#e8efe9;--positive:#1a7046;--positive-bg:#e8f3ed;--negative:#b4322e;--negative-bg:#f7e7e5;--warning:#8a6a2f;--warning-bg:#f5eedb;--info:#2d4a7a;--info-bg:#e5ecf5;--mega:#7c3aed;--mega-bg:#ede9fe;--ink:#1a1a17;--ink-2:#3a3a35;--ink-3:#6b6b64;--ink-4:#9c9c94;--display:'Fraunces',Georgia,serif;--sans:'Inter Tight',sans-serif;--mono:'JetBrains Mono',monospace}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:var(--sans);background:var(--bg);color:var(--ink);-webkit-font-smoothing:antialiased;min-height:100vh}
.app{max-width:1380px;margin:0 auto}
.hdr{padding:24px 40px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:20px;border-bottom:1px solid var(--border)}
.brand{display:flex;align-items:center;gap:14px}
.brand-mark{width:38px;height:38px;border-radius:10px;background:var(--accent);display:flex;align-items:center;justify-content:center;position:relative}
.brand-mark::before{content:'';width:14px;height:14px;border:2px solid var(--bg);border-radius:50%}
.brand-mark::after{content:'';position:absolute;width:4px;height:4px;background:var(--bg);border-radius:50%}
.brand-text h1{font-family:var(--display);font-weight:500;font-size:19px;letter-spacing:-.02em}
.brand-text small{font-size:10px;font-family:var(--mono);color:var(--ink-4);text-transform:uppercase;letter-spacing:.12em}
.nav-tabs{display:flex;gap:4px;background:var(--bg-subtle);border-radius:10px;padding:3px}
.nav-tab{padding:7px 14px;font-size:12px;font-weight:500;color:var(--ink-3);border-radius:8px;text-decoration:none}
.nav-tab.active{background:var(--surface);color:var(--ink);box-shadow:0 1px 2px rgba(0,0,0,.04)}
.hero{padding:44px 40px 28px;border-bottom:1px solid var(--border)}
.hero-label{font-size:10px;font-family:var(--mono);color:var(--ink-4);text-transform:uppercase;letter-spacing:.15em;margin-bottom:12px;display:flex;align-items:center;gap:10px}
.hero-label::before{content:'';width:24px;height:1px;background:var(--ink-4)}
.hero-title{font-family:var(--display);font-weight:400;font-size:clamp(32px,4.5vw,46px);line-height:1.03;letter-spacing:-.035em;margin-bottom:14px}
.hero-title em{font-style:italic;color:var(--accent)}
.hero-sub{font-size:15px;color:var(--ink-3);max-width:600px;line-height:1.55}
.stats-row{padding:20px 40px;display:grid;grid-template-columns:repeat(5,1fr);gap:0;border-bottom:1px solid var(--border);background:var(--surface)}
.stats-row .stat{padding:0 24px;border-left:1px solid var(--border)}
.stats-row .stat:first-child{padding-left:0;border-left:none}
.stat-label{font-size:10px;font-family:var(--mono);color:var(--ink-4);text-transform:uppercase;letter-spacing:.14em;margin-bottom:8px;font-weight:500}
.stat-value{font-family:var(--display);font-weight:400;font-size:30px;line-height:1;letter-spacing:-.03em}
.tier-section{padding:36px 40px 8px}
.tier-header{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:20px;flex-wrap:wrap}
.tier-title{font-family:var(--display);font-weight:500;font-size:24px;letter-spacing:-.02em;display:flex;align-items:center;gap:10px}
.tier-badge{font-size:10px;font-family:var(--mono);color:var(--ink-4);background:var(--bg-subtle);padding:4px 10px;border-radius:100px;letter-spacing:.08em;text-transform:uppercase}
.tier-desc{font-size:13px;color:var(--ink-3)}
.slips-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:20px;padding:0 40px 20px}
.slip{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;transition:all .2s;position:relative}
.slip:hover{border-color:var(--border-strong);transform:translateY(-1px);box-shadow:0 4px 16px rgba(0,0,0,.03)}
.slip-safe{border-top:3px solid var(--positive)}
.slip-medium{border-top:3px solid var(--warning)}
.slip-value{border-top:3px solid var(--accent)}
.slip-mega{border-top:3px solid var(--mega)}
.slip-head{padding:16px 20px;background:var(--bg-subtle);display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--border)}
.slip-label{font-family:var(--display);font-weight:500;font-size:15px;letter-spacing:-.01em}
.slip-odds{font-family:var(--mono);font-weight:600;font-size:17px;color:var(--accent)}
.slip-body{padding:4px 0}
.match-row{padding:14px 20px;border-bottom:1px solid var(--border);display:flex;flex-direction:column;gap:6px}
.match-row:last-child{border-bottom:none}
.match-teams{font-weight:500;color:var(--ink);font-size:14px;line-height:1.3}
.match-meta{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--ink-4);font-family:var(--mono);flex-wrap:wrap}
.meta-league{background:var(--accent-soft);color:var(--accent);padding:2px 6px;border-radius:4px;font-weight:500}
.meta-sep{color:var(--border-strong)}
.pick-line{display:flex;align-items:center;gap:10px;margin-top:4px;flex-wrap:wrap}
.pick-type{font-size:11px;color:var(--ink-3);font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em}
.pick-value{font-weight:600;color:var(--accent);font-size:13px;background:var(--accent-soft);padding:2px 8px;border-radius:4px}
.pick-conf{font-family:var(--mono);font-size:11px;color:var(--ink-3);margin-left:auto}
.conf-bar{flex:1;max-width:60px;height:3px;background:var(--border);border-radius:2px;overflow:hidden;margin-left:10px}
.conf-bar-fill{height:100%;background:var(--positive);border-radius:2px}
.status-chip{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:100px;font-size:10px;font-weight:500;margin-left:6px}
.status-pending{background:var(--info-bg);color:var(--info)}
.status-won{background:var(--positive-bg);color:var(--positive)}
.status-lost{background:var(--negative-bg);color:var(--negative)}
.status-live{background:var(--warning-bg);color:var(--warning)}
.section-head{padding:40px 40px 16px;display:flex;align-items:baseline;justify-content:space-between;gap:14px;flex-wrap:wrap}
.section-title{font-family:var(--display);font-weight:500;font-size:22px;letter-spacing:-.02em}
.btn{font-family:var(--sans);font-size:13px;font-weight:500;padding:9px 16px;border-radius:8px;border:1px solid var(--border);background:var(--surface);color:var(--ink);cursor:pointer;display:inline-flex;align-items:center;gap:7px}
.btn-primary{background:var(--accent);color:var(--bg);border-color:var(--accent)}
.btn-primary:hover{background:var(--accent-muted)}
.otp-wrap,.hist-wrap{margin:0 40px 24px;background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden}
table{width:100%;border-collapse:collapse;font-size:13px;min-width:700px}
.table-scroll{overflow-x:auto}
thead{background:var(--bg-subtle);border-bottom:1px solid var(--border)}
thead th{text-align:left;padding:14px 16px;font-size:10px;font-family:var(--mono);font-weight:500;color:var(--ink-3);text-transform:uppercase;letter-spacing:.1em;white-space:nowrap}
thead th:first-child{padding-left:24px}
thead th:last-child{padding-right:24px}
tbody td{padding:14px 16px;border-bottom:1px solid var(--border);color:var(--ink-2)}
tbody td:first-child{padding-left:24px}
tbody td:last-child{padding-right:24px}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:var(--bg)}
.empty{padding:60px 40px;text-align:center;color:var(--ink-3)}
.empty-mark{width:52px;height:52px;border-radius:14px;background:var(--bg-subtle);display:inline-flex;align-items:center;justify-content:center;font-size:22px;margin-bottom:14px;border:1px solid var(--border)}
.empty h3{font-family:var(--display);font-size:19px;margin-bottom:10px;color:var(--ink);font-weight:500}
.empty p{font-size:14px;max-width:460px;margin:0 auto;line-height:1.6}
.footer{padding:28px 40px 48px;border-top:1px solid var(--border);text-align:center;font-size:11px;font-family:var(--mono);color:var(--ink-4);margin-top:24px}
@media(max-width:800px){.stats-row{grid-template-columns:repeat(3,1fr)}.stat:nth-child(n+4){border-top:1px solid var(--border);padding-top:16px;margin-top:16px}.hero,.stats-row,.tier-section,.section-head,.footer{padding-left:20px;padding-right:20px}.slips-grid,.otp-wrap,.hist-wrap{margin-left:20px;margin-right:20px;padding-left:0;padding-right:0}}
</style></head><body>
<div class="app">
<header class="hdr">
  <div class="brand"><div class="brand-mark"></div>
    <div class="brand-text"><h1>Limitless</h1><small>CMVNG · Football Picks</small></div></div>
  <div style="display:flex;gap:12px">
    <nav class="nav-tabs">
      <a href="/" class="nav-tab">Home</a>
      <a href="/app" class="nav-tab">Crypto</a>
      <a href="/app/football" class="nav-tab active">Football</a>
    </nav>
  </div>
</header>

<section class="hero">
  <div class="hero-label">Daily Accumulators</div>
  <h2 class="hero-title">Grouped picks,<br><em>calculated payouts.</em></h2>
  <p class="hero-sub">Four strategy tiers — 2x, 3x, 10x, 100x — each split into multiple independent slips. Pick the slip you like best, place it as a single accumulator bet. Past matches are filtered out automatically.</p>
</section>

<div class="stats-row">
  <div class="stat"><div class="stat-label">2x Slips</div><div class="stat-value" style="color:var(--positive)">{{ stats.safe_slips_count }}</div></div>
  <div class="stat"><div class="stat-label">3x Slips</div><div class="stat-value" style="color:var(--warning)">{{ stats.medium_slips_count }}</div></div>
  <div class="stat"><div class="stat-label">10x Slips</div><div class="stat-value" style="color:var(--accent)">{{ stats.value_slips_count }}</div></div>
  <div class="stat"><div class="stat-label">100x Slips</div><div class="stat-value" style="color:var(--mega)">{{ stats.mega_slips_count }}</div></div>
  <div class="stat"><div class="stat-label">OTP Picks</div><div class="stat-value">{{ otp_picks|length }}</div></div>
</div>

{% if not has_keys %}
<div class="empty">
  <div class="empty-mark">🔑</div>
  <h3>Setup required</h3>
  <p>Football module needs <code style="font-family:var(--mono);background:var(--bg-subtle);padding:2px 6px;border-radius:4px">ANTHROPIC_API_KEY</code> in Railway environment variables.</p>
</div>
{% elif acca_total == 0 and not otp_picks %}
<div class="empty">
  <div class="empty-mark">⚽</div>
  <h3>Building picks</h3>
  <p>The football analyzer runs every 6 hours. It scans tomorrow's fixtures, analyzes each match with AI, and builds multiple accumulator slips per tier. Manual trigger: hit <code>/scan</code> endpoint.</p>
</div>
{% else %}

{# Helper macro to render match meta - kickoff time + league #}
{% macro match_meta(pick) -%}
  <div class="match-meta">
    {% if pick.competition %}<span class="meta-league">{{ pick.competition }}</span>{% endif %}
    {% if pick.kickoff_time %}
      <span class="meta-sep">·</span>
      <span>{{ pick.kickoff_time[:16].replace("T"," ") }}</span>
    {% endif %}
  </div>
{%- endmacro %}

{% macro render_slip(slip, tier_class) %}
<div class="slip slip-{{ tier_class }}">
  <div class="slip-head">
    <div class="slip-label">Slip #{{ slip.slip_number }}</div>
    <div class="slip-odds">{{ "%.2f"|format(slip.total_odds) }}x</div>
  </div>
  <div class="slip-body">
    {% for pick in slip.picks %}
    <div class="match-row">
      <div class="match-teams">{{ pick.match_id or (pick.home_team + " vs " + pick.away_team) }}</div>
      {{ match_meta(pick) }}
      <div class="pick-line">
        <span class="pick-value">{{ pick.pick_value or "—" }}</span>
        <div class="conf-bar"><div class="conf-bar-fill" style="width:{{ pick.confidence|int }}%"></div></div>
        <span class="pick-conf">{{ pick.confidence|int }}%</span>
      </div>
      {% if pick.reasoning %}<div style="font-size:11px;color:var(--ink-4);margin-top:2px;font-style:italic">{{ pick.reasoning.split("| ", 1)[-1] if "| " in (pick.reasoning or "") else pick.reasoning }}</div>{% endif %}
    </div>
    {% endfor %}
  </div>
</div>
{% endmacro %}

{% if safe_slips %}
<div class="tier-section">
  <div class="tier-header">
    <div>
      <div class="tier-title">🟢 2x Slips <span class="tier-badge">Safe</span></div>
      <div class="tier-desc">High confidence picks · ~2x total payout per slip</div>
    </div>
  </div>
</div>
<div class="slips-grid">
  {% for slip in safe_slips %}{{ render_slip(slip, "safe") }}{% endfor %}
</div>
{% endif %}

{% if medium_slips %}
<div class="tier-section">
  <div class="tier-header">
    <div>
      <div class="tier-title">🟡 3x Slips <span class="tier-badge">Medium</span></div>
      <div class="tier-desc">Balanced risk · ~3x total payout per slip</div>
    </div>
  </div>
</div>
<div class="slips-grid">
  {% for slip in medium_slips %}{{ render_slip(slip, "medium") }}{% endfor %}
</div>
{% endif %}

{% if value_slips %}
<div class="tier-section">
  <div class="tier-header">
    <div>
      <div class="tier-title">🔥 10x Slips <span class="tier-badge">Value</span></div>
      <div class="tier-desc">Higher risk, higher reward · ~10x per slip</div>
    </div>
  </div>
</div>
<div class="slips-grid">
  {% for slip in value_slips %}{{ render_slip(slip, "value") }}{% endfor %}
</div>
{% endif %}

{% if mega_slips %}
<div class="tier-section">
  <div class="tier-header">
    <div>
      <div class="tier-title">🚀 100x Slips <span class="tier-badge">Mega</span></div>
      <div class="tier-desc">Long shot · massive payout potential</div>
    </div>
  </div>
</div>
<div class="slips-grid">
  {% for slip in mega_slips %}{{ render_slip(slip, "mega") }}{% endfor %}
</div>
{% endif %}

{% endif %}

<!-- Off The Pitch Section -->
<div class="section-head">
  <div><span class="section-title">Off The Pitch · Limitless</span></div>
  <button class="btn btn-primary" onclick="fetch('/otp/scan').then(r=>r.json()).then(d=>alert('OTP scan triggered. Wait ~30s then refresh.'))">◎ Scan OTP</button>
</div>

{% if otp_picks %}
<div class="otp-wrap">
  <div class="table-scroll">
    <table>
      <thead><tr>
        <th>Market</th><th>Pick</th><th>Odds</th><th>Conf</th><th>Reasoning</th><th>Status</th>
      </tr></thead>
      <tbody>
        {% for p in otp_picks %}
        <tr>
          <td style="font-weight:500;color:var(--ink);max-width:360px" title="{{ p.match_id }}">
            {% if p.away_team %}
              <a href="https://limitless.exchange/markets/{{ p.away_team }}" target="_blank" style="color:var(--ink);text-decoration:none">{{ p.match_id }}</a>
            {% else %}
              {{ p.match_id }}
            {% endif %}
          </td>
          <td><span class="pick-value">{{ p.pick_value }}</span></td>
          <td style="font-family:var(--mono);font-weight:600">{{ "%.1f"|format(p.implied_odds) }}%</td>
          <td><span class="pick-conf">{{ p.confidence|int }}%</span></td>
          <td style="color:var(--ink-3);font-size:12px;max-width:280px">{{ p.reasoning }}</td>
          <td><span class="status-chip status-{{ p.status|lower|replace(' ', '-')|replace('✅', 'won')|replace('❌', 'lost') }}">{{ p.status }}</span></td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% else %}
<div class="empty">
  <div class="empty-mark">⚽</div>
  <h3>No OTP picks yet</h3>
  <p>The Off The Pitch scanner pulls football prop markets from Limitless every 30 minutes. If the scanner runs but finds nothing, no sports markets are live in that window. Tap <b>Scan OTP</b> to trigger a manual scan now.</p>
</div>
{% endif %}

<!-- History Section -->
<div class="section-head">
  <div><span class="section-title">Performance Tracker</span></div>
  <button class="btn" onclick="fetch('/football/scan').then(()=>alert('Football scan triggered'))">⚡ Manual Scan</button>
</div>

<!-- Performance Stats Cards -->
<div class="slips-grid" style="margin-bottom:24px">
  {% for tier_key, tier_label in [('safe_2x','🟢 2x Safe'),('medium_3x','🟡 3x Medium'),('value_10x','🔥 10x Value'),('mega_100x','🚀 100x Mega')] %}
  <div class="slip" style="border-top:none">
    <div class="slip-head">
      <div class="slip-label">{{ tier_label }}</div>
      <div class="slip-odds">{{ perf[tier_key].rate }}%</div>
    </div>
    <div style="padding:16px 20px;display:flex;gap:24px">
      <div><span style="font-family:var(--mono);font-size:24px;color:var(--positive);font-weight:600">{{ perf[tier_key].w }}</span><br><small style="color:var(--ink-4)">Won</small></div>
      <div><span style="font-family:var(--mono);font-size:24px;color:var(--negative);font-weight:600">{{ perf[tier_key].l }}</span><br><small style="color:var(--ink-4)">Lost</small></div>
      <div><span style="font-family:var(--mono);font-size:24px;color:var(--ink-3);font-weight:600">{{ perf[tier_key].total }}</span><br><small style="color:var(--ink-4)">Total</small></div>
    </div>
  </div>
  {% endfor %}
</div>

{% if history_picks %}
<div class="hist-wrap">
  <div class="table-scroll">
    <table>
      <thead><tr>
        <th>Match</th><th>League</th><th>Tier</th><th>Pick</th><th>Conf</th><th>Reasoning</th><th>Outcome</th><th>Resolved</th>
      </tr></thead>
      <tbody>
        {% for p in history_picks %}
        <tr>
          <td style="font-weight:500;color:var(--ink);max-width:220px">{{ p.match_id }}</td>
          <td style="font-family:var(--mono);font-size:11px;color:var(--ink-3)">{{ p.competition or "—" }}</td>
          <td><span class="tier-badge">{{ p.accumulator_tier or "otp" }}</span></td>
          <td><span class="pick-value">{{ p.pick_value or "—" }}</span> <small style="color:var(--ink-4)">{{ (p.pick_type or "").replace("_", " ")[:25] }}</small></td>
          <td><span class="pick-conf">{{ p.confidence|int }}%</span></td>
          <td style="color:var(--ink-3);font-size:12px;max-width:200px">{{ p.reasoning or "—" }}</td>
          <td><span class="status-chip {{ 'status-won' if '✅' in (p.status or '') else 'status-lost' }}">{{ p.status }}</span></td>
          <td style="font-family:var(--mono);font-size:11px;color:var(--ink-4)">{{ p.resolved_at[:16].replace("T"," ") if p.resolved_at else "—" }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% else %}
<div class="empty" style="padding:32px">
  <p>No resolved picks yet. Results appear here after matches finish (checked every 5 minutes, 2 hours after kickoff).</p>
</div>
{% endif %}

<footer class="footer">Accumulators update every 6 hours · Past matches auto-filtered · Auto-refresh 60s</footer>
</div>

<script>setTimeout(()=>location.reload(),60000);</script>
</body></html>"""

LANDING_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cmvng Predictions</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #fafff5;
  --surface: #ffffff;
  --text: #0f1419;
  --text-2: #536471;
  --text-3: #8b98a5;
  --green: #22c55e;
  --green-dark: #16a34a;
  --green-light: #bbf7d0;
  --green-bg: #f0fdf4;
  --accent: #10b981;
  --border: #e5e7eb;
  --sans: 'DM Sans', -apple-system, sans-serif;
  --mono: 'Space Mono', monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:var(--sans);background:var(--bg);color:var(--text);line-height:1.6;-webkit-font-smoothing:antialiased}
.container{max-width:1200px;margin:0 auto;padding:0 24px}

/* NAV */
nav{background:var(--surface);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100;backdrop-filter:blur(12px);background:rgba(255,255,255,.85)}
.nav-inner{max-width:1200px;margin:0 auto;padding:20px 24px;display:flex;justify-content:space-between;align-items:center}
.logo{font-size:24px;font-weight:700;color:var(--green-dark);text-decoration:none;display:flex;align-items:center;gap:8px}
.logo-dot{width:8px;height:8px;background:var(--green);border-radius:50%;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(.85)}}
.nav-link{padding:10px 24px;background:var(--green);color:white;border-radius:8px;text-decoration:none;font-weight:600;font-size:14px;transition:all .2s;display:inline-flex;align-items:center;gap:6px}
.nav-link:hover{background:var(--green-dark);transform:translateY(-1px)}
.nav-link svg{width:16px;height:16px}

/* HERO */
.hero{padding:80px 0 60px;text-align:center}
.hero h1{font-size:clamp(40px,8vw,72px);font-weight:700;color:var(--text);margin-bottom:20px;line-height:1.1}
.hero h1 .highlight{color:var(--green);position:relative}
.hero p{font-size:20px;color:var(--text-2);max-width:680px;margin:0 auto 40px;font-weight:400}
.hero-cta{display:inline-flex;gap:12px;align-items:center}
.btn{padding:14px 32px;background:var(--green);color:white;border-radius:10px;text-decoration:none;font-weight:600;font-size:16px;transition:all .2s;display:inline-flex;align-items:center;gap:8px;border:2px solid var(--green)}
.btn:hover{background:var(--green-dark);border-color:var(--green-dark);transform:translateY(-2px);box-shadow:0 8px 24px rgba(34,197,94,.25)}
.btn svg{width:18px;height:18px}
.stats-bar{display:flex;justify-content:center;gap:48px;margin-top:60px;flex-wrap:wrap}
.stat{text-align:center}
.stat-value{font-size:36px;font-weight:700;color:var(--green-dark);font-family:var(--mono)}
.stat-label{font-size:13px;color:var(--text-3);text-transform:uppercase;letter-spacing:.05em;margin-top:4px}

/* FEATURES */
.features{padding:80px 0;background:var(--surface)}
.section-title{text-align:center;font-size:40px;font-weight:700;color:var(--text);margin-bottom:48px}
.features-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:32px;margin-top:48px}
.feature-card{background:var(--green-bg);border:2px solid var(--green-light);border-radius:16px;padding:40px;position:relative;overflow:hidden}
.feature-card::before{content:'';position:absolute;top:0;right:0;width:120px;height:120px;background:radial-gradient(circle,rgba(34,197,94,.12),transparent);border-radius:50%}
.feature-icon{width:56px;height:56px;background:var(--green);border-radius:12px;display:flex;align-items:center;justify-content:center;margin-bottom:24px;color:white;font-size:28px}
.feature-title{font-size:24px;font-weight:700;color:var(--text);margin-bottom:12px}
.feature-desc{font-size:15px;color:var(--text-2);line-height:1.6}

/* PREVIEW - Charts section */
.preview{padding:80px 0;background:var(--bg)}
.preview-title{text-align:center;font-size:40px;font-weight:700;color:var(--text);margin-bottom:60px}
.preview-grid{display:grid;grid-template-columns:1fr 1fr;gap:32px}
.preview-card{background:var(--surface);border:2px solid var(--border);border-radius:16px;padding:32px;position:relative}
.preview-card h3{font-size:18px;font-weight:700;color:var(--text);margin-bottom:24px;display:flex;align-items:center;gap:12px}
.preview-card h3::before{content:'';width:8px;height:8px;background:var(--green);border-radius:50%}

/* Mini bar chart for crypto signals */
.chart-bars{display:flex;align-items:flex-end;gap:8px;height:180px;margin-top:12px}
.bar{flex:1;background:var(--green-light);border-radius:4px 4px 0 0;position:relative;transition:all .3s;cursor:pointer}
.bar:hover{background:var(--green)}
.bar.positive{background:var(--green)}
.bar.negative{background:#fca5a5}
.bar-label{position:absolute;bottom:-24px;left:50%;transform:translateX(-50%);font-size:10px;color:var(--text-3);font-family:var(--mono);white-space:nowrap}

/* Football field graphic */
.football-field{width:100%;height:240px;background:linear-gradient(180deg,#22c55e 0%,#16a34a 100%);border-radius:12px;position:relative;overflow:hidden;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:12px;color:white}
.football-field::before{content:'';position:absolute;top:50%;left:0;right:0;height:2px;background:rgba(255,255,255,.3)}
.football-field::after{content:'';position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:80px;height:80px;border:2px solid rgba(255,255,255,.3);border-radius:50%}
.field-stat{font-size:48px;font-weight:700;font-family:var(--mono);z-index:1}
.field-label{font-size:14px;opacity:.9;z-index:1}

/* CTA */
.cta{padding:80px 0;text-align:center;background:linear-gradient(180deg,var(--green-bg) 0%,var(--surface) 100%)}
.cta h2{font-size:48px;font-weight:700;color:var(--text);margin-bottom:20px}
.cta p{font-size:18px;color:var(--text-2);margin-bottom:32px}

/* FOOTER */
footer{padding:40px 0;background:var(--surface);border-top:1px solid var(--border);text-align:center}
footer p{font-size:14px;color:var(--text-3)}
footer a{color:var(--green-dark);text-decoration:none;font-weight:600}
footer a:hover{text-decoration:underline}

@media(max-width:768px){
  .hero h1{font-size:36px}
  .hero p{font-size:18px}
  .stats-bar{gap:32px}
  .features-grid,.preview-grid{grid-template-columns:1fr}
  .nav-inner{padding:16px 20px}
}
</style>
</head><body>

<nav>
  <div class="nav-inner">
    <a href="/" class="logo"><span class="logo-dot"></span>Cmvng</a>
    <a href="/app" class="nav-link">Open Dashboard <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg></a>
  </div>
</nav>

<main>

<section class="hero">
  <div class="container">
    <h1>Smarter predictions for <span class="highlight">crypto</span> and <span class="highlight">football</span> markets</h1>
    <p>AI-powered scanner tracking {{ markets_total }} live markets on Limitless Exchange. Get instant Telegram alerts when real opportunities appear.</p>
    <div class="hero-cta">
      <a href="/app" class="btn">View Live Dashboard <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg></a>
    </div>

    <div class="stats-bar">
      <div class="stat">
        <div class="stat-value" data-count="{{ crypto_total }}">0</div>
        <div class="stat-label">Total Predictions</div>
      </div>
      <div class="stat">
        <div class="stat-value" data-count="{{ markets_total }}">0</div>
        <div class="stat-label">Markets Tracked</div>
      </div>
      <div class="stat">
        <div class="stat-value" data-count="{{ win_rate }}">0<span style="font-size:.6em">%</span></div>
        <div class="stat-label">Win Rate</div>
      </div>
    </div>
  </div>
</section>

<section class="features">
  <div class="container">
    <h2 class="section-title">Four prediction engines working for you</h2>
    <div class="features-grid">
      <div class="feature-card">
        <div class="feature-icon">₿</div>
        <div class="feature-title">Crypto Scanner</div>
        <div class="feature-desc">Monitors price movements every 5 minutes. Fires signals when trends align with your strategy rules and Lagos trading hours.</div>
      </div>
      <div class="feature-card">
        <div class="feature-icon">⚽</div>
        <div class="feature-title">Football Accumulators</div>
        <div class="feature-desc">Builds 4-tier betting slips targeting 2x, 3x, 10x, and 100x returns. Each match appears once across all tiers.</div>
      </div>
      <div class="feature-card">
        <div class="feature-icon">📊</div>
        <div class="feature-title">Off-the-Pitch Props</div>
        <div class="feature-desc">Analyzes player and match props using real bookmaker statistics. Hybrid engine combines heuristics with AI.</div>
      </div>
      <div class="feature-card">
        <div class="feature-icon">✓</div>
        <div class="feature-title">Auto Tracking</div>
        <div class="feature-desc">Every prediction is tracked and resolved automatically using live match data. See what actually works.</div>
      </div>
    </div>
  </div>
</section>

<section class="preview">
  <div class="container">
    <h2 class="preview-title">Live market intelligence</h2>
    <div class="preview-grid">
      
      <div class="preview-card">
        <h3>Crypto Signals</h3>
        <div class="chart-bars">
          <div class="bar positive" style="height:45%"><span class="bar-label">Mon</span></div>
          <div class="bar positive" style="height:68%"><span class="bar-label">Tue</span></div>
          <div class="bar negative" style="height:32%"><span class="bar-label">Wed</span></div>
          <div class="bar positive" style="height:78%"><span class="bar-label">Thu</span></div>
          <div class="bar positive" style="height:52%"><span class="bar-label">Fri</span></div>
          <div class="bar positive" style="height:85%"><span class="bar-label">Sat</span></div>
          <div class="bar positive" style="height:61%"><span class="bar-label">Sun</span></div>
        </div>
      </div>

      <div class="preview-card">
        <h3>Football Accumulators</h3>
        <div class="football-field">
          <div class="field-stat">{{ markets_total }}</div>
          <div class="field-label">Active markets this week</div>
        </div>
      </div>

    </div>
  </div>
</section>

<section class="cta">
  <div class="container">
    <h2>Start tracking predictions</h2>
    <p>Free access to live dashboard and Telegram alerts</p>
    <a href="/app" class="btn">Open Dashboard <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg></a>
  </div>
</section>

</main>

<footer>
  <div class="container">
    <p>Cmvng Predictions · <a href="/app">Dashboard</a> · <a href="/app/football">Football</a> · Built for Limitless Exchange</p>
  </div>
</footer>

<script>
// Count-up animation
const obs = new IntersectionObserver((entries) => {
  entries.forEach(e => {
    if (!e.isIntersecting || e.target.dataset.counted) return;
    e.target.dataset.counted = '1';
    const target = parseFloat(e.target.dataset.count) || 0;
    const duration = 1200;
    const start = performance.now();
    const tick = (now) => {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      const current = Math.round(target * eased);
      e.target.innerHTML = current.toLocaleString() + (e.target.innerHTML.includes('%') ? '<span style="font-size:.6em">%</span>' : '');
      if (progress < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
}, {threshold: 0.4});
document.querySelectorAll('[data-count]').forEach(el => obs.observe(el));
</script>

</body></html>"""




@app.route("/")
def landing():
    """Public marketing landing page."""
    try:
        conn = get_db()
        lp_rows = conn.run("SELECT COUNT(*), COALESCE(SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN outcome IN ('WIN','LOSS') THEN 1 ELSE 0 END), 0) FROM limitless_predictions")
        row = lp_rows[0] if lp_rows else (0, 0, 0)
        crypto_total = int(row[0] or 0)
        wins = int(row[1] or 0)
        resolved = int(row[2] or 0)
        conn.close()
    except Exception as e:
        print("Landing DB error: {}".format(e))
        crypto_total = 0; wins = 0; resolved = 0

    win_rate = round(wins / resolved * 100, 1) if resolved > 0 else 0
    markets_total = 933  # from Limitless category counts — real number

    return render_template_string(
        LANDING_HTML,
        crypto_total=crypto_total,
        win_rate=win_rate,
        markets_total=markets_total,
        btc_trend=_btc_trend_cache.get("trend"),
        in_window=is_lagos_window(),
    )

@app.route("/app")
def dashboard():
    try:
        conn = get_db()
        lp_rows = conn.run("SELECT * FROM limitless_predictions ORDER BY id DESC")
        lp_cols = [c['name'] for c in conn.columns]
        preds = [dict(zip(lp_cols, r)) for r in lp_rows]
        conn.close()
    except Exception as e:
        print("Dashboard DB error: {}".format(e))
        preds = []

    total = len(preds)
    wins = sum(1 for p in preds if p.get("outcome") == "WIN")
    losses = sum(1 for p in preds if p.get("outcome") == "LOSS")
    pending = sum(1 for p in preds if p.get("status") == "Pending")
    closed = wins + losses
    wr = round(wins / closed * 100, 1) if closed > 0 else 0
    today_str = datetime.now(LAGOS_TZ).strftime("%Y-%m-%d")
    today = sum(1 for p in preds if p.get("fired_at", "").startswith(today_str))
    stats = {"total": total, "wins": wins, "losses": losses,
             "pending": pending, "wr": wr, "today": today}
    return render_template_string(
        DASHBOARD_HTML, preds=preds, stats=stats,
        btc_trend=_btc_trend_cache.get("trend"),
        in_window=is_lagos_window()
    )

def _group_picks_into_slips(picks, target_odds, hard_max_picks=10):
    """Group picks into slips. If picks have 'Slip N' in reasoning (from save),
    group by that. Otherwise group sequentially by odds."""
    if not picks:
        return []

    # Check if picks have slip numbers embedded in reasoning
    slip_groups = {}
    has_slip_nums = False
    for pick in picks:
        reasoning = pick.get("reasoning", "") or ""
        if reasoning.startswith("Slip "):
            has_slip_nums = True
            try:
                slip_num = int(reasoning.split("|")[0].replace("Slip", "").strip())
                slip_groups.setdefault(slip_num, []).append(pick)
            except:
                slip_groups.setdefault(999, []).append(pick)
        else:
            slip_groups.setdefault(999, []).append(pick)

    if has_slip_nums and 999 not in slip_groups:
        # Use the pre-assigned slip numbers
        slips = []
        for slip_num in sorted(slip_groups.keys()):
            group = slip_groups[slip_num]
            total_odds = 1.0
            for p in group:
                total_odds *= float(p.get("implied_odds") or 1.0)
            slips.append({"picks": group, "total_odds": round(total_odds, 2), "slip_number": slip_num})
        return slips

    # Fallback: group sequentially
    sorted_picks = sorted(picks, key=lambda p: float(p.get("confidence") or 0), reverse=True)
    slips = []
    current_slip = []
    current_odds = 1.0
    slip_number = 1

    for pick in sorted_picks:
        odds = float(pick.get("implied_odds") or 1.0)
        if odds < 1.0:
            continue
        current_slip.append(pick)
        current_odds *= odds
        if current_odds >= target_odds:
            slips.append({"picks": current_slip, "total_odds": round(current_odds, 2),
                          "slip_number": slip_number})
            current_slip = []
            current_odds = 1.0
            slip_number += 1
        elif len(current_slip) >= hard_max_picks:
            current_slip = []
            current_odds = 1.0

    if not slips and current_slip and current_odds >= target_odds * 0.7:
        slips.append({"picks": current_slip, "total_odds": round(current_odds, 2),
                      "slip_number": slip_number})
    return slips

def _is_kickoff_future(kickoff_str):
    """Check if kickoff is in future or within last 15 mins (still live)."""
    if not kickoff_str:
        return True
    try:
        dt = datetime.fromisoformat(kickoff_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt > datetime.now(timezone.utc) - timedelta(minutes=15)
    except:
        return True

@app.route("/app/football")
def football_page():
    has_keys = bool(ANTHROPIC_KEY)
    try:
        conn = get_db()
        # OTP pending picks
        rows = conn.run(
            "SELECT * FROM football_picks WHERE pick_type='limitless_otp' "
            "AND status='Pending' ORDER BY id DESC LIMIT 50"
        )
        cols = [c['name'] for c in conn.columns]
        otp_picks = [dict(zip(cols, r)) for r in rows]

        # Recent resolved history (last 72hrs)
        rows_hist = conn.run(
            "SELECT * FROM football_picks "
            "WHERE status IN ('\u2705 Won', '\u274c Lost') "
            "AND resolved_at IS NOT NULL ORDER BY id DESC LIMIT 30"
        )
        cols_h = [c['name'] for c in conn.columns]
        history_picks = [dict(zip(cols_h, r)) for r in rows_hist]

        # Active accumulator picks (only Pending)
        rows2 = conn.run(
            "SELECT * FROM football_picks "
            "WHERE pick_type != 'limitless_otp' AND status='Pending' "
            "ORDER BY confidence DESC"
        )
        cols2 = [c['name'] for c in conn.columns]
        all_acca = [dict(zip(cols2, r)) for r in rows2]
        conn.close()
    except Exception as e:
        print("Football page error: {}".format(e))
        otp_picks = []
        history_picks = []
        all_acca = []

    # Filter: only keep picks for FUTURE matches
    all_acca = [p for p in all_acca if _is_kickoff_future(p.get("kickoff_time", ""))]

    # Split by tier
    safe_picks   = [p for p in all_acca if p.get("accumulator_tier") == "safe_2x"]
    medium_picks = [p for p in all_acca if p.get("accumulator_tier") == "medium_3x"]
    value_picks  = [p for p in all_acca if p.get("accumulator_tier") == "value_10x"]
    mega_picks   = [p for p in all_acca if p.get("accumulator_tier") == "mega_100x"]

    # Group each tier into multiple slips
    safe_slips   = _group_picks_into_slips(safe_picks,   target_odds=2.0,   hard_max_picks=8)
    medium_slips = _group_picks_into_slips(medium_picks, target_odds=3.0,   hard_max_picks=8)
    value_slips  = _group_picks_into_slips(value_picks,  target_odds=10.0,  hard_max_picks=10)
    mega_slips   = _group_picks_into_slips(mega_picks,   target_odds=100.0, hard_max_picks=12)

    stats = {
        "safe":   len(safe_picks),
        "medium": len(medium_picks),
        "value":  len(value_picks),
        "mega":   len(mega_picks),
        "safe_slips_count":   len(safe_slips),
        "medium_slips_count": len(medium_slips),
        "value_slips_count":  len(value_slips),
        "mega_slips_count":   len(mega_slips),
    }
    acca_total = len(all_acca)

    # Performance tracking — win/loss by tier
    perf = {"safe_2x": {"w": 0, "l": 0}, "medium_3x": {"w": 0, "l": 0},
            "value_10x": {"w": 0, "l": 0}, "mega_100x": {"w": 0, "l": 0}, "otp": {"w": 0, "l": 0}}
    try:
        conn3 = get_db()
        perf_rows = conn3.run(
            "SELECT accumulator_tier, outcome, COUNT(*) FROM football_picks "
            "WHERE outcome IN ('WIN','LOSS') GROUP BY accumulator_tier, outcome"
        )
        conn3.close()
        for tier, outcome, cnt in perf_rows:
            key = tier if tier in perf else ("otp" if "otp" in (tier or "") else None)
            if key:
                if outcome == "WIN":
                    perf[key]["w"] += cnt
                else:
                    perf[key]["l"] += cnt
    except Exception as e:
        print("Perf stats error: {}".format(e))

    # Calculate win rates
    for k in perf:
        total = perf[k]["w"] + perf[k]["l"]
        perf[k]["rate"] = round(perf[k]["w"] / total * 100, 1) if total > 0 else 0
        perf[k]["total"] = total

    return render_template_string(
        FOOTBALL_HTML,
        has_keys=has_keys,
        otp_picks=otp_picks,
        history_picks=history_picks,
        safe_slips=safe_slips,
        medium_slips=medium_slips,
        value_slips=value_slips,
        mega_slips=mega_slips,
        stats=stats,
        acca_total=acca_total,
        perf=perf,
    )

# ═══════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════

try:
    init_db()
except Exception as e:
    print("DB init error: {}".format(e))

threading.Thread(target=scan_loop, daemon=True).start()
threading.Thread(target=outcome_loop, daemon=True).start()
threading.Thread(target=football_loop, daemon=True).start()
threading.Thread(target=otp_loop, daemon=True).start()
print("Limitless Bot v3 — 3 threads running")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
