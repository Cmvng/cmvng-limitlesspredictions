from flask import Flask, request, jsonify, render_template_string
import requests
import pg8000.native
import os
import re
import threading
import time
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TWELVEDATA_KEY   = os.environ.get("TWELVEDATA_KEY", "")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
EXPIRY_DAYS      = 3
CHECK_INTERVAL   = 900

# Lagos timezone UTC+1
LAGOS_TZ      = timezone(timedelta(hours=1))
LIMITLESS_API = "https://api.limitless.exchange"

# Favourite hourly pairs - always qualify regardless of time window
FAVOURITE_HOURLY = ["ADA", "BNB", "HYPE"]

# Yahoo Finance symbol map for crypto assets on Limitless
YAHOO_MAP = {
    "BTC":"BTC-USD", "ETH":"ETH-USD", "SOL":"SOL-USD",
    "ADA":"ADA-USD", "BNB":"BNB-USD", "DOGE":"DOGE-USD",
    "XRP":"XRP-USD", "AVAX":"AVAX-USD","LINK":"LINK-USD",
    "LTC":"LTC-USD", "BCH":"BCH-USD", "XLM":"XLM-USD",
    "HYPE":"HYPE-USD","SUI":"SUI-USD", "ZEC":"ZEC-USD",
    "XMR":"XMR-USD", "ONDO":"ONDO-USD","MNT":"MNT-USD",
    "DOT":"DOT-USD", "UNI":"UNI-USD", "ATOM":"ATOM-USD",
}

PAIRS = {
    "XAUUSD_30M":  {"category": "Tier 1", "risk": 0.5,  "grade": "A - Strong"},
    "EURJPY_15M":  {"category": "Tier 1", "risk": 0.5,  "grade": "A - Strong"},
    "USDJPY_15M":  {"category": "Tier 1", "risk": 0.5,  "grade": "A - Strong"},
    "EURUSD_1H":   {"category": "Tier 1", "risk": 0.5,  "grade": "A - Strong"},
    "GBPJPY_30M":  {"category": "Tier 1", "risk": 0.5,  "grade": "A - Strong"},
    "GBPNZD_30M":  {"category": "Tier 1", "risk": 0.5,  "grade": "A - Strong"},
    "EURUSD_30M":  {"category": "Tier 1", "risk": 0.5,  "grade": "A - Strong"},
    "NZDCAD_1H":   {"category": "Tier 1", "risk": 0.5,  "grade": "A - Strong"},
    "XAGUSD_30M":  {"category": "Tier 2", "risk": 0.25, "grade": "B - Good"},
    "EURCHF_30M":  {"category": "Tier 2", "risk": 0.25, "grade": "B - Good"},
    "GBPUSD_1H":   {"category": "Tier 2", "risk": 0.25, "grade": "B - Good"},
    "USDCAD_1H":   {"category": "Tier 2", "risk": 0.25, "grade": "B - Good"},
    "EURNZD_15M":  {"category": "Tier 2", "risk": 0.25, "grade": "B - Good"},
    "ADAUSD_1H":   {"category": "Crypto", "risk": 0.1,  "grade": "A - Strong"},
    "HYPEUSD_15M": {"category": "Crypto", "risk": 0.1,  "grade": "A - Strong"},
    "BNBUSD_1H":   {"category": "Crypto", "risk": 0.1,  "grade": "B - Good"},
    "BTCUSD_15M":  {"category": "Crypto", "risk": 0.1,  "grade": "A - Strong"},
    "ZECUSD_15M":  {"category": "Crypto", "risk": 0.1,  "grade": "A - Strong"},
}

SYMBOL_MAP = {
    "XAUUSD": "XAU/USD", "EURJPY": "EUR/JPY", "USDJPY": "USD/JPY",
    "EURUSD": "EUR/USD", "GBPJPY": "GBP/JPY", "EURCHF": "EUR/CHF",
    "GBPUSD": "GBP/USD", "EURNZD": "EUR/NZD", "ADAUSD": "ADA/USD",
    "BTCUSD": "BTC/USD", "XAGUSD": "XAG/USD", "NZDCAD": "NZD/CAD",
    "GBPNZD": "GBP/NZD", "USDCAD": "USD/CAD", "GBPCAD": "GBP/CAD",
    "CADJPY": "CAD/JPY", "AUDUSD": "AUD/USD", "EURCAD": "EUR/CAD",
    "BNBUSD": "BNB/USD", "ZECUSD": "ZEC/USD", "SOLUSD": "SOL/USD",
    "ETHUSD": "ETH/USD", "XRPUSD": "XRP/USD", "HYPEUSD": "HYPE/USD",
}

# ═══════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════

def get_db():
    import urllib.parse
    db_url = DATABASE_URL.replace('postgres://', 'postgresql://')
    url    = urllib.parse.urlparse(db_url)
    return pg8000.native.Connection(
        host=url.hostname, port=url.port or 5432,
        database=url.path.lstrip('/'),
        user=url.username, password=url.password,
        ssl_context=True
    )

def init_db():
    conn = get_db()
    # Original signals table
    conn.run("""
        CREATE TABLE IF NOT EXISTS signals (
            id        SERIAL PRIMARY KEY,
            pair      TEXT,
            timeframe TEXT,
            direction TEXT,
            entry     REAL,
            sl        REAL,
            tp        REAL,
            rr        REAL,
            risk      REAL,
            category  TEXT,
            grade     TEXT,
            status    TEXT DEFAULT 'Pending',
            fired_at  TEXT,
            closed_at TEXT
        )
    """)
    # New Limitless predictions table
    conn.run("""
        CREATE TABLE IF NOT EXISTS limitless_predictions (
            id            SERIAL PRIMARY KEY,
            market_id     TEXT,
            title         TEXT,
            asset         TEXT,
            direction     TEXT,
            baseline      REAL,
            bet_side      TEXT,
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
            slug          TEXT
        )
    """)
    conn.close()

# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            "https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_TOKEN),
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print("Telegram error: {}".format(e))

# ═══════════════════════════════════════════════════════════
# TWELVEDATA — batch price fetch (Cmvng signals)
# ═══════════════════════════════════════════════════════════

def get_prices_batch(pairs):
    symbols = []
    pair_to_symbol = {}
    for pair in pairs:
        symbol = SYMBOL_MAP.get(pair.upper())
        if symbol:
            symbols.append(symbol)
            pair_to_symbol[pair.upper()] = symbol
    if not symbols or not TWELVEDATA_KEY:
        return {}
    try:
        r    = requests.get(
            "https://api.twelvedata.com/price?symbol={}&apikey={}".format(",".join(symbols), TWELVEDATA_KEY),
            timeout=15
        )
        data = r.json()
        prices = {}
        for pair in pairs:
            symbol = pair_to_symbol.get(pair.upper())
            if not symbol:
                continue
            if symbol in data and "price" in data[symbol]:
                prices[pair.upper()] = float(data[symbol]["price"])
            elif "price" in data and len(symbols) == 1:
                prices[pair.upper()] = float(data["price"])
        return prices
    except Exception as e:
        print("TwelveData error: {}".format(e))
        return {}

# ═══════════════════════════════════════════════════════════
# YAHOO FINANCE — crypto price for Limitless pairs
# ═══════════════════════════════════════════════════════════

def get_yahoo_price(asset):
    symbol = YAHOO_MAP.get(asset.upper())
    if not symbol:
        return None
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1m&range=1d".format(symbol)
        r   = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        return float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception as e:
        print("Yahoo Finance error {}: {}".format(asset, e))
        return None

# ═══════════════════════════════════════════════════════════
# LIMITLESS HELPERS
# ═══════════════════════════════════════════════════════════

def is_lagos_window():
    """Returns True if current Lagos time is 5am-12pm or 6pm-12am"""
    hour = datetime.now(LAGOS_TZ).hour
    return (5 <= hour < 12) or (18 <= hour < 24)

def get_btc_trend():
    """Read BTC direction from most recent BTCUSD signal (only if < 4hrs old)"""
    try:
        conn = get_db()
        rows = conn.run(
            "SELECT direction, fired_at FROM signals WHERE pair='BTCUSD' ORDER BY id DESC LIMIT 1"
        )
        conn.close()
        if rows:
            direction, fired_at = rows[0][0], rows[0][1]
            if fired_at:
                dt = datetime.fromisoformat(fired_at)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - dt).total_seconds() / 3600 <= 4:
                    return direction
    except Exception as e:
        print("BTC trend error: {}".format(e))
    return None

def parse_market(market):
    """Extract asset, direction, baseline, timing from a Limitless market"""
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

    prices   = market.get("prices", [50, 50])
    yes_odds = float(prices[0]) if prices else 50.0
    tags      = market.get("tags", [])
    cats      = market.get("categories", [])
    is_hourly = "Hourly" in tags or "Hourly" in cats

    return {
        "market_id": str(market.get("id", "")),
        "title":     title,
        "asset":     asset,
        "direction": direction,
        "baseline":  baseline,
        "expiry_dt": expiry_dt,
        "mins_left": mins_left,
        "hours_left":hours_left,
        "yes_odds":  yes_odds,
        "is_hourly": is_hourly,
        "slug":      market.get("slug", ""),
    }

def score_market(p, btc_trend, price):
    """
    Apply all your rules. Returns score dict or None if market should be skipped.
    Rules applied:
      1. Lagos trading window (5am-12pm, 6pm-12am) — bypassed for favourite pairs
      2. Time to expiry: hourly 5-15 mins / daily 0.5-10 hrs (favourites skip)
      3. Price must already be on winning side
      4. Odds must be 75-97%
      5. BTC trend alignment check
    """
    is_fav = p["asset"] in FAVOURITE_HOURLY

    # 1. Time window
    if not is_lagos_window() and not is_fav:
        return None

    # 2. Expiry filter
    if p["is_hourly"]:
        if not is_fav and not (5 <= p["mins_left"] <= 15):
            return None
    else:
        if p["hours_left"] < 0.5:
            return None
        if p["hours_left"] > 10 and not is_fav:
            return None

    # 3. Price must exist
    if price is None:
        return None

    # 4. Price on winning side
    if p["direction"] == "above":
        if price <= p["baseline"]:
            return None
        btc_aligned = (btc_trend == "BUY") if btc_trend else True
    else:
        if price >= p["baseline"]:
            return None
        btc_aligned = (btc_trend == "SELL") if btc_trend else True

    odds = p["yes_odds"]

    # 5. Odds filter
    if not (75 <= odds <= 97):
        return None

    # Confidence
    if not btc_aligned and btc_trend:
        confidence = "MEDIUM"
    elif odds >= 90 or (odds >= 80 and btc_aligned):
        confidence = "HIGH"
    else:
        confidence = "MEDIUM"

    # Size recommendation
    if odds >= 94:
        size_rec = "$20-50 (high odds — go with size)"
    elif odds >= 85:
        size_rec = "$10-20 (normal size)"
    else:
        size_rec = "$5-10 (cautious)"

    # Reversal warning
    reversal = ""
    if p["is_hourly"] and p["mins_left"] <= 60 and 78 <= odds <= 88:
        reversal = "⚠️ Reversal risk — last hour, watch carefully"

    return {
        "bet_odds":   odds,
        "confidence": confidence,
        "size_rec":   size_rec,
        "margin":     abs(price - p["baseline"]),
        "reversal":   reversal,
        "btc_aligned":btc_aligned,
    }

def fmt_price(v):
    if v is None:
        return "-"
    return "${:,.4f}".format(v) if v < 100 else "${:,.2f}".format(v)

def save_and_alert(p, score, price, btc_trend):
    """Save prediction to DB and send Telegram alert"""
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn = get_db()
        rows = conn.run(
            """INSERT INTO limitless_predictions
            (market_id,title,asset,direction,baseline,bet_side,bet_odds,confidence,
             size_rec,current_price,hours_left,market_type,status,fired_at,slug)
            VALUES (:mid,:ttl,:ast,:dir,:base,'YES',:odds,:conf,:sz,:pr,:hrs,:mt,'Pending',:now,:slg)
            RETURNING id""",
            mid=p["market_id"], ttl=p["title"], ast=p["asset"],
            dir=p["direction"], base=p["baseline"],
            odds=score["bet_odds"], conf=score["confidence"], sz=score["size_rec"],
            pr=price, hrs=round(p["hours_left"], 2),
            mt="Hourly" if p["is_hourly"] else "Daily",
            now=now, slg=p["slug"]
        )
        pid = rows[0][0]
        conn.close()

        trend_str  = "🟢 Bullish" if btc_trend == "BUY" else "🔴 Bearish" if btc_trend == "SELL" else "⚪ Unknown"
        conf_emoji = "🔥" if score["confidence"] == "HIGH" else "🟡"
        hrs_str    = "{:.1f} hrs".format(p["hours_left"]) if p["hours_left"] >= 1 else "{:.0f} mins".format(p["mins_left"])
        exp_str    = p["expiry_dt"].strftime("%d %b %H:%M UTC")

        msg = (
            "🎯 <b>LIMITLESS PREDICTION #{}</b>\n"
            "──────────────────────────\n"
            "📌 {}\n"
            "──────────────────────────\n"
            "<b>Bet:</b> YES ✅\n"
            "<b>Odds:</b> {:.1f}% chance\n"
            "<b>Current Price:</b> {}\n"
            "<b>Baseline:</b> {}\n"
            "<b>Margin {} baseline:</b> {}\n"
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
            score["bet_odds"],
            fmt_price(price), fmt_price(p["baseline"]),
            p["direction"], fmt_price(score["margin"]),
            hrs_str, exp_str,
            "Hourly ⏱" if p["is_hourly"] else "Daily 📅",
            conf_emoji, score["confidence"],
            score["size_rec"], trend_str,
            score["reversal"] + "\n" if score["reversal"] else "",
            p["slug"]
        )
        send_telegram(msg)
        print("Limitless alert #{}: {}".format(pid, p["title"]))
    except Exception as e:
        print("Alert error: {}".format(e))

# ═══════════════════════════════════════════════════════════
# LIMITLESS SCANNER — runs every 5 minutes
# ═══════════════════════════════════════════════════════════

def run_scan():
    """Single scan pass — used by background thread and manual trigger"""
    try:
        btc_trend = get_btc_trend()
        r = requests.get("{}/markets/active".format(LIMITLESS_API), timeout=15)
        if r.status_code != 200:
            print("Limitless API error: {}".format(r.status_code))
            return 0
        markets = r.json().get("data", [])
        print("Scan: {} markets, BTC={}".format(len(markets), btc_trend))

        # Get already-alerted market IDs in the last 6 hours (avoid duplicates)
        conn = get_db()
        alerted_rows = conn.run(
            "SELECT market_id FROM limitless_predictions WHERE fired_at > NOW() - INTERVAL '6 hours'"
        )
        alerted_ids = set(str(row[0]) for row in alerted_rows)
        conn.close()

        count = 0
        for market in markets:
            try:
                parsed = parse_market(market)
                if not parsed or parsed["market_id"] in alerted_ids:
                    continue
                price  = get_yahoo_price(parsed["asset"])
                scored = score_market(parsed, btc_trend, price)
                if not scored:
                    continue
                save_and_alert(parsed, scored, price, btc_trend)
                alerted_ids.add(parsed["market_id"])
                count += 1
                time.sleep(1)  # Small gap between alerts
            except Exception as e:
                print("Market error: {}".format(e))

        print("Scan done: {}/{} qualified".format(count, len(markets)))
        return count
    except Exception as e:
        print("Scanner error: {}".format(e))
        return 0

def scan_loop():
    time.sleep(30)  # Wait for full boot
    while True:
        run_scan()
        time.sleep(300)  # Every 5 minutes

# ═══════════════════════════════════════════════════════════
# LIMITLESS OUTCOME CHECKER — resolves expired predictions
# ═══════════════════════════════════════════════════════════

def outcome_loop():
    while True:
        try:
            conn  = get_db()
            rows  = conn.run("SELECT * FROM limitless_predictions WHERE status='Pending'")
            cols  = [c['name'] for c in conn.columns]
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
                        continue  # Not expired yet

                    price = get_yahoo_price(p["asset"])
                    if price is None:
                        continue

                    won    = (price > p["baseline"]) if p["direction"] == "above" else (price < p["baseline"])
                    outcome = "WIN" if won else "LOSS"
                    status  = "✅ Won" if won else "❌ Lost"

                    conn2 = get_db()
                    conn2.run(
                        "UPDATE limitless_predictions SET status=:s, outcome=:o, resolved_at=:r WHERE id=:i",
                        s=status, o=outcome, r=now.isoformat(), i=p["id"]
                    )
                    conn2.close()

                    emoji = "✅" if won else "❌"
                    send_telegram(
                        "{} <b>PREDICTION {} — #{}</b>\n"
                        "──────────────────────────\n"
                        "📌 {}\n"
                        "<b>Closed price:</b> {}\n"
                        "<b>Baseline was:</b> {}".format(
                            emoji, outcome, p["id"], p["title"],
                            fmt_price(price), fmt_price(p["baseline"])
                        )
                    )
                    print("Prediction #{} -> {}".format(p["id"], outcome))
                except Exception as e:
                    print("Outcome error #{}: {}".format(p["id"], e))
        except Exception as e:
            print("Outcome loop error: {}".format(e))
        time.sleep(300)

# ═══════════════════════════════════════════════════════════
# CMVNG SIGNAL MONITOR (original — unchanged)
# ═══════════════════════════════════════════════════════════

def update_signal_auto(sig_id, status, pair, direction, price=None, tp=None, sl=None):
    conn = get_db()
    conn.run("UPDATE signals SET status=:s, closed_at=:c WHERE id=:i",
             s=status, c=datetime.now(timezone.utc).isoformat(), i=sig_id)
    conn.close()
    if status == "TP Hit":
        msg = "✅ <b>TP HIT — {} {}</b>\nPrice: {} | TP: {}\n🆔 Signal #{}".format(pair, direction, price, tp, sig_id)
    elif status == "SL Hit":
        msg = "❌ <b>SL HIT — {} {}</b>\nPrice: {} | SL: {}\n🆔 Signal #{}".format(pair, direction, price, sl, sig_id)
    else:
        msg = "⏰ <b>EXPIRED — {} {}</b> (3 days)\n🆔 Signal #{}".format(pair, direction, sig_id)
    send_telegram(msg)

def check_pending_signals():
    while True:
        try:
            conn    = get_db()
            rows    = conn.run("SELECT * FROM signals WHERE status = 'Pending'")
            cols    = [c['name'] for c in conn.columns]
            pending = [dict(zip(cols, r)) for r in rows]
            conn.close()
            if not pending:
                time.sleep(CHECK_INTERVAL)
                continue
            unique_pairs = list(set(s["pair"] for s in pending))
            prices = get_prices_batch(unique_pairs)
            print("Monitor: {} pairs, {} pending".format(len(unique_pairs), len(pending)))
            for s in pending:
                try:
                    fired_dt = datetime.fromisoformat(s["fired_at"])
                    if fired_dt.tzinfo is None:
                        fired_dt = fired_dt.replace(tzinfo=timezone.utc)
                    if datetime.now(timezone.utc) - fired_dt > timedelta(days=EXPIRY_DAYS):
                        update_signal_auto(s["id"], "Expired", s["pair"], s["direction"])
                        continue
                    price = prices.get(s["pair"].upper())
                    if price is None:
                        continue
                    if s["direction"] == "BUY":
                        if price >= s["tp"]:
                            update_signal_auto(s["id"], "TP Hit", s["pair"], s["direction"], price, s["tp"], s["sl"])
                        elif price <= s["sl"]:
                            update_signal_auto(s["id"], "SL Hit", s["pair"], s["direction"], price, s["tp"], s["sl"])
                    elif s["direction"] == "SELL":
                        if price <= s["tp"]:
                            update_signal_auto(s["id"], "TP Hit", s["pair"], s["direction"], price, s["tp"], s["sl"])
                        elif price >= s["sl"]:
                            update_signal_auto(s["id"], "SL Hit", s["pair"], s["direction"], price, s["tp"], s["sl"])
                except Exception as e:
                    print("Signal #{} error: {}".format(s["id"], e))
        except Exception as e:
            print("Monitor error: {}".format(e))
        time.sleep(CHECK_INTERVAL)

# ═══════════════════════════════════════════════════════════
# ROUTES — original
# ═══════════════════════════════════════════════════════════

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data      = request.get_json(force=True)
        pair      = data.get("pair", "").upper()
        timeframe = data.get("timeframe", "").upper()
        direction = data.get("direction", "").upper()
        entry     = float(data.get("entry", 0))
        sl        = float(data.get("sl", 0))
        tp        = float(data.get("tp", 0))
        key       = "{}_{}".format(pair, timeframe)
        cfg       = PAIRS.get(key, {"category": "Unknown", "risk": 0.1, "grade": "Unrated"})
        sl_dist   = abs(entry - sl)
        tp_dist   = abs(tp - entry)
        rr        = round(tp_dist / sl_dist, 2) if sl_dist > 0 else 0
        now       = datetime.now(timezone.utc).isoformat()
        conn      = get_db()
        result    = conn.run(
            """INSERT INTO signals
            (pair,timeframe,direction,entry,sl,tp,rr,risk,category,grade,status,fired_at)
            VALUES (:pair,:tf,:dir,:entry,:sl,:tp,:rr,:risk,:cat,:grade,'Pending',:now) RETURNING id""",
            pair=pair, tf=timeframe, dir=direction, entry=entry,
            sl=sl, tp=tp, rr=rr, risk=cfg["risk"],
            cat=cfg["category"], grade=cfg["grade"], now=now
        )
        signal_id = result[0][0]
        conn.close()
        is_jpy    = "JPY" in pair
        is_metal  = "XAU" in pair or "XAG" in pair
        dec       = 2 if (is_jpy or is_metal) else 5
        emoji     = "🟢" if direction == "BUY" else "🔴"
        ts        = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
        msg = (
            "{} <b>{} SIGNAL — {}</b>\n"
            "──────────────────────\n"
            "<b>Timeframe :</b> {}\n"
            "<b>Entry     :</b> {:.{}f}\n"
            "<b>Stop Loss :</b> {:.{}f}\n"
            "<b>Take Profit:</b> {:.{}f}\n"
            "──────────────────────\n"
            "<b>Risk      :</b> {}% ({})\n"
            "<b>RR        :</b> 1 : {:.1f}\n"
            "<b>Rating    :</b> {}\n"
            "──────────────────────\n"
            "⏰ Expires in 3 days\n"
            "📅 {}\n"
            "🆔 Signal #{}"
        ).format(emoji, direction, pair, timeframe,
                 entry, dec, sl, dec, tp, dec,
                 cfg["risk"], cfg["category"], rr, cfg["grade"], ts, signal_id)
        send_telegram(msg)
        return jsonify({"status": "ok", "signal_id": signal_id}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/update/<int:signal_id>/<status>", methods=["POST"])
def update_signal(signal_id, status):
    if status not in ["TP Hit", "SL Hit", "Expired", "Pending"]:
        return jsonify({"error": "Invalid status"}), 400
    conn = get_db()
    conn.run("UPDATE signals SET status=:s, closed_at=:c WHERE id=:i",
             s=status, c=datetime.now(timezone.utc).isoformat(), i=signal_id)
    conn.close()
    emoji = "✅" if status == "TP Hit" else "❌" if status == "SL Hit" else "⏰"
    send_telegram("{} Signal #{} — <b>{}</b>".format(emoji, signal_id, status))
    return jsonify({"status": "updated"}), 200

@app.route("/add", methods=["POST"])
def add_signal():
    try:
        data      = request.get_json(force=True)
        pair      = data.get("pair", "").upper()
        timeframe = data.get("timeframe", "").upper()
        direction = data.get("direction", "").upper()
        entry     = float(data.get("entry", 0))
        sl        = float(data.get("sl", 0))
        tp        = float(data.get("tp", 0))
        fired_at  = data.get("fired_at", datetime.now(timezone.utc).isoformat())
        key       = "{}_{}".format(pair, timeframe)
        cfg       = PAIRS.get(key, {"category": "Unknown", "risk": 0.1, "grade": "Unrated"})
        sl_dist   = abs(entry - sl)
        tp_dist   = abs(tp - entry)
        rr        = round(tp_dist / sl_dist, 2) if sl_dist > 0 else 0
        conn = get_db()
        result = conn.run(
            """INSERT INTO signals
            (pair,timeframe,direction,entry,sl,tp,rr,risk,category,grade,status,fired_at)
            VALUES (:pair,:tf,:dir,:entry,:sl,:tp,:rr,:risk,:cat,:grade,'Pending',:now) RETURNING id""",
            pair=pair, tf=timeframe, dir=direction, entry=entry,
            sl=sl, tp=tp, rr=rr, risk=cfg["risk"],
            cat=cfg["category"], grade=cfg["grade"], now=fired_at
        )
        signal_id = result[0][0]
        conn.close()
        return jsonify({"status": "ok", "signal_id": signal_id}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ═══════════════════════════════════════════════════════════
# ROUTES — new Limitless endpoints
# ═══════════════════════════════════════════════════════════

@app.route("/limitless/update/<int:pred_id>/<status>", methods=["POST"])
def update_limitless(pred_id, status):
    if status not in ["✅ Won", "❌ Lost", "Pending"]:
        return jsonify({"error": "Invalid status"}), 400
    outcome = "WIN" if status == "✅ Won" else "LOSS" if status == "❌ Lost" else ""
    conn = get_db()
    conn.run("UPDATE limitless_predictions SET status=:s,outcome=:o,resolved_at=:r WHERE id=:i",
             s=status, o=outcome, r=datetime.now(timezone.utc).isoformat(), i=pred_id)
    conn.close()
    return jsonify({"status": "updated"}), 200

@app.route("/limitless/scan", methods=["GET"])
def manual_scan():
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({"status": "scan triggered — check Telegram in ~30 seconds"}), 200

# ═══════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════

DASHBOARD_HTML = """<!DOCTYPE html>
<html><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cmvng Platform</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,Arial,sans-serif;background:#0d1117;color:#e6edf3}
.hdr{padding:16px 24px;background:#161b22;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #30363d;flex-wrap:wrap;gap:10px}
.hdr h1{font-size:16px;font-weight:700;color:#fff}
.hdr-r{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.live{font-size:12px;color:#3fb950;display:flex;align-items:center;gap:5px}
.dot{width:7px;height:7px;background:#3fb950;border-radius:50%;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.badge{font-size:11px;padding:3px 10px;border-radius:20px;font-weight:700}
.bg{background:#238636;color:#fff}
.bo{background:#1a4731;color:#3fb950;border:1px solid #238636}
.bc{background:#4a1520;color:#f85149;border:1px solid #da3633}
.tabs{display:flex;background:#161b22;border-bottom:1px solid #30363d;padding:0 24px}
.tab{padding:12px 16px;font-size:13px;font-weight:600;cursor:pointer;color:#8b949e;border-bottom:2px solid transparent;white-space:nowrap}
.tab.active{color:#58a6ff;border-bottom-color:#58a6ff}
.pane{display:none;padding:20px 24px}
.pane.active{display:block}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px;margin-bottom:20px}
.stat{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px}
.slbl{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;font-weight:700}
.sval{font-size:20px;font-weight:700}
.g{color:#3fb950}.r{color:#f85149}.a{color:#e3b341}.b{color:#58a6ff}
.pgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(175px,1fr));gap:10px;margin-bottom:20px}
.pc{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px;border-left:3px solid #238636}
.pn{font-size:13px;font-weight:700;margin-bottom:8px}
.pnums{display:flex;gap:12px}
.pnum{font-size:10px;color:#8b949e;font-weight:700;text-transform:uppercase}
.pnum span{display:block;font-size:14px;font-weight:700;color:#e6edf3;margin-top:1px}
.stit{font-size:11px;font-weight:700;color:#8b949e;text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px}
.sbtn{background:#238636;color:#fff;border:none;padding:8px 16px;border-radius:8px;font-size:13px;font-weight:700;cursor:pointer;margin-bottom:16px}
.tw{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:12px;min-width:680px}
th{text-align:left;padding:9px 10px;font-size:10px;color:#8b949e;text-transform:uppercase;border-bottom:1px solid #30363d;background:#161b22;font-weight:700}
td{padding:9px 10px;border-bottom:1px solid #21262d}
tr:hover td{background:#1c2128}
.bdg{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:700}
.pnd{background:#1f4068;color:#58a6ff}
.won{background:#1a4731;color:#3fb950}
.lost{background:#4a1520;color:#f85149}
.exp{background:#2d2d2d;color:#8b949e}
.tph{background:#1a4731;color:#3fb950}
.slh{background:#4a1520;color:#f85149}
.chigh{background:#1a4731;color:#3fb950}
.cmed{background:#3d2f0a;color:#e3b341}
.buy{color:#3fb950;font-weight:700}.sell{color:#f85149;font-weight:700}
.t1{background:#1a4731;color:#3fb950;padding:2px 7px;border-radius:20px;font-size:10px;font-weight:700}
.t2{background:#3d2f0a;color:#e3b341;padding:2px 7px;border-radius:20px;font-size:10px;font-weight:700}
.cry{background:#2d1f42;color:#a78bfa;padding:2px 7px;border-radius:20px;font-size:10px;font-weight:700}
.btn{padding:3px 8px;border-radius:5px;border:1px solid;cursor:pointer;font-size:10px;font-weight:700;margin-right:2px;background:transparent}
.btp{color:#3fb950;border-color:#238636}
.bsl{color:#f85149;border-color:#da3633}
.bex{color:#8b949e;border-color:#30363d}
.empty{text-align:center;padding:32px;color:#8b949e}
.ref{font-size:11px;color:#30363d;text-align:right;padding:10px 24px}
</style></head><body>
<div class="hdr">
  <h1>⚡ Cmvng Prediction Platform</h1>
  <div class="hdr-r">
    <span class="badge {{ 'bo' if in_window else 'bc' }}">
      {{ '🟢 Trading Window OPEN' if in_window else '🔴 Outside Trading Hours' }}
    </span>
    <span class="badge bg">Auto-monitoring ON</span>
    <div class="live"><div class="dot"></div> Live</div>
  </div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('signals',this)">📊 Cmvng Signals</div>
  <div class="tab" onclick="showTab('limitless',this)">🎯 Limitless Predictions</div>
</div>

<!-- SIGNALS TAB -->
<div class="pane active" id="tab-signals">
  <div class="stats">
    <div class="stat"><div class="slbl">Total</div><div class="sval b">{{ stats.total }}</div></div>
    <div class="stat"><div class="slbl">Win Rate</div><div class="sval {{ 'g' if stats.wr >= 45 else 'a' if stats.wr >= 35 else 'r' }}">{{ stats.wr }}%</div></div>
    <div class="stat"><div class="slbl">Prof Factor</div><div class="sval {{ 'g' if stats.pf >= 1.4 else 'a' if stats.pf >= 1.0 else 'r' }}">{{ stats.pf }}</div></div>
    <div class="stat"><div class="slbl">TP Hit</div><div class="sval g">{{ stats.tp }}</div></div>
    <div class="stat"><div class="slbl">SL Hit</div><div class="sval r">{{ stats.sl }}</div></div>
    <div class="stat"><div class="slbl">Pending</div><div class="sval a">{{ stats.pending }}</div></div>
  </div>
  {% if pair_stats %}
  <div class="stit">Pair Performance</div>
  <div class="pgrid">
    {% for p in pair_stats %}
    <div class="pc">
      <div class="pn">{{ p.pair }} <span style="color:#8b949e;font-weight:400;font-size:11px">{{ p.timeframe }}</span></div>
      <div class="pnums">
        <div class="pnum">Total<span>{{ p.total }}</span></div>
        <div class="pnum">Wins<span class="g">{{ p.tp }}</span></div>
        <div class="pnum">Loss<span class="r">{{ p.sl }}</span></div>
      </div>
    </div>
    {% endfor %}
  </div>
  {% endif %}
  <div class="stit">Signal Log</div>
  <div class="tw"><table>
    <thead><tr><th>#</th><th>Pair</th><th>TF</th><th>Dir</th><th>Entry</th><th>SL</th><th>TP</th><th>RR</th><th>Risk</th><th>Cat</th><th>Status</th><th>Time</th><th>Action</th></tr></thead>
    <tbody>
      {% if not signals %}<tr><td colspan="13"><div class="empty">📡 No signals yet</div></td></tr>{% endif %}
      {% for s in signals %}
      <tr>
        <td style="color:#8b949e">{{ s.id }}</td>
        <td style="font-weight:700">{{ s.pair }}</td>
        <td style="color:#8b949e">{{ s.timeframe }}</td>
        <td class="{{ 'buy' if s.direction == 'BUY' else 'sell' }}">{{ s.direction }}</td>
        <td>{{ s.entry }}</td>
        <td class="r">{{ s.sl }}</td>
        <td class="g">{{ s.tp }}</td>
        <td>1:{{ s.rr }}</td>
        <td>{{ s.risk }}%</td>
        <td><span class="{{ 't1' if s.category == 'Tier 1' else 't2' if s.category == 'Tier 2' else 'cry' }}">{{ s.category }}</span></td>
        <td><span class="bdg {{ 'pnd' if s.status == 'Pending' else 'tph' if s.status == 'TP Hit' else 'slh' if s.status == 'SL Hit' else 'exp' }}">{{ s.status }}</span></td>
        <td style="color:#8b949e;font-size:11px">{{ s.fired_at[:16].replace("T"," ") if s.fired_at else "" }}</td>
        <td>{% if s.status == "Pending" %}
          <button class="btn btp" onclick="upd({{ s.id }},'TP Hit')">TP</button>
          <button class="btn bsl" onclick="upd({{ s.id }},'SL Hit')">SL</button>
          <button class="btn bex" onclick="upd({{ s.id }},'Expired')">Exp</button>
        {% endif %}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table></div>
</div>

<!-- LIMITLESS TAB -->
<div class="pane" id="tab-limitless">
  <div class="stats">
    <div class="stat"><div class="slbl">Total Sent</div><div class="sval b">{{ lp.total }}</div></div>
    <div class="stat"><div class="slbl">Win Rate</div><div class="sval {{ 'g' if lp.wr >= 65 else 'a' if lp.wr >= 50 else 'r' }}">{{ lp.wr }}%</div></div>
    <div class="stat"><div class="slbl">Wins</div><div class="sval g">{{ lp.wins }}</div></div>
    <div class="stat"><div class="slbl">Losses</div><div class="sval r">{{ lp.losses }}</div></div>
    <div class="stat"><div class="slbl">Pending</div><div class="sval a">{{ lp.pending }}</div></div>
    <div class="stat"><div class="slbl">BTC Trend</div>
      <div class="sval {{ 'g' if btc_trend == 'BUY' else 'r' if btc_trend == 'SELL' else 'a' }}">
        {{ '🟢 BUY' if btc_trend == 'BUY' else '🔴 SELL' if btc_trend == 'SELL' else '⚪ N/A' }}
      </div>
    </div>
  </div>
  <button class="sbtn" onclick="triggerScan()">🔍 Scan Now</button>
  <div class="stit">Predictions Log</div>
  <div class="tw"><table>
    <thead><tr><th>#</th><th>Market</th><th>Asset</th><th>Type</th><th>Odds</th><th>Price@Alert</th><th>Baseline</th><th>Hrs Left</th><th>Conf</th><th>Status</th><th>Time</th><th>Action</th></tr></thead>
    <tbody>
      {% if not preds %}<tr><td colspan="12"><div class="empty">🎯 No predictions yet — scanner runs every 5 mins during trading hours</div></td></tr>{% endif %}
      {% for p in preds %}
      <tr>
        <td style="color:#8b949e">{{ p.id }}</td>
        <td style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{{ p.title }}">{{ p.title }}</td>
        <td style="font-weight:700">{{ p.asset }}</td>
        <td style="color:#8b949e;font-size:11px">{{ p.market_type }}</td>
        <td class="a" style="font-weight:700">{{ "%.1f"|format(p.bet_odds) }}%</td>
        <td style="font-size:11px">{{ "$%.4f"|format(p.current_price) if p.current_price and p.current_price < 100 else "$%.2f"|format(p.current_price) if p.current_price else "-" }}</td>
        <td style="font-size:11px">{{ "$%.4f"|format(p.baseline) if p.baseline < 100 else "$%.2f"|format(p.baseline) }}</td>
        <td style="color:#8b949e">{{ "%.1fh"|format(p.hours_left) if p.hours_left else "-" }}</td>
        <td><span class="bdg {{ 'chigh' if p.confidence == 'HIGH' else 'cmed' }}">{{ p.confidence }}</span></td>
        <td><span class="bdg {{ 'pnd' if p.status == 'Pending' else 'won' if '✅' in (p.status or '') else 'lost' if '❌' in (p.status or '') else 'exp' }}">{{ p.status }}</span></td>
        <td style="color:#8b949e;font-size:11px">{{ p.fired_at[:16].replace("T"," ") if p.fired_at else "" }}</td>
        <td>{% if p.status == "Pending" %}
          <button class="btn btp" onclick="updL({{ p.id }},'✅ Won')">Won</button>
          <button class="btn bsl" onclick="updL({{ p.id }},'❌ Lost')">Lost</button>
        {% endif %}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table></div>
</div>

<div class="ref">Cmvng monitor: 15 mins · Limitless scanner: 5 mins · Outcomes: auto-checked · PostgreSQL</div>
<script>
function showTab(t,el){
  document.querySelectorAll('.tab').forEach(e=>e.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(e=>e.classList.remove('active'));
  document.getElementById('tab-'+t).classList.add('active');
  el.classList.add('active');
}
function upd(id,s){fetch('/update/'+id+'/'+encodeURIComponent(s),{method:'POST'}).then(()=>location.reload())}
function updL(id,s){fetch('/limitless/update/'+id+'/'+encodeURIComponent(s),{method:'POST'}).then(()=>location.reload())}
function triggerScan(){fetch('/limitless/scan').then(()=>alert('Scan running — check Telegram in ~30 seconds'))}
setTimeout(()=>location.reload(),60000);
</script>
</body></html>"""

@app.route("/")
def dashboard():
    conn   = get_db()
    rows   = conn.run("SELECT * FROM signals ORDER BY id DESC")
    cols   = [c['name'] for c in conn.columns]
    signals = [dict(zip(cols, r)) for r in rows]

    prows  = conn.run("""
        SELECT pair, timeframe, category, risk,
               COUNT(*) as total,
               SUM(CASE WHEN status='TP Hit' THEN 1 ELSE 0 END) as tp,
               SUM(CASE WHEN status='SL Hit' THEN 1 ELSE 0 END) as sl
        FROM signals GROUP BY pair, timeframe, category, risk ORDER BY total DESC
    """)
    pcols  = [c['name'] for c in conn.columns]
    pair_stats = [dict(zip(pcols, r)) for r in prows]

    lp_rows = conn.run("SELECT * FROM limitless_predictions ORDER BY id DESC")
    lp_cols = [c['name'] for c in conn.columns]
    preds   = [dict(zip(lp_cols, r)) for r in lp_rows]
    conn.close()

    total   = len(signals)
    tp      = sum(1 for s in signals if s["status"] == "TP Hit")
    sl      = sum(1 for s in signals if s["status"] == "SL Hit")
    pending = sum(1 for s in signals if s["status"] == "Pending")
    closed  = tp + sl
    wr      = round(tp / closed * 100, 1) if closed > 0 else 0
    pf      = round((tp * 1.5) / sl, 2)   if sl > 0    else 0
    stats   = {"total": total, "tp": tp, "sl": sl, "pending": pending, "wr": wr, "pf": pf}

    lp_total   = len(preds)
    lp_wins    = sum(1 for p in preds if p.get("outcome") == "WIN")
    lp_losses  = sum(1 for p in preds if p.get("outcome") == "LOSS")
    lp_pending = sum(1 for p in preds if p.get("status") == "Pending")
    lp_closed  = lp_wins + lp_losses
    lp_wr      = round(lp_wins / lp_closed * 100, 1) if lp_closed > 0 else 0
    lp = {"total": lp_total, "wins": lp_wins, "losses": lp_losses, "pending": lp_pending, "wr": lp_wr}

    return render_template_string(
        DASHBOARD_HTML,
        signals=signals, stats=stats, pair_stats=pair_stats,
        preds=preds, lp=lp,
        btc_trend=get_btc_trend(),
        in_window=is_lagos_window()
    )

@app.route("/test")
def test():
    btc = get_btc_trend()
    win = is_lagos_window()
    send_telegram(
        "✅ <b>Cmvng Platform v2 — LIVE</b>\n\n"
        "✅ Cmvng signal monitor active\n"
        "✅ Limitless scanner active (every 5 mins)\n"
        "✅ Outcome tracker active\n"
        "✅ PostgreSQL connected\n\n"
        "<b>BTC Trend:</b> {}\n"
        "<b>Trading Window:</b> {}".format(
            btc or "No recent BTC signal",
            "🟢 OPEN" if win else "🔴 CLOSED (outside Lagos hours)"
        )
    )
    return jsonify({"status": "ok", "btc_trend": btc, "in_window": win}), 200

# ═══════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════

try:
    init_db()
    print("Database initialized OK")
except Exception as e:
    print("DB init error: {}".format(e))

threading.Thread(target=check_pending_signals, daemon=True).start()
threading.Thread(target=scan_loop,             daemon=True).start()
threading.Thread(target=outcome_loop,          daemon=True).start()
print("Cmvng Platform v2 started — 3 threads running")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
