from flask import Flask, request, jsonify, render_template_string
import pg8000.native
import os
import re
import threading
import time
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")

# Lagos timezone UTC+1
LAGOS_TZ      = timezone(timedelta(hours=1))
LIMITLESS_API = "https://api.limitless.exchange"

# Global BTC trend cache — updated by scanner, read by dashboard
_btc_trend_cache = {"trend": None}

# Favourite pairs — always qualify regardless of time window
FAVOURITE_HOURLY = ["ADA", "BNB", "HYPE"]

# Yahoo Finance / yfinance symbol map
YAHOO_MAP = {
    "BTC":"BTC-USD",  "ETH":"ETH-USD",  "SOL":"SOL-USD",
    "ADA":"ADA-USD",  "BNB":"BNB-USD",  "DOGE":"DOGE-USD",
    "XRP":"XRP-USD",  "AVAX":"AVAX-USD","LINK":"LINK-USD",
    "LTC":"LTC-USD",  "BCH":"BCH-USD",  "XLM":"XLM-USD",
    "HYPE":"HYPE-USD","SUI":"SUI-USD",  "ZEC":"ZEC-USD",
    "XMR":"XMR-USD",  "ONDO":"ONDO-USD","MNT":"MNT-USD",
    "DOT":"DOT-USD",  "UNI":"UNI-USD",  "ATOM":"ATOM-USD",
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
            slug          TEXT
        )
    """)
    conn.close()
    print("DB initialized OK")

# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured")
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
# YAHOO FINANCE — price and BTC trend via yfinance
# ═══════════════════════════════════════════════════════════

def get_price(asset):
    """Get current price for any asset using yfinance"""
    import yfinance as yf
    symbol = YAHOO_MAP.get(asset.upper())
    if not symbol:
        return None
    try:
        ticker = yf.Ticker(symbol)
        price  = ticker.fast_info.last_price
        if price and price > 0:
            return float(price)
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
        return None
    except Exception as e:
        print("yfinance error {}: {}".format(asset, e))
        return None

def get_btc_trend():
    """
    Determine BTC trend using yfinance.
    Uses 1H candles — if current price is above the 10-period SMA = BUY, else SELL.
    Updates global cache so dashboard never calls yfinance directly.
    """
    import yfinance as yf
    try:
        btc  = yf.Ticker("BTC-USD")
        hist = btc.history(period="2d", interval="1h")
        if hist.empty or len(hist) < 10:
            return _btc_trend_cache.get("trend")
        closes  = hist["Close"].tolist()
        current = closes[-1]
        sma10   = sum(closes[-10:]) / 10
        trend   = "BUY" if current > sma10 else "SELL"
        _btc_trend_cache["trend"] = trend
        print("BTC trend: {} (price={:.0f}, sma10={:.0f})".format(trend, current, sma10))
        return trend
    except Exception as e:
        print("BTC trend error: {}".format(e))
        return _btc_trend_cache.get("trend")

# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def is_lagos_window():
    """5am-12pm and 6pm-12am Lagos time (UTC+1)"""
    hour = datetime.now(LAGOS_TZ).hour
    return (5 <= hour < 12) or (18 <= hour < 24)

def fmt_price(v):
    if v is None:
        return "-"
    return "${:,.4f}".format(v) if v < 100 else "${:,.2f}".format(v)

# ═══════════════════════════════════════════════════════════
# LIMITLESS MARKET PARSER
# ═══════════════════════════════════════════════════════════

def parse_market(market):
    """Extract asset, direction, baseline, timing from a Limitless market"""
    title = market.get("title", "")
    # Matches: $DOGE above $0.216 OR SOL above $84.79
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

    prices    = market.get("prices", [50, 50])
    yes_odds  = float(prices[0]) if prices else 50.0
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

# ═══════════════════════════════════════════════════════════
# SCORING — applies all your rules
# ═══════════════════════════════════════════════════════════

def score_market(p, btc_trend, price):
    """
    Rules:
    1. Lagos window (5am-12pm, 6pm-12am) — bypassed for favourite pairs
    2. Expiry: hourly 5-15 mins / daily 0.5-10 hrs (favourites bypass max)
    3. Price already on winning side
    4. Odds 75-97%
    5. BTC trend alignment
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

# ═══════════════════════════════════════════════════════════
# SAVE AND ALERT
# ═══════════════════════════════════════════════════════════

def save_and_alert(p, score, price, btc_trend):
    try:
        now  = datetime.now(timezone.utc).isoformat()
        conn = get_db()
        rows = conn.run(
            """INSERT INTO limitless_predictions
            (market_id,title,asset,direction,baseline,bet_odds,confidence,
             size_rec,current_price,hours_left,market_type,status,fired_at,slug)
            VALUES (:mid,:ttl,:ast,:dir,:base,:odds,:conf,:sz,:pr,:hrs,:mt,'Pending',:now,:slg)
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
        print("Alert #{}: {}".format(pid, p["title"]))
    except Exception as e:
        print("Alert error: {}".format(e))

# ═══════════════════════════════════════════════════════════
# SCANNER — runs every 5 minutes
# ═══════════════════════════════════════════════════════════

def run_scan():
    import requests as req
    try:
        btc_trend = get_btc_trend()
        r = req.get("{}/markets/active".format(LIMITLESS_API), timeout=15)
        if r.status_code != 200:
            print("Limitless API error: {}".format(r.status_code))
            return 0
        markets = r.json().get("data", [])
        print("Scan: {} markets, BTC={}".format(len(markets), btc_trend))

        conn         = get_db()
        alerted_rows = conn.run(
            "SELECT market_id FROM limitless_predictions WHERE fired_at > NOW() - INTERVAL '6 hours'"
        )
        alerted_ids  = set(str(row[0]) for row in alerted_rows)
        conn.close()

        count = 0
        for market in markets:
            try:
                parsed = parse_market(market)
                if not parsed or parsed["market_id"] in alerted_ids:
                    continue
                price  = get_price(parsed["asset"])
                scored = score_market(parsed, btc_trend, price)
                if not scored:
                    continue
                save_and_alert(parsed, scored, price, btc_trend)
                alerted_ids.add(parsed["market_id"])
                count += 1
                time.sleep(1)
            except Exception as e:
                print("Market error: {}".format(e))

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
# OUTCOME CHECKER — auto-resolves expired predictions
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
                        continue

                    price = get_price(p["asset"])
                    if price is None:
                        continue

                    won    = (price > p["baseline"]) if p["direction"] == "above" else (price < p["baseline"])
                    outcome = "WIN" if won else "LOSS"
                    status  = "✅ Won" if won else "❌ Lost"

                    conn2 = get_db()
                    conn2.run(
                        "UPDATE limitless_predictions SET status=:s,outcome=:o,resolved_at=:r WHERE id=:i",
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
    return {"status": "scan triggered — check Telegram in ~30 seconds"}, 200

@app.route("/test")
def test():
    btc = get_btc_trend()
    win = is_lagos_window()
    send_telegram(
        "✅ <b>Limitless Prediction Bot — LIVE</b>\n\n"
        "✅ Scanner active (every 5 mins)\n"
        "✅ Outcome tracker active\n"
        "✅ PostgreSQL connected\n"
        "✅ Yahoo Finance (yfinance) active\n\n"
        "<b>BTC Trend:</b> {}\n"
        "<b>Lagos Window:</b> {}".format(
            btc or "Calculating...",
            "🟢 OPEN" if win else "🔴 CLOSED"
        )
    )
    return {"status": "ok", "btc_trend": btc, "in_window": win}, 200

# ═══════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Limitless — Prediction Platform</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,500;9..144,600;9..144,700&family=Inter+Tight:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  /* ── Neutral cream palette ── */
  --bg: #fafaf7;
  --bg-subtle: #f4f3ed;
  --surface: #ffffff;
  --surface-hover: #fbfaf5;
  --border: #ececea;
  --border-strong: #dcdbd7;

  /* ── Deep forest greens ── */
  --accent: #1a3d2e;
  --accent-muted: #2d5a42;
  --accent-soft: #e8efe9;
  --accent-line: #c5d6c9;

  /* ── Status ── */
  --positive: #1a7046;
  --positive-bg: #e8f3ed;
  --negative: #b4322e;
  --negative-bg: #f7e7e5;
  --warning: #8a6a2f;
  --warning-bg: #f5eedb;
  --info: #2d4a7a;
  --info-bg: #e5ecf5;

  /* ── Typography ── */
  --ink: #1a1a17;
  --ink-2: #3a3a35;
  --ink-3: #6b6b64;
  --ink-4: #9c9c94;

  /* ── Fonts ── */
  --display: 'Fraunces', 'Georgia', serif;
  --sans: 'Inter Tight', -apple-system, BlinkMacSystemFont, sans-serif;
  --mono: 'JetBrains Mono', 'SF Mono', Consolas, monospace;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

::selection { background: var(--accent); color: var(--bg); }

html { scroll-behavior: smooth; }

body {
  font-family: var(--sans);
  background: var(--bg);
  color: var(--ink);
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  font-feature-settings: "ss01", "cv11";
  min-height: 100vh;
  overflow-x: hidden;
}

/* ── Subtle paper texture ── */
body::before {
  content: '';
  position: fixed;
  inset: 0;
  background-image:
    radial-gradient(circle at 20% 30%, rgba(26, 61, 46, 0.015) 0%, transparent 40%),
    radial-gradient(circle at 80% 70%, rgba(26, 61, 46, 0.015) 0%, transparent 40%);
  pointer-events: none;
  z-index: 0;
}

.app { position: relative; z-index: 1; max-width: 1380px; margin: 0 auto; }

/* ═══════════════════════════════════════════════════════════
   HEADER
   ═══════════════════════════════════════════════════════════ */
.hdr {
  padding: 28px 40px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 20px;
  border-bottom: 1px solid var(--border);
}

.brand {
  display: flex;
  align-items: center;
  gap: 16px;
}
.brand-mark {
  width: 40px;
  height: 40px;
  border-radius: 10px;
  background: var(--accent);
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: 0 1px 2px rgba(26, 61, 46, 0.1);
}
.brand-mark::after {
  content: '';
  width: 16px;
  height: 16px;
  border: 2px solid var(--bg);
  border-radius: 50%;
  position: relative;
}
.brand-mark::before {
  content: '';
  position: absolute;
  width: 4px;
  height: 4px;
  background: var(--bg);
  border-radius: 50%;
  z-index: 1;
}
.brand-text {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.brand-text h1 {
  font-family: var(--display);
  font-weight: 500;
  font-size: 20px;
  letter-spacing: -0.02em;
  color: var(--ink);
  font-variation-settings: "opsz" 14;
  line-height: 1;
}
.brand-text small {
  font-size: 10px;
  font-family: var(--mono);
  color: var(--ink-4);
  text-transform: uppercase;
  letter-spacing: 0.12em;
}

.hdr-pills {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.pill {
  font-size: 11px;
  font-weight: 500;
  padding: 6px 11px;
  border-radius: 100px;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-family: var(--sans);
  letter-spacing: -0.005em;
  background: var(--surface);
  border: 1px solid var(--border);
  color: var(--ink-2);
  transition: border-color 0.15s;
}
.pill:hover { border-color: var(--border-strong); }

.pill-active { background: var(--positive-bg); color: var(--positive); border-color: transparent; }
.pill-inactive { background: var(--warning-bg); color: var(--warning); border-color: transparent; }
.pill-btc { background: var(--surface); font-family: var(--mono); font-size: 11px; }
.pill-btc-up { border-color: var(--positive); color: var(--positive); }
.pill-btc-down { border-color: var(--negative); color: var(--negative); }

.dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: currentColor;
  position: relative;
}
.dot.live::after {
  content: '';
  position: absolute;
  inset: -3px;
  border-radius: 50%;
  border: 1.5px solid currentColor;
  opacity: 0;
  animation: ring 2s ease-out infinite;
}
@keyframes ring {
  0% { opacity: 1; transform: scale(0.8); }
  80%, 100% { opacity: 0; transform: scale(2); }
}

/* ═══════════════════════════════════════════════════════════
   HERO SECTION
   ═══════════════════════════════════════════════════════════ */
.hero {
  padding: 48px 40px 32px;
  border-bottom: 1px solid var(--border);
  position: relative;
}

.hero-label {
  font-size: 10px;
  font-family: var(--mono);
  color: var(--ink-4);
  text-transform: uppercase;
  letter-spacing: 0.15em;
  margin-bottom: 12px;
  display: flex;
  align-items: center;
  gap: 10px;
}
.hero-label::before {
  content: '';
  width: 24px;
  height: 1px;
  background: var(--ink-4);
}

.hero-title {
  font-family: var(--display);
  font-weight: 400;
  font-size: clamp(36px, 5vw, 54px);
  line-height: 1.02;
  letter-spacing: -0.035em;
  color: var(--ink);
  font-variation-settings: "opsz" 60, "SOFT" 30;
  max-width: 900px;
  margin-bottom: 16px;
}
.hero-title em {
  font-style: italic;
  color: var(--accent);
  font-weight: 400;
  font-variation-settings: "opsz" 144;
}

.hero-sub {
  font-size: 15px;
  color: var(--ink-3);
  max-width: 560px;
  line-height: 1.55;
  font-weight: 400;
}

/* ═══════════════════════════════════════════════════════════
   STATS GRID
   ═══════════════════════════════════════════════════════════ */
.stats {
  padding: 32px 40px;
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 0;
  border-bottom: 1px solid var(--border);
}

.stat {
  padding: 0 24px;
  position: relative;
}
.stat + .stat { border-left: 1px solid var(--border); }
.stat:first-child { padding-left: 0; }
.stat:last-child { padding-right: 0; }

.stat-label {
  font-size: 10px;
  font-family: var(--mono);
  color: var(--ink-4);
  text-transform: uppercase;
  letter-spacing: 0.14em;
  margin-bottom: 10px;
  font-weight: 500;
}
.stat-value {
  font-family: var(--display);
  font-weight: 400;
  font-size: 40px;
  line-height: 1;
  letter-spacing: -0.04em;
  color: var(--ink);
  font-variation-settings: "opsz" 80;
  margin-bottom: 6px;
}
.stat-value.is-positive { color: var(--positive); }
.stat-value.is-negative { color: var(--negative); }
.stat-value.is-warning { color: var(--warning); }
.stat-value.is-accent { color: var(--accent); }

.stat-meta {
  font-size: 11px;
  font-family: var(--mono);
  color: var(--ink-4);
  display: flex;
  align-items: center;
  gap: 4px;
}

@media (max-width: 900px) {
  .stats { grid-template-columns: repeat(3, 1fr); gap: 24px 0; }
  .stat:nth-child(3n+1) { padding-left: 0; }
  .stat:nth-child(3n) { padding-right: 0; }
  .stat:nth-child(n+4) { border-top: 1px solid var(--border); padding-top: 24px; }
}
@media (max-width: 600px) {
  .stats { grid-template-columns: repeat(2, 1fr); }
  .stat { border-left: none !important; padding: 0; }
  .stat:nth-child(n+3) { border-top: 1px solid var(--border); padding-top: 20px; margin-top: 4px; }
}

/* ═══════════════════════════════════════════════════════════
   ACTION BAR
   ═══════════════════════════════════════════════════════════ */
.action-bar {
  padding: 24px 40px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}

.section-head {
  display: flex;
  align-items: baseline;
  gap: 14px;
}
.section-title {
  font-family: var(--display);
  font-weight: 500;
  font-size: 22px;
  letter-spacing: -0.02em;
  color: var(--ink);
  font-variation-settings: "opsz" 24;
}
.section-count {
  font-size: 11px;
  font-family: var(--mono);
  color: var(--ink-4);
  background: var(--bg-subtle);
  padding: 3px 8px;
  border-radius: 100px;
  letter-spacing: 0.05em;
}

.actions {
  display: flex;
  gap: 8px;
  align-items: center;
}

.btn {
  font-family: var(--sans);
  font-size: 13px;
  font-weight: 500;
  padding: 9px 16px;
  border-radius: 8px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--ink);
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  letter-spacing: -0.005em;
  transition: all 0.15s;
  box-shadow: 0 1px 2px rgba(0,0,0,0.02);
}
.btn:hover {
  border-color: var(--border-strong);
  background: var(--surface-hover);
  transform: translateY(-0.5px);
}
.btn-primary {
  background: var(--accent);
  color: var(--bg);
  border-color: var(--accent);
  box-shadow: 0 1px 2px rgba(26, 61, 46, 0.15);
}
.btn-primary:hover {
  background: var(--accent-muted);
  border-color: var(--accent-muted);
}
.btn-icon {
  font-size: 13px;
  display: inline-flex;
}

/* ═══════════════════════════════════════════════════════════
   TABLE
   ═══════════════════════════════════════════════════════════ */
.table-wrap {
  margin: 0 40px 32px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 14px;
  overflow: hidden;
}
.table-scroll { overflow-x: auto; }

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
  min-width: 900px;
}

thead {
  background: var(--bg-subtle);
  border-bottom: 1px solid var(--border);
}
thead th {
  text-align: left;
  padding: 14px 16px;
  font-size: 10px;
  font-family: var(--mono);
  font-weight: 500;
  color: var(--ink-3);
  text-transform: uppercase;
  letter-spacing: 0.1em;
  white-space: nowrap;
}
thead th:first-child { padding-left: 24px; }
thead th:last-child { padding-right: 24px; }

tbody td {
  padding: 16px;
  border-bottom: 1px solid var(--border);
  vertical-align: middle;
  color: var(--ink-2);
}
tbody td:first-child { padding-left: 24px; }
tbody td:last-child { padding-right: 24px; }
tbody tr:last-child td { border-bottom: none; }
tbody tr { transition: background 0.1s; }
tbody tr:hover { background: var(--bg); }

.cell-id { font-family: var(--mono); color: var(--ink-4); font-size: 12px; }
.cell-market {
  font-weight: 500;
  color: var(--ink);
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.cell-asset {
  font-family: var(--mono);
  font-weight: 600;
  font-size: 12px;
  color: var(--accent);
  letter-spacing: 0.02em;
}
.cell-type {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--ink-4);
  text-transform: uppercase;
  letter-spacing: 0.08em;
}
.cell-odds {
  font-family: var(--mono);
  font-weight: 600;
  font-size: 13px;
  color: var(--ink);
}
.cell-price {
  font-family: var(--mono);
  font-size: 12px;
  color: var(--ink-2);
}
.cell-time {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--ink-4);
}

/* ── Tags ── */
.tag {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 9px;
  border-radius: 100px;
  font-size: 11px;
  font-weight: 500;
  font-family: var(--sans);
  letter-spacing: -0.003em;
}
.tag-pending { background: var(--info-bg); color: var(--info); }
.tag-won { background: var(--positive-bg); color: var(--positive); }
.tag-lost { background: var(--negative-bg); color: var(--negative); }
.tag-high { background: var(--accent-soft); color: var(--accent); }
.tag-med { background: var(--warning-bg); color: var(--warning); }

/* ── Action buttons ── */
.act {
  font-family: var(--sans);
  font-size: 11px;
  font-weight: 500;
  padding: 5px 10px;
  border-radius: 6px;
  border: 1px solid transparent;
  cursor: pointer;
  margin-right: 4px;
  transition: all 0.15s;
}
.act-won {
  background: var(--positive-bg);
  color: var(--positive);
  border-color: transparent;
}
.act-won:hover { background: var(--positive); color: var(--bg); }
.act-lost {
  background: var(--negative-bg);
  color: var(--negative);
}
.act-lost:hover { background: var(--negative); color: var(--bg); }

/* ── Empty ── */
.empty-state {
  padding: 64px 24px;
  text-align: center;
}
.empty-mark {
  width: 56px;
  height: 56px;
  border-radius: 14px;
  background: var(--bg-subtle);
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-size: 22px;
  margin-bottom: 16px;
  border: 1px solid var(--border);
}
.empty-state h3 {
  font-family: var(--display);
  font-weight: 500;
  font-size: 18px;
  color: var(--ink);
  margin-bottom: 6px;
  letter-spacing: -0.015em;
}
.empty-state p {
  font-size: 13px;
  color: var(--ink-3);
  max-width: 320px;
  margin: 0 auto;
  line-height: 1.55;
}

/* ═══════════════════════════════════════════════════════════
   FOOTER
   ═══════════════════════════════════════════════════════════ */
.footer {
  padding: 24px 40px 40px;
  border-top: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}
.footer-left, .footer-right {
  font-size: 11px;
  font-family: var(--mono);
  color: var(--ink-4);
  letter-spacing: 0.02em;
}
.footer-right { display: flex; gap: 16px; }
.footer-right span { display: inline-flex; align-items: center; gap: 6px; }

/* ═══════════════════════════════════════════════════════════
   TOAST
   ═══════════════════════════════════════════════════════════ */
.toast {
  position: fixed;
  bottom: 24px;
  left: 50%;
  transform: translateX(-50%) translateY(80px);
  background: var(--ink);
  color: var(--bg);
  padding: 12px 20px;
  border-radius: 100px;
  font-size: 13px;
  font-weight: 500;
  box-shadow: 0 10px 40px rgba(0,0,0,0.15);
  opacity: 0;
  transition: all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
  z-index: 1000;
  display: inline-flex;
  align-items: center;
  gap: 8px;
}
.toast.show {
  opacity: 1;
  transform: translateX(-50%) translateY(0);
}

/* ═══════════════════════════════════════════════════════════
   ANIMATIONS
   ═══════════════════════════════════════════════════════════ */
@keyframes fade-up {
  from { opacity: 0; transform: translateY(12px); }
  to { opacity: 1; transform: translateY(0); }
}
.hero-label, .hero-title, .hero-sub { animation: fade-up 0.6s ease both; }
.hero-title { animation-delay: 0.1s; }
.hero-sub { animation-delay: 0.2s; }
.stat { animation: fade-up 0.5s ease both; }
.stat:nth-child(1) { animation-delay: 0.3s; }
.stat:nth-child(2) { animation-delay: 0.35s; }
.stat:nth-child(3) { animation-delay: 0.4s; }
.stat:nth-child(4) { animation-delay: 0.45s; }
.stat:nth-child(5) { animation-delay: 0.5s; }
.stat:nth-child(6) { animation-delay: 0.55s; }

/* ── Responsive tweaks ── */
@media (max-width: 720px) {
  .hdr, .hero, .stats, .action-bar, .footer { padding-left: 20px; padding-right: 20px; }
  .table-wrap { margin-left: 20px; margin-right: 20px; }
  .hero { padding-top: 36px; padding-bottom: 24px; }
  .hero-title { font-size: 32px; }
  .stat-value { font-size: 30px; }
}
</style>
</head>
<body>

<div class="app">

  <!-- ═══ HEADER ═══ -->
  <header class="hdr">
    <div class="brand">
      <div class="brand-mark"></div>
      <div class="brand-text">
        <h1>Limitless</h1>
        <small>Prediction Platform · CMVNG</small>
      </div>
    </div>
    <div class="hdr-pills">
      <span class="pill {{ 'pill-active' if in_window else 'pill-inactive' }}">
        <span class="dot live"></span>
        {{ 'Window Open' if in_window else 'Window Closed' }}
      </span>
      <span class="pill {{ 'pill-btc-up' if btc_trend == 'BUY' else 'pill-btc-down' if btc_trend == 'SELL' else '' }} pill-btc">
        BTC {{ '↗ BUY' if btc_trend == 'BUY' else '↘ SELL' if btc_trend == 'SELL' else '— N/A' }}
      </span>
      <span class="pill pill-active">
        <span class="dot live"></span>
        Live
      </span>
    </div>
  </header>

  <!-- ═══ HERO ═══ -->
  <section class="hero">
    <div class="hero-label">Prediction Intelligence</div>
    <h2 class="hero-title">
      Precision scanning,<br>
      <em>effortless compounding.</em>
    </h2>
    <p class="hero-sub">
      Automated market scanner monitoring Limitless prediction markets in real-time, surfacing only opportunities that match your edge. Every signal backed by price, timing and trend alignment.
    </p>
  </section>

  <!-- ═══ STATS ═══ -->
  <section class="stats">
    <div class="stat">
      <div class="stat-label">Total Sent</div>
      <div class="stat-value">{{ stats.total }}</div>
      <div class="stat-meta">all time</div>
    </div>
    <div class="stat">
      <div class="stat-label">Win Rate</div>
      <div class="stat-value {{ 'is-positive' if stats.wr >= 65 else 'is-warning' if stats.wr >= 50 else 'is-negative' if stats.total > 0 else '' }}">{{ stats.wr }}<span style="font-size:.5em;color:var(--ink-4)">%</span></div>
      <div class="stat-meta">{{ stats.wins }}W · {{ stats.losses }}L</div>
    </div>
    <div class="stat">
      <div class="stat-label">Wins</div>
      <div class="stat-value is-positive">{{ stats.wins }}</div>
      <div class="stat-meta">resolved</div>
    </div>
    <div class="stat">
      <div class="stat-label">Losses</div>
      <div class="stat-value is-negative">{{ stats.losses }}</div>
      <div class="stat-meta">resolved</div>
    </div>
    <div class="stat">
      <div class="stat-label">Pending</div>
      <div class="stat-value is-warning">{{ stats.pending }}</div>
      <div class="stat-meta">in play</div>
    </div>
    <div class="stat">
      <div class="stat-label">Today</div>
      <div class="stat-value is-accent">{{ stats.today }}</div>
      <div class="stat-meta">Lagos time</div>
    </div>
  </section>

  <!-- ═══ ACTION BAR ═══ -->
  <div class="action-bar">
    <div class="section-head">
      <h3 class="section-title">Predictions</h3>
      <span class="section-count">{{ stats.total }} total</span>
    </div>
    <div class="actions">
      <button class="btn" onclick="location.reload()">
        <span class="btn-icon">↻</span> Refresh
      </button>
      <button class="btn btn-primary" onclick="triggerScan()">
        <span class="btn-icon">◎</span> Scan Now
      </button>
    </div>
  </div>

  <!-- ═══ TABLE ═══ -->
  <div class="table-wrap">
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Market</th>
            <th>Asset</th>
            <th>Type</th>
            <th>Odds</th>
            <th>Price @ Alert</th>
            <th>Baseline</th>
            <th>Time Left</th>
            <th>Confidence</th>
            <th>Status</th>
            <th>Logged</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {% if not preds %}
          <tr><td colspan="12">
            <div class="empty-state">
              <div class="empty-mark">◎</div>
              <h3>Awaiting first signal</h3>
              <p>Scanner runs every 5 minutes during your Lagos trading window. Click <b>Scan Now</b> to trigger manually.</p>
            </div>
          </td></tr>
          {% endif %}
          {% for p in preds %}
          <tr>
            <td class="cell-id">{{ p.id }}</td>
            <td><div class="cell-market" title="{{ p.title }}">{{ p.title }}</div></td>
            <td><span class="cell-asset">{{ p.asset }}</span></td>
            <td><span class="cell-type">{{ p.market_type }}</span></td>
            <td><span class="cell-odds">{{ "%.1f"|format(p.bet_odds) }}%</span></td>
            <td><span class="cell-price">{{ "$%.4f"|format(p.current_price) if p.current_price and p.current_price < 100 else "$%.2f"|format(p.current_price) if p.current_price else "—" }}</span></td>
            <td><span class="cell-price">{{ "$%.4f"|format(p.baseline) if p.baseline < 100 else "$%.2f"|format(p.baseline) }}</span></td>
            <td><span class="cell-time">{{ "%.1fh"|format(p.hours_left) if p.hours_left else "—" }}</span></td>
            <td>
              <span class="tag {{ 'tag-high' if p.confidence == 'HIGH' else 'tag-med' }}">
                {{ 'High' if p.confidence == 'HIGH' else 'Medium' }}
              </span>
            </td>
            <td>
              <span class="tag {{ 'tag-pending' if p.status == 'Pending' else 'tag-won' if '✅' in (p.status or '') else 'tag-lost' }}">
                {{ 'Pending' if p.status == 'Pending' else 'Won' if '✅' in (p.status or '') else 'Lost' }}
              </span>
            </td>
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

  <!-- ═══ FOOTER ═══ -->
  <footer class="footer">
    <div class="footer-left">Scanner · 5min intervals · Auto-resolving</div>
    <div class="footer-right">
      <span>Yahoo Finance</span>
      <span>·</span>
      <span>Limitless API</span>
      <span>·</span>
      <span>Auto-refresh 60s</span>
    </div>
  </footer>

</div>

<div class="toast" id="toast">
  <span style="font-size:10px">◎</span>
  <span id="toast-msg">Scan triggered</span>
</div>

<script>
function updL(id, s) {
  fetch('/limitless/update/' + id + '/' + encodeURIComponent(s), { method: 'POST' })
    .then(() => location.reload());
}
function triggerScan() {
  fetch('/scan').then(() => {
    showToast('Scan running — check Telegram shortly');
  });
}
function showToast(msg) {
  const t = document.getElementById('toast');
  document.getElementById('toast-msg').textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3000);
}
setTimeout(() => location.reload(), 60000);
</script>
</body></html>"""

@app.route("/")
def dashboard():
    from flask import render_template_string
    try:
        conn     = get_db()
        lp_rows  = conn.run("SELECT * FROM limitless_predictions ORDER BY id DESC")
        lp_cols  = [c['name'] for c in conn.columns]
        preds    = [dict(zip(lp_cols, r)) for r in lp_rows]
        conn.close()
    except Exception as e:
        print("Dashboard DB error: {}".format(e))
        preds = []

    total   = len(preds)
    wins    = sum(1 for p in preds if p.get("outcome") == "WIN")
    losses  = sum(1 for p in preds if p.get("outcome") == "LOSS")
    pending = sum(1 for p in preds if p.get("status") == "Pending")
    closed  = wins + losses
    wr      = round(wins / closed * 100, 1) if closed > 0 else 0

    today_str = datetime.now(LAGOS_TZ).strftime("%Y-%m-%d")
    today   = sum(1 for p in preds if p.get("fired_at", "").startswith(today_str))

    stats = {"total": total, "wins": wins, "losses": losses,
             "pending": pending, "wr": wr, "today": today}

    # Use cached BTC trend — never call yfinance on page load
    return render_template_string(
        DASHBOARD_HTML,
        preds=preds, stats=stats,
        btc_trend=_btc_trend_cache.get("trend"),
        in_window=is_lagos_window()
    )

# ═══════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════

try:
    init_db()
except Exception as e:
    print("DB init error: {}".format(e))

threading.Thread(target=scan_loop,    daemon=True).start()
threading.Thread(target=outcome_loop, daemon=True).start()
print("Limitless Prediction Bot started — 2 threads running")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
