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
<title>Limitless Prediction Platform</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root {
  --g0:#001a0a; --g1:#002d12; --g2:#003d18; --g3:#00521f;
  --g4:#006b28; --g5:#00852f; --g6:#00a838; --g7:#00d147;
  --g8:#39e86a; --g9:#7fffa0;
  --gold:#c8f566; --amber:#f0b429;
  --red:#ff4d6d; --blue:#38bdf8;
  --text:#e8f5ee; --muted:#4d7a5a; --border:#0a2e15;
  --card:#00150a; --card2:#001f0d;
  --font:'Syne',sans-serif; --mono:'DM Mono',monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{
  font-family:var(--font);
  background:var(--g0);
  color:var(--text);
  min-height:100vh;
  overflow-x:hidden;
}

/* ── Background grid ── */
body::before{
  content:'';
  position:fixed;inset:0;
  background-image:
    linear-gradient(rgba(0,168,56,.04) 1px, transparent 1px),
    linear-gradient(90deg, rgba(0,168,56,.04) 1px, transparent 1px);
  background-size:40px 40px;
  pointer-events:none;
  z-index:0;
}

/* ── Glow orb ── */
body::after{
  content:'';
  position:fixed;
  top:-200px; left:50%;
  transform:translateX(-50%);
  width:800px; height:500px;
  background:radial-gradient(ellipse, rgba(0,168,56,.12) 0%, transparent 70%);
  pointer-events:none;
  z-index:0;
}

.wrap{position:relative;z-index:1}

/* ── Header ── */
.hdr{
  padding:18px 32px;
  background:rgba(0,21,10,.85);
  backdrop-filter:blur(20px);
  border-bottom:1px solid var(--border);
  display:flex;
  align-items:center;
  justify-content:space-between;
  flex-wrap:wrap;
  gap:12px;
  position:sticky;top:0;z-index:100;
}
.logo{
  display:flex;align-items:center;gap:12px;
}
.logo-icon{
  width:36px;height:36px;
  background:linear-gradient(135deg,var(--g6),var(--g8));
  border-radius:10px;
  display:flex;align-items:center;justify-content:center;
  font-size:18px;
  box-shadow:0 0 20px rgba(0,168,56,.4);
}
.logo h1{
  font-size:15px;font-weight:800;
  letter-spacing:-.02em;
  color:#fff;
}
.logo span{
  font-size:11px;font-weight:400;
  color:var(--muted);
  display:block;
  font-family:var(--mono);
  letter-spacing:.05em;
}
.hdr-r{display:flex;align-items:center;gap:10px;flex-wrap:wrap}

/* ── Pills ── */
.pill{
  font-size:11px;font-weight:600;
  padding:5px 12px;border-radius:20px;
  font-family:var(--mono);
  letter-spacing:.03em;
  display:inline-flex;align-items:center;gap:5px;
}
.pill-green{background:rgba(0,168,56,.15);color:var(--g8);border:1px solid rgba(0,168,56,.3)}
.pill-red{background:rgba(255,77,109,.1);color:var(--red);border:1px solid rgba(255,77,109,.25)}
.pill-btc{background:rgba(0,168,56,.1);color:var(--gold);border:1px solid rgba(200,245,102,.2)}
.pill-blue{background:rgba(56,189,248,.1);color:var(--blue);border:1px solid rgba(56,189,248,.2)}

.pulse{
  width:7px;height:7px;border-radius:50%;
  background:var(--g7);
  box-shadow:0 0 8px var(--g7);
  animation:pulse 2s infinite;
}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(.85)}}

/* ── Main content ── */
.main{padding:28px 32px;max-width:1400px;margin:0 auto}

/* ── Stats grid ── */
.stats{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
  gap:12px;
  margin-bottom:28px;
}
.stat{
  background:var(--card);
  border:1px solid var(--border);
  border-radius:14px;
  padding:18px 16px;
  position:relative;
  overflow:hidden;
  transition:border-color .2s, transform .2s;
}
.stat:hover{border-color:var(--g4);transform:translateY(-2px)}
.stat::before{
  content:'';
  position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--g5),transparent);
}
.slbl{
  font-size:10px;font-weight:600;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:.08em;
  margin-bottom:8px;
  font-family:var(--mono);
}
.sval{
  font-size:28px;font-weight:800;
  letter-spacing:-.03em;
  color:var(--text);
  line-height:1;
}
.sval.g{color:var(--g8)}
.sval.r{color:var(--red)}
.sval.a{color:var(--amber)}
.sval.b{color:var(--blue)}
.sval.gold{color:var(--gold)}
.stat-sub{
  font-size:10px;color:var(--muted);
  margin-top:4px;font-family:var(--mono);
}

/* ── Section title ── */
.section-hdr{
  display:flex;
  align-items:center;
  justify-content:space-between;
  margin-bottom:16px;
  flex-wrap:wrap;gap:10px;
}
.stit{
  font-size:11px;font-weight:700;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:.1em;
  display:flex;align-items:center;gap:8px;
}
.stit::before{
  content:'';
  width:3px;height:14px;
  background:linear-gradient(var(--g7),var(--g5));
  border-radius:2px;
}

/* ── Buttons ── */
.btn-primary{
  background:linear-gradient(135deg,var(--g5),var(--g6));
  color:#fff;
  border:none;
  padding:9px 18px;
  border-radius:10px;
  font-size:12px;font-weight:700;
  cursor:pointer;
  font-family:var(--font);
  letter-spacing:.02em;
  transition:all .2s;
  box-shadow:0 4px 12px rgba(0,168,56,.25);
  display:inline-flex;align-items:center;gap:6px;
}
.btn-primary:hover{
  transform:translateY(-1px);
  box-shadow:0 6px 20px rgba(0,168,56,.4);
}
.btn-secondary{
  background:transparent;
  color:var(--muted);
  border:1px solid var(--border);
  padding:9px 18px;
  border-radius:10px;
  font-size:12px;font-weight:600;
  cursor:pointer;
  font-family:var(--font);
  transition:all .2s;
  display:inline-flex;align-items:center;gap:6px;
}
.btn-secondary:hover{border-color:var(--g4);color:var(--text)}

/* ── Table ── */
.tw{overflow-x:auto;border-radius:14px;border:1px solid var(--border)}
table{
  width:100%;
  border-collapse:collapse;
  font-size:12px;
  min-width:750px;
}
thead{background:var(--card2)}
th{
  text-align:left;
  padding:12px 14px;
  font-size:10px;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:.08em;
  font-weight:700;
  font-family:var(--mono);
  border-bottom:1px solid var(--border);
  white-space:nowrap;
}
td{
  padding:11px 14px;
  border-bottom:1px solid rgba(10,46,21,.8);
  vertical-align:middle;
}
tbody tr:last-child td{border-bottom:none}
tbody tr{
  background:var(--card);
  transition:background .15s;
}
tbody tr:hover{background:var(--card2)}

/* ── Badges ── */
.bdg{
  display:inline-flex;align-items:center;gap:4px;
  padding:3px 10px;border-radius:20px;
  font-size:10px;font-weight:700;
  font-family:var(--mono);
  letter-spacing:.03em;
  white-space:nowrap;
}
.pnd{background:rgba(56,189,248,.1);color:var(--blue);border:1px solid rgba(56,189,248,.2)}
.won{background:rgba(0,168,56,.12);color:var(--g8);border:1px solid rgba(0,168,56,.25)}
.lost{background:rgba(255,77,109,.1);color:var(--red);border:1px solid rgba(255,77,109,.2)}
.chigh{background:rgba(0,209,71,.1);color:var(--g8);border:1px solid rgba(0,168,56,.3)}
.cmed{background:rgba(240,180,41,.08);color:var(--amber);border:1px solid rgba(240,180,41,.2)}

/* ── Asset name ── */
.asset-name{
  font-weight:700;
  color:var(--g8);
  font-family:var(--mono);
  font-size:13px;
}
.mkt-title{
  font-size:11px;
  color:var(--text);
  opacity:.8;
  max-width:200px;
  overflow:hidden;
  text-overflow:ellipsis;
  white-space:nowrap;
}
.odds-val{
  font-family:var(--mono);
  font-weight:700;
  font-size:13px;
  color:var(--gold);
}
.price-val{
  font-family:var(--mono);
  font-size:11px;
  color:var(--text);
}
.time-val{
  font-family:var(--mono);
  font-size:10px;
  color:var(--muted);
}

/* ── Action buttons ── */
.act-won{
  background:rgba(0,168,56,.1);
  color:var(--g8);
  border:1px solid rgba(0,168,56,.3);
  padding:4px 10px;border-radius:7px;
  font-size:10px;font-weight:700;
  cursor:pointer;
  font-family:var(--mono);
  transition:all .15s;
  margin-right:4px;
}
.act-won:hover{background:rgba(0,168,56,.2)}
.act-lost{
  background:rgba(255,77,109,.08);
  color:var(--red);
  border:1px solid rgba(255,77,109,.25);
  padding:4px 10px;border-radius:7px;
  font-size:10px;font-weight:700;
  cursor:pointer;
  font-family:var(--mono);
  transition:all .15s;
}
.act-lost:hover{background:rgba(255,77,109,.15)}

/* ── Empty state ── */
.empty{
  text-align:center;
  padding:60px 24px;
  color:var(--muted);
}
.empty-icon{font-size:40px;margin-bottom:12px;opacity:.5}
.empty h3{font-size:15px;font-weight:700;color:var(--text);margin-bottom:6px;opacity:.6}
.empty p{font-size:12px;font-family:var(--mono);opacity:.5}

/* ── Footer ── */
.footer{
  text-align:right;
  padding:16px 32px;
  font-size:10px;
  color:var(--muted);
  font-family:var(--mono);
  letter-spacing:.04em;
  border-top:1px solid var(--border);
  margin-top:40px;
}

/* ── Scan toast ── */
.toast{
  position:fixed;bottom:24px;right:24px;
  background:var(--g4);
  color:#fff;
  padding:12px 20px;border-radius:12px;
  font-size:13px;font-weight:600;
  box-shadow:0 8px 24px rgba(0,0,0,.4);
  transform:translateY(100px);opacity:0;
  transition:all .3s cubic-bezier(.34,1.56,.64,1);
  z-index:999;
}
.toast.show{transform:translateY(0);opacity:1}

/* ── Animations ── */
@keyframes fadeUp{
  from{opacity:0;transform:translateY(16px)}
  to{opacity:1;transform:translateY(0)}
}
.stat{animation:fadeUp .4s ease both}
.stat:nth-child(1){animation-delay:.05s}
.stat:nth-child(2){animation-delay:.1s}
.stat:nth-child(3){animation-delay:.15s}
.stat:nth-child(4){animation-delay:.2s}
.stat:nth-child(5){animation-delay:.25s}
.stat:nth-child(6){animation-delay:.3s}

@media(max-width:600px){
  .hdr{padding:14px 16px}
  .main{padding:16px}
  .sval{font-size:22px}
}
</style>
</head>
<body>
<div class="wrap">

<!-- HEADER -->
<header class="hdr">
  <div class="logo">
    <div class="logo-icon">🎯</div>
    <div>
      <h1>Limitless Prediction Platform</h1>
      <span>CMVNG · Powered by Yahoo Finance</span>
    </div>
  </div>
  <div class="hdr-r">
    <span class="pill {{ 'pill-green' if in_window else 'pill-red' }}">
      <span class="pulse" style="{{ '' if in_window else 'background:var(--red);box-shadow:0 0 8px var(--red)' }}"></span>
      {{ 'Trading Window OPEN' if in_window else 'Outside Trading Hours' }}
    </span>
    <span class="pill pill-btc">
      ₿ BTC: {{ '▲ BUY' if btc_trend == 'BUY' else '▼ SELL' if btc_trend == 'SELL' else '— N/A' }}
    </span>
    <span class="pill pill-blue">
      <span class="pulse" style="background:var(--blue);box-shadow:0 0 8px var(--blue)"></span>
      Live
    </span>
  </div>
</header>

<!-- MAIN -->
<main class="main">

  <!-- Stats -->
  <div class="stats">
    <div class="stat">
      <div class="slbl">Total Sent</div>
      <div class="sval b">{{ stats.total }}</div>
      <div class="stat-sub">all time</div>
    </div>
    <div class="stat">
      <div class="slbl">Win Rate</div>
      <div class="sval {{ 'g' if stats.wr >= 65 else 'a' if stats.wr >= 50 else 'r' }}">{{ stats.wr }}%</div>
      <div class="stat-sub">{{ stats.wins }}W / {{ stats.losses }}L</div>
    </div>
    <div class="stat">
      <div class="slbl">Wins</div>
      <div class="sval g">{{ stats.wins }}</div>
      <div class="stat-sub">resolved ✅</div>
    </div>
    <div class="stat">
      <div class="slbl">Losses</div>
      <div class="sval r">{{ stats.losses }}</div>
      <div class="stat-sub">resolved ❌</div>
    </div>
    <div class="stat">
      <div class="slbl">Pending</div>
      <div class="sval a">{{ stats.pending }}</div>
      <div class="stat-sub">in play</div>
    </div>
    <div class="stat">
      <div class="slbl">Today</div>
      <div class="sval gold">{{ stats.today }}</div>
      <div class="stat-sub">Lagos time</div>
    </div>
  </div>

  <!-- Predictions Table -->
  <div class="section-hdr">
    <div class="stit">Predictions Log</div>
    <div style="display:flex;gap:8px">
      <button class="btn-primary" onclick="triggerScan()">🔍 Scan Now</button>
      <button class="btn-secondary" onclick="location.reload()">↻ Refresh</button>
    </div>
  </div>

  <div class="tw">
    <table>
      <thead><tr>
        <th>#</th>
        <th>Market</th>
        <th>Asset</th>
        <th>Type</th>
        <th>Odds</th>
        <th>Price @ Alert</th>
        <th>Baseline</th>
        <th>Hrs Left</th>
        <th>Confidence</th>
        <th>Status</th>
        <th>Time</th>
        <th>Action</th>
      </tr></thead>
      <tbody>
        {% if not preds %}
        <tr><td colspan="12">
          <div class="empty">
            <div class="empty-icon">🎯</div>
            <h3>No predictions yet</h3>
            <p>Scanner runs every 5 mins during Lagos trading hours<br>Click "Scan Now" to trigger manually</p>
          </div>
        </td></tr>
        {% endif %}
        {% for p in preds %}
        <tr>
          <td class="time-val">{{ p.id }}</td>
          <td><div class="mkt-title" title="{{ p.title }}">{{ p.title }}</div></td>
          <td><span class="asset-name">{{ p.asset }}</span></td>
          <td><span class="time-val">{{ p.market_type }}</span></td>
          <td><span class="odds-val">{{ "%.1f"|format(p.bet_odds) }}%</span></td>
          <td><span class="price-val">{{ "$%.4f"|format(p.current_price) if p.current_price and p.current_price < 100 else "$%.2f"|format(p.current_price) if p.current_price else "—" }}</span></td>
          <td><span class="price-val">{{ "$%.4f"|format(p.baseline) if p.baseline < 100 else "$%.2f"|format(p.baseline) }}</span></td>
          <td><span class="time-val">{{ "%.1fh"|format(p.hours_left) if p.hours_left else "—" }}</span></td>
          <td><span class="bdg {{ 'chigh' if p.confidence == 'HIGH' else 'cmed' }}">{{ '🔥 HIGH' if p.confidence == 'HIGH' else '🟡 MED' }}</span></td>
          <td><span class="bdg {{ 'pnd' if p.status == 'Pending' else 'won' if '✅' in (p.status or '') else 'lost' }}">{{ p.status }}</span></td>
          <td><span class="time-val">{{ p.fired_at[:16].replace("T"," ") if p.fired_at else "—" }}</span></td>
          <td>
            {% if p.status == "Pending" %}
            <button class="act-won" onclick="updL({{ p.id }},'✅ Won')">✅ Won</button>
            <button class="act-lost" onclick="updL({{ p.id }},'❌ Lost')">❌ Lost</button>
            {% endif %}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

</main>

<footer class="footer">
  Scanner: every 5 mins &nbsp;·&nbsp; Outcomes: auto-checked &nbsp;·&nbsp;
  Yahoo Finance + Limitless API &nbsp;·&nbsp;
  Auto-refresh: 60s
</footer>

</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<script>
function updL(id,s){
  fetch('/limitless/update/'+id+'/'+encodeURIComponent(s),{method:'POST'})
    .then(()=>location.reload())
}
function triggerScan(){
  fetch('/scan').then(()=>{
    showToast('🔍 Scan running — check Telegram in ~30 seconds');
  })
}
function showToast(msg){
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'), 3500);
}
// Auto-refresh every 60s
setTimeout(()=>location.reload(), 60000);
// Show last refresh time
console.log('Dashboard loaded:', new Date().toLocaleTimeString());
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
