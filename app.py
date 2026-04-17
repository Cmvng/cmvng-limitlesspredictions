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
    """
    import yfinance as yf
    try:
        btc  = yf.Ticker("BTC-USD")
        hist = btc.history(period="2d", interval="1h")
        if hist.empty or len(hist) < 10:
            return None
        closes  = hist["Close"].tolist()
        current = closes[-1]
        sma10   = sum(closes[-10:]) / 10
        trend   = "BUY" if current > sma10 else "SELL"
        print("BTC trend: {} (price={:.0f}, sma10={:.0f})".format(trend, current, sma10))
        return trend
    except Exception as e:
        print("BTC trend error: {}".format(e))
        return None

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
<html><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Limitless Predictions</title>
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
.wrap{padding:20px 24px}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:20px}
.stat{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px}
.slbl{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;font-weight:700}
.sval{font-size:22px;font-weight:700}
.g{color:#3fb950}.r{color:#f85149}.a{color:#e3b341}.b{color:#58a6ff}
.stit{font-size:11px;font-weight:700;color:#8b949e;text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px}
.sbtn{background:#238636;color:#fff;border:none;padding:8px 16px;border-radius:8px;font-size:13px;font-weight:700;cursor:pointer;margin-bottom:16px;margin-right:10px}
.sbtn:hover{background:#2ea043}
.tw{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:12px;min-width:700px}
th{text-align:left;padding:9px 10px;font-size:10px;color:#8b949e;text-transform:uppercase;border-bottom:1px solid #30363d;background:#161b22;font-weight:700}
td{padding:9px 10px;border-bottom:1px solid #21262d}
tr:hover td{background:#1c2128}
.bdg{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:700}
.pnd{background:#1f4068;color:#58a6ff}
.won{background:#1a4731;color:#3fb950}
.lost{background:#4a1520;color:#f85149}
.chigh{background:#1a4731;color:#3fb950}
.cmed{background:#3d2f0a;color:#e3b341}
.btn{padding:3px 8px;border-radius:5px;border:1px solid;cursor:pointer;font-size:10px;font-weight:700;margin-right:2px;background:transparent}
.btp{color:#3fb950;border-color:#238636}
.bsl{color:#f85149;border-color:#da3633}
.empty{text-align:center;padding:40px;color:#8b949e;font-size:14px}
.ref{font-size:11px;color:#30363d;text-align:right;padding:10px 24px}
</style></head><body>
<div class="hdr">
  <h1>🎯 Limitless Prediction Platform</h1>
  <div class="hdr-r">
    <span class="badge {{ 'bo' if in_window else 'bc' }}">
      {{ '🟢 Trading Window OPEN' if in_window else '🔴 Outside Trading Hours' }}
    </span>
    <span class="badge" style="background:#1f3a5f;color:#58a6ff">
      BTC: {{ '🟢 BUY' if btc_trend == 'BUY' else '🔴 SELL' if btc_trend == 'SELL' else '⚪ N/A' }}
    </span>
    <div class="live"><div class="dot"></div> Live</div>
  </div>
</div>
<div class="wrap">
  <div class="stats">
    <div class="stat"><div class="slbl">Total Sent</div><div class="sval b">{{ stats.total }}</div></div>
    <div class="stat"><div class="slbl">Win Rate</div><div class="sval {{ 'g' if stats.wr >= 65 else 'a' if stats.wr >= 50 else 'r' }}">{{ stats.wr }}%</div></div>
    <div class="stat"><div class="slbl">Wins</div><div class="sval g">{{ stats.wins }}</div></div>
    <div class="stat"><div class="slbl">Losses</div><div class="sval r">{{ stats.losses }}</div></div>
    <div class="stat"><div class="slbl">Pending</div><div class="sval a">{{ stats.pending }}</div></div>
    <div class="stat"><div class="slbl">Today</div><div class="sval b">{{ stats.today }}</div></div>
  </div>

  <button class="sbtn" onclick="triggerScan()">🔍 Scan Now</button>
  <button class="sbtn" style="background:#1f3a5f" onclick="location.reload()">🔄 Refresh</button>

  <div class="stit">Predictions Log</div>
  <div class="tw"><table>
    <thead><tr>
      <th>#</th><th>Market</th><th>Asset</th><th>Type</th>
      <th>Odds</th><th>Price@Alert</th><th>Baseline</th>
      <th>Hrs Left</th><th>Conf</th><th>Status</th><th>Time</th><th>Action</th>
    </tr></thead>
    <tbody>
      {% if not preds %}
      <tr><td colspan="12"><div class="empty">
        🎯 No predictions yet<br>
        <span style="font-size:12px;color:#8b949e">Scanner runs every 5 mins during Lagos trading hours</span>
      </div></td></tr>
      {% endif %}
      {% for p in preds %}
      <tr>
        <td style="color:#8b949e">{{ p.id }}</td>
        <td style="font-size:11px;max-width:210px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{{ p.title }}">{{ p.title }}</td>
        <td style="font-weight:700;color:#58a6ff">{{ p.asset }}</td>
        <td style="color:#8b949e;font-size:11px">{{ p.market_type }}</td>
        <td class="a" style="font-weight:700">{{ "%.1f"|format(p.bet_odds) }}%</td>
        <td style="font-size:11px">
          {{ "$%.4f"|format(p.current_price) if p.current_price and p.current_price < 100 else "$%.2f"|format(p.current_price) if p.current_price else "-" }}
        </td>
        <td style="font-size:11px">
          {{ "$%.4f"|format(p.baseline) if p.baseline < 100 else "$%.2f"|format(p.baseline) }}
        </td>
        <td style="color:#8b949e">{{ "%.1fh"|format(p.hours_left) if p.hours_left else "-" }}</td>
        <td><span class="bdg {{ 'chigh' if p.confidence == 'HIGH' else 'cmed' }}">{{ p.confidence }}</span></td>
        <td><span class="bdg {{ 'pnd' if p.status == 'Pending' else 'won' if '✅' in (p.status or '') else 'lost' }}">{{ p.status }}</span></td>
        <td style="color:#8b949e;font-size:11px">{{ p.fired_at[:16].replace("T"," ") if p.fired_at else "" }}</td>
        <td>
          {% if p.status == "Pending" %}
          <button class="btn btp" onclick="updL({{ p.id }},'✅ Won')">Won</button>
          <button class="btn bsl" onclick="updL({{ p.id }},'❌ Lost')">Lost</button>
          {% endif %}
        </td>
      </tr>
      {% endfor %}
    </tbody>
  </table></div>
</div>
<div class="ref">Scanner: every 5 mins · Outcomes: auto-checked · Powered by Yahoo Finance + Limitless API</div>
<script>
function updL(id,s){
  fetch('/limitless/update/'+id+'/'+encodeURIComponent(s),{method:'POST'})
    .then(()=>location.reload())
}
function triggerScan(){
  fetch('/scan').then(()=>alert('Scan running — check Telegram in ~30 seconds'))
}
setTimeout(()=>location.reload(),60000);
</script>
</body></html>"""

@app.route("/")
def dashboard():
    from flask import render_template_string
    conn     = get_db()
    lp_rows  = conn.run("SELECT * FROM limitless_predictions ORDER BY id DESC")
    lp_cols  = [c['name'] for c in conn.columns]
    preds    = [dict(zip(lp_cols, r)) for r in lp_rows]
    conn.close()

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

    return render_template_string(
        DASHBOARD_HTML,
        preds=preds, stats=stats,
        btc_trend=get_btc_trend(),
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
