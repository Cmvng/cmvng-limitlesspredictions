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

LAGOS_TZ      = timezone(timedelta(hours=1))
LIMITLESS_API = "https://api.limitless.exchange"

# Global BTC trend cache
_btc_trend_cache = {"trend": None, "price": None, "sma10": None, "updated": None}
# Debug log for last scan
_last_scan_log = {"time": None, "total": 0, "qualified": 0, "filtered": []}

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
            slug          TEXT
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
            fired_at        TEXT
        )
    """)
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

    # 1. Time window
    if not is_lagos_window() and not is_fav:
        return reject("outside Lagos window")

    # 2. Expiry filter
    if p["is_short"]:
        # Short-term (minutes/hourly): need 5-30 mins left, OR favourite bypasses
        if not is_fav and not (5 <= p["mins_left"] <= 30):
            return reject("short-term not in 5-30 min window (got {:.0f} mins)".format(p["mins_left"]))
    else:
        # Daily: 0.5-10 hours
        if p["hours_left"] < 0.5:
            return reject("daily too close to expiry ({:.1f}h)".format(p["hours_left"]))
        if p["hours_left"] > 10 and not is_fav:
            return reject("daily too far out ({:.1f}h)".format(p["hours_left"]))

    # 3. Price must exist
    if price is None:
        return reject("no Yahoo price for {}".format(p["asset"]))

    # 4. Price on winning side
    if p["direction"] == "above":
        if price <= p["baseline"]:
            return reject("price ${:.4f} not above baseline ${:.4f}".format(price, p["baseline"]))
        btc_aligned = (btc_trend == "BUY") if btc_trend else True
    else:
        if price >= p["baseline"]:
            return reject("price ${:.4f} not below baseline ${:.4f}".format(price, p["baseline"]))
        btc_aligned = (btc_trend == "SELL") if btc_trend else True

    odds = p["yes_odds"]

    # 5. Odds filter (now using 0-100 scale properly)
    if not (73 <= odds <= 99):
        return reject("odds {:.1f}% outside 73-99% range".format(odds))

    # Confidence
    if not btc_aligned and btc_trend:
        confidence = "MEDIUM"
    elif odds >= 90 or (odds >= 80 and btc_aligned):
        confidence = "HIGH"
    else:
        confidence = "MEDIUM"

    # Size
    if odds >= 94:
        size_rec = "$20-50 (high odds — go with size)"
    elif odds >= 85:
        size_rec = "$10-20 (normal size)"
    else:
        size_rec = "$5-10 (cautious)"

    reversal = ""
    if p["is_short"] and p["mins_left"] <= 60 and 78 <= odds <= 88:
        reversal = "⚠️ Reversal risk — watch carefully"

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
            mt="Short" if p["is_short"] else "Daily",
            now=now, slg=p["slug"]
        )
        pid = rows[0][0]
        conn.close()

        trend_str  = "🟢 Bullish" if btc_trend == "BUY" else "🔴 Bearish" if btc_trend == "SELL" else "⚪ Unknown"
        conf_emoji = "🔥" if score["confidence"] == "HIGH" else "🟡"
        hrs_str    = "{:.1f} hrs".format(p["hours_left"]) if p["hours_left"] >= 1 else "{:.0f} mins".format(p["mins_left"])
        exp_str    = p["expiry_dt"].strftime("%d %b %H:%M UTC")

        msg = (
            "🎯 <b>PREDICTION #{}</b>\n"
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
            "Short ⏱" if p["is_short"] else "Daily 📅",
            conf_emoji, score["confidence"],
            score["size_rec"], trend_str,
            score["reversal"] + "\n" if score["reversal"] else "",
            p["slug"]
        )
        send_telegram(msg)
        print("ALERT #{}: {} at {:.1f}%".format(pid, p["title"][:50], score["bet_odds"]))
    except Exception as e:
        print("Alert error: {}".format(e))

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
                    won = (price > p["baseline"]) if p["direction"] == "above" else (price < p["baseline"])
                    outcome = "WIN" if won else "LOSS"
                    status = "✅ Won" if won else "❌ Lost"
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
                        "<b>Closed:</b> {}\n"
                        "<b>Baseline:</b> {}".format(
                            emoji, outcome, p["id"], p["title"],
                            fmt_price(price), fmt_price(p["baseline"])
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
    """Try all 3 football APIs in order: API-Football → football-data.org → TheSportsDB"""
    # Try API-Football first (richest data)
    fixtures = _fetch_api_football()
    if fixtures:
        return fixtures
    # Fallback to football-data.org
    fixtures = _fetch_football_data()
    if fixtures:
        return fixtures
    # Final fallback — TheSportsDB (no key needed)
    return _fetch_thesportsdb()

def analyze_match_with_claude(match):
    """Use Claude Haiku to analyze a match and output picks — CHEAP model"""
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
            "Return a JSON array of 5-8 DIVERSE prediction picks covering different risk levels.\n\n"
            "REQUIRED DISTRIBUTION:\n"
            "- 2 SAFE picks: confidence 85-95, implied_odds 1.03-1.25\n"
            "- 2 MEDIUM picks: confidence 75-84, implied_odds 1.25-1.60\n"
            "- 2 VALUE picks: confidence 65-74, implied_odds 1.60-2.50\n"
            "- 1-2 MEGA LONGSHOT picks: confidence 55-70, implied_odds 2.50-8.00 (for 100x accumulator)\n\n"
            "STRICT RULES:\n"
            "- pick_value MUST be ONE OF (exact match): Yes, No, Home, Away, Draw, Over, Under\n"
            "- pick_type MUST be one of: match_winner, both_teams_score, over_0.5_goals, over_1.5_goals, over_2.5_goals, over_3.5_goals, draw_no_bet, double_chance\n"
            "- NEVER leave pick_value blank\n"
            "- implied_odds must be realistic decimal odds (1.01-10.00)\n"
            "- reasoning: max 100 chars, useful context\n\n"
            'Example: [{{"pick_type":"over_0.5_goals","pick_value":"Yes","confidence":92,"implied_odds":1.06,"reasoning":"both teams avg 2.1 goals scored"}},'
            '{{"pick_type":"match_winner","pick_value":"Home","confidence":68,"implied_odds":2.10,"reasoning":"home team 7W-1L last 8 home games"}}]\n\n'
            "Output ONLY the JSON array. No markdown, no preamble, no commentary."
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
                "max_tokens": 2048,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        if r.status_code != 200:
            print("Claude error: {}".format(r.text[:200]))
            return None
        data = r.json()
        text = data["content"][0]["text"].strip()
        # Strip code fences
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        return json.loads(text)
    except Exception as e:
        print("Claude analyze error: {}".format(e))
        return None

def build_accumulators(picks):
    """Build 2x / 3x / 10x / 100x accumulators. Each MATCH appears in at most ONE tier.
    Smart distribution so all 4 tiers get populated."""
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

    used_matches = set()

    def build_tier(target_odds, strategy, max_picks=None):
        """Build a tier with optional max_picks cap to leave picks for other tiers."""
        tier_picks = []
        cumulative = 1.0
        candidates = []

        for match, match_picks in match_groups.items():
            if match in used_matches:
                continue
            best = match_picks[0]
            implied = float(best.get("implied_odds") or 1.0)
            conf = float(best.get("confidence") or 0)

            if strategy == "safe":
                if conf >= 85 and implied >= 1.03:
                    candidates.append((best, match, implied, conf))
            elif strategy == "medium":
                # Any pick per match, wider range
                for p in match_picks:
                    pi = float(p.get("implied_odds") or 1.0)
                    pc = float(p.get("confidence") or 0)
                    if 75 <= pc < 90 and 1.15 <= pi <= 1.70:
                        candidates.append((p, match, pi, pc))
                        break
                else:
                    # Fallback: accept the best pick if in acceptable range
                    if 70 <= conf < 90 and implied >= 1.20:
                        candidates.append((best, match, implied, conf))
            elif strategy == "value":
                for p in match_picks:
                    pi = float(p.get("implied_odds") or 1.0)
                    pc = float(p.get("confidence") or 0)
                    if 1.50 <= pi <= 2.80 and pc >= 60:
                        candidates.append((p, match, pi, pc))
                        break
            elif strategy == "mega":
                for p in match_picks:
                    pi = float(p.get("implied_odds") or 1.0)
                    pc = float(p.get("confidence") or 0)
                    if pi >= 2.50 and pc >= 50:
                        candidates.append((p, match, pi, pc))
                        break

        if strategy in ("safe", "medium"):
            candidates.sort(key=lambda x: -x[3])
        else:
            candidates.sort(key=lambda x: -x[2])

        for pick, match, implied, conf in candidates:
            if match in used_matches:
                continue
            if max_picks and len(tier_picks) >= max_picks:
                break
            if cumulative >= target_odds:
                break
            tier_picks.append(pick)
            used_matches.add(match)
            cumulative *= implied

        return {"picks": tier_picks, "total_odds": round(cumulative, 2)}

    total_matches = len(match_groups)
    # Limit each tier to leave picks for others — divide available matches fairly
    safe_cap   = max(3, min(6, total_matches // 3))
    medium_cap = max(3, min(5, total_matches // 4))
    value_cap  = max(3, min(5, total_matches // 4))

    safe   = build_tier(2.0,   "safe",   max_picks=safe_cap)
    medium = build_tier(3.0,   "medium", max_picks=medium_cap)
    value  = build_tier(10.0,  "value",  max_picks=value_cap)
    mega   = build_tier(100.0, "mega")  # no cap, uses what's left

    return {
        "safe_2x":   safe,
        "medium_3x": medium,
        "value_10x": value,
        "mega_100x": mega,
    }


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

def analyze_otp_market_with_claude(market, parsed_odds):
    """Use Claude Haiku to judge if an Off The Pitch market is a sure winner"""
    if not ANTHROPIC_KEY:
        return None
    import requests as req
    try:
        title = market.get("title", "")
        yes_odds = parsed_odds["yes_odds"]
        no_odds  = 100 - yes_odds
        hours    = parsed_odds["hours_left"]
        
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
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        if r.status_code != 200:
            print("OTP Claude error: {}".format(r.status_code))
            return None
        data = r.json()
        text = data["content"][0]["text"].strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
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
        
        now = datetime.now(timezone.utc).isoformat()
        conn = get_db()
        rows = conn.run(
            """INSERT INTO football_picks
            (match_id, home_team, away_team, competition, kickoff_time,
             pick_type, pick_value, confidence, reasoning, implied_odds,
             accumulator_tier, status, fired_at)
            VALUES (:mid, :h, :a, :c, :k, 'limitless_otp', :pv, :conf, :r, :o, 'single', 'Pending', :now)
            RETURNING id""",
            mid=parsed["market_id"], h="", a="", c="Limitless OTP",
            k=parsed["expiry_dt"].isoformat(), pv=action,
            conf=conf, r=reasoning[:200],
            o=parsed["yes_odds"] if action == "YES" else (100 - parsed["yes_odds"]),
            now=now
        )
        pid = rows[0][0]
        conn.close()
        
        odds_val = parsed["yes_odds"] if action == "YES" else (100 - parsed["yes_odds"])
        hrs_str = "{:.1f} hrs".format(parsed["hours_left"]) if parsed["hours_left"] >= 1 else "{:.0f} mins".format(parsed["mins_left"])
        conf_emoji = "🔥" if conf >= 85 else "🟡"
        
        msg = (
            "⚽ <b>OFF THE PITCH PICK #{}</b>\n"
            "──────────────────────────\n"
            "📌 {}\n"
            "──────────────────────────\n"
            "<b>Bet:</b> {} ✅\n"
            "<b>Market Odds:</b> {:.1f}%\n"
            "<b>Time Left:</b> {}\n"
            "──────────────────────────\n"
            "{} <b>AI Confidence:</b> {}%\n"
            "💭 <b>Reasoning:</b> {}\n"
            "🔗 limitless.exchange/markets/{}"
        ).format(
            pid, parsed["title"],
            action, odds_val, hrs_str,
            conf_emoji, conf, reasoning,
            parsed["slug"]
        )
        send_telegram(msg)
        print("OTP alert #{}: {} ({})".format(pid, parsed["title"][:50], action))
    except Exception as e:
        print("OTP alert error: {}".format(e))

def run_otp_scan():
    """Scan Limitless for sports/OTP markets using multiple strategies."""
    import requests as req
    if not ANTHROPIC_KEY:
        print("OTP scan skipped — no ANTHROPIC_API_KEY")
        return 0
    try:
        otp_markets = []

        # Strategy 1: fetch with sports automation filter
        try:
            r = req.get(
                "{}/markets/active?automationType=sports&limit=100".format(LIMITLESS_API),
                timeout=15
            )
            if r.status_code == 200:
                otp_markets = r.json().get("data", [])
                print("OTP strategy 1 (automationType=sports): {} markets".format(len(otp_markets)))
        except Exception as e:
            print("OTP strategy 1 failed: {}".format(e))

        # Strategy 2: fetch ALL markets and aggressively filter
        if not otp_markets:
            try:
                # Fetch multiple pages
                for page in range(1, 4):  # up to 3 pages
                    r = req.get(
                        "{}/markets/active?page={}&limit=50".format(LIMITLESS_API, page),
                        timeout=15
                    )
                    if r.status_code != 200:
                        break
                    markets = r.json().get("data", [])
                    if not markets:
                        break
                    page_otp = [m for m in markets if is_otp_market(m)]
                    otp_markets.extend(page_otp)
                    if len(markets) < 50:
                        break  # last page
                print("OTP strategy 2 (title filter across pages): {} markets found".format(
                    len(otp_markets)))
            except Exception as e:
                print("OTP strategy 2 failed: {}".format(e))

        if not otp_markets:
            print("OTP scan: NO sports markets found — this likely means none are live now")
            return 0
        
        # Get already-alerted
        conn = get_db()
        alerted = conn.run(
            "SELECT match_id FROM football_picks WHERE fired_at::timestamptz > NOW() - INTERVAL '6 hours' AND pick_type='limitless_otp'"
        )
        alerted_ids = set(str(r[0]) for r in alerted)
        conn.close()
        
        count = 0
        for market in otp_markets[:15]:  # Cap at 15 to control costs
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
                
                # Only analyze markets with interesting odds (not extremes)
                if yes_odds < 20 or yes_odds > 90:
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
                
                analysis = analyze_otp_market_with_claude(market, parsed)
                if analysis and analysis.get("action") in ("YES", "NO"):
                    # Only fire if AI confidence >= 75
                    if analysis.get("confidence", 0) >= 75:
                        save_and_alert_otp(market, parsed, analysis)
                        count += 1
                        time.sleep(2)
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
    This prevents duplicate matches accumulating from multiple runs."""
    try:
        conn = get_db()
        now = datetime.now(timezone.utc).isoformat()
        # Wipe ALL old pending accumulator picks — a new batch is fully replacing them
        conn.run(
            "UPDATE football_picks SET status='Replaced' "
            "WHERE status='Pending' AND pick_type != 'limitless_otp' "
            "AND accumulator_tier IN ('safe_2x','medium_3x','value_10x','mega_100x')"
        )
        # Save new accumulators
        for tier_name, tier in accas.items():
            for p in tier.get("picks", []):
                match_str = p.get("match", "")
                home, away = "", ""
                if " vs " in match_str:
                    parts = match_str.split(" vs ", 1)
                    home, away = parts[0].strip(), parts[1].strip()
                conn.run(
                    """INSERT INTO football_picks
                    (match_id, home_team, away_team, competition, kickoff_time,
                     pick_type, pick_value, confidence, reasoning, implied_odds,
                     accumulator_tier, status, fired_at)
                    VALUES (:m, :h, :a, :c, :k, :pt, :pv, :conf, :r, :o, :tier, 'Pending', :now)""",
                    m=match_str[:100], h=home[:50], a=away[:50],
                    c=p.get("competition", "")[:50],
                    k=p.get("kickoff", ""),
                    pt=p.get("pick_type", ""),
                    pv=str(p.get("pick_value", ""))[:50],
                    conf=float(p.get("confidence", 0)),
                    r=str(p.get("reasoning", ""))[:200],
                    o=float(p.get("implied_odds", 1.0)),
                    tier=tier_name,
                    now=now
                )
        conn.close()
        print("Accumulators saved to DB")
    except Exception as e:
        print("Save accumulator error: {}".format(e))

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
            for match in fixtures[:12]:  # limit to 12 matches per run
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

                for tier_name, tier in accas.items():
                    if not tier.get("picks"):
                        continue
                    label = {"safe_2x": "🟢 <b>Safe Bet</b>",
                             "medium_3x": "🟡 <b>Medium Risk</b>",
                             "value_10x": "🔥 <b>Value (10x Target)</b>",
                             "mega_100x": "🚀 <b>Mega Long Shot (100x Target)</b>"}[tier_name]
                    msg += "{} — {:.2f}x total\n".format(label, tier["total_odds"])
                    msg += "─────────────────\n"
                    for p in tier["picks"]:
                        match = p.get("match", "Unknown")
                        comp = p.get("competition", "")
                        ko = fmt_kickoff(p.get("kickoff", ""))
                        pick_type = p.get("pick_type", "").replace("_", " ").title()
                        pick_val = clean_value(p.get("pick_value", ""))
                        conf = int(p.get("confidence", 0))

                        msg += "⚡ <b>{}</b>\n".format(match)
                        if comp or ko:
                            line = []
                            if comp:
                                line.append(comp)
                            if ko:
                                line.append(ko)
                            msg += "   🏆 <i>{}</i>\n".format(" · ".join(line))
                        msg += "   → {}: <b>{}</b> ({}%)\n\n".format(pick_type, pick_val, conf)
                    msg += "\n"
                msg += "💡 <i>Each match appears only once across all tiers</i>\n"
                msg += "📊 <i>View all picks on your dashboard</i>"
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
      <a href="/" class="nav-tab active">Crypto</a>
      <a href="/football" class="nav-tab">Football</a>
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
        <th>#</th><th>Market</th><th>Asset</th><th>Type</th><th>Odds</th>
        <th>Price @ Alert</th><th>Baseline</th><th>Time Left</th>
        <th>Confidence</th><th>Status</th><th>Logged</th><th>Action</th>
      </tr></thead>
      <tbody>
        {% if not preds %}
        <tr><td colspan="12">
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
          <td><span class="cell-type">{{ p.market_type }}</span></td>
          <td><span class="cell-odds">{{ "%.1f"|format(p.bet_odds) }}%</span></td>
          <td><span class="cell-price">{{ "$%.4f"|format(p.current_price) if p.current_price and p.current_price < 100 else "$%.2f"|format(p.current_price) if p.current_price else "—" }}</span></td>
          <td><span class="cell-price">{{ "$%.4f"|format(p.baseline) if p.baseline < 100 else "$%.2f"|format(p.baseline) }}</span></td>
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
      <a href="/" class="nav-tab">Crypto</a>
      <a href="/football" class="nav-tab active">Football</a>
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
        <span class="pick-type">{{ pick.pick_type.replace("_", " ") }}</span>
        <span class="pick-value">{{ pick.pick_value or "—" }}</span>
        <div class="conf-bar"><div class="conf-bar-fill" style="width:{{ pick.confidence|int }}%"></div></div>
        <span class="pick-conf">{{ pick.confidence|int }}%</span>
      </div>
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
          <td style="font-weight:500;color:var(--ink);max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{{ p.match_id }}</td>
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
{% if history_picks %}
<div class="section-head">
  <div><span class="section-title">Recent Results</span></div>
</div>
<div class="hist-wrap">
  <div class="table-scroll">
    <table>
      <thead><tr>
        <th>Match</th><th>League</th><th>Pick</th><th>Confidence</th><th>Outcome</th><th>Resolved</th>
      </tr></thead>
      <tbody>
        {% for p in history_picks %}
        <tr>
          <td style="font-weight:500;color:var(--ink);max-width:260px">{{ p.match_id }}</td>
          <td style="font-family:var(--mono);font-size:11px;color:var(--ink-3)">{{ p.competition or "—" }}</td>
          <td><span class="pick-value">{{ p.pick_value or "—" }}</span> <small style="color:var(--ink-4)">{{ p.pick_type.replace("_", " ") }}</small></td>
          <td><span class="pick-conf">{{ p.confidence|int }}%</span></td>
          <td><span class="status-chip {{ 'status-won' if '✅' in (p.status or '') else 'status-lost' }}">{{ p.status }}</span></td>
          <td style="font-family:var(--mono);font-size:11px;color:var(--ink-4)">{{ p.resolved_at[:16].replace("T"," ") if p.resolved_at else "—" }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% endif %}

<footer class="footer">Accumulators update every 6 hours · Past matches auto-filtered · Auto-refresh 60s</footer>
</div>

<script>setTimeout(()=>location.reload(),60000);</script>
</body></html>"""

@app.route("/")
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

def _group_picks_into_slips(picks, target_odds, max_picks_per_slip=5):
    """Group picks into multiple accumulator slips each reaching target odds."""
    if not picks:
        return []
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
        if current_odds >= target_odds or len(current_slip) >= max_picks_per_slip:
            slips.append({"picks": current_slip, "total_odds": round(current_odds, 2),
                          "slip_number": slip_number})
            current_slip = []
            current_odds = 1.0
            slip_number += 1
    if current_slip and current_odds >= 1.3:
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

@app.route("/football")
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
    safe_slips   = _group_picks_into_slips(safe_picks,   target_odds=2.0,   max_picks_per_slip=4)
    medium_slips = _group_picks_into_slips(medium_picks, target_odds=3.0,   max_picks_per_slip=4)
    value_slips  = _group_picks_into_slips(value_picks,  target_odds=10.0,  max_picks_per_slip=5)
    mega_slips   = _group_picks_into_slips(mega_picks,   target_odds=100.0, max_picks_per_slip=7)

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
