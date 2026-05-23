"""
═══════════════════════════════════════════════════════════════════════════════
P4.0 — Standalone Claude prediction engine
═══════════════════════════════════════════════════════════════════════════════

What this is:
  Raw candles → Claude → direction + confidence → order on Polymarket

What this is NOT:
  Not coupled to P3.0
  Not using P3.0's balance, filters, or stake logic
  Not falling back to rules when Claude is uncertain
  Not deferring to anything

This is the manual chart-reading approach (68.7% in clean tests) automated.

DEPLOY:
  1. (ANTHROPIC_API_KEY already set on your Railway — used by football module)
  2. Append to end of app.py
  3. POST /api/p40/enable → starts shadow mode
  4. After 1hr verification → POST /api/p40/go_live
"""

# P4.0 uses the existing ANTHROPIC_KEY (already defined at line 753 of app.py).
# Calls the API directly via requests — same pattern as analyze_match_with_claude().
# No additional SDK install needed.


# ═════════════════════════════════════════════════════════════════════════════
# P4.0 OWN STATE — independent of P3.0
# ═════════════════════════════════════════════════════════════════════════════

P40_CONFIG = {
    "enabled": False,
    "shadow_mode": True,
    "model": "claude-sonnet-4-5",
    "assets": ["BTC", "ETH"],
    "lead_time_seconds": 15,
    "memory_depth": 20,
    "min_confidence_to_fire": 6,
    "stake_usd": 2.50,
}

# P4.0 own balance pool — separate from P3.0
_p40_state = {
    "balance": 20.0,
    "starting_balance": 20.0,
    "peak_balance": 20.0,
    "floor_balance": 5.0,
    "trades_today": 0,
    "wins_today": 0,
    "losses_today": 0,
}

_p40_predicted_boundaries = set()
_p40_traded_keys = set()


# ═════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═════════════════════════════════════════════════════════════════════════════

def _p40_create_table():
    sql = """
    CREATE TABLE IF NOT EXISTS p40_predictions (
        id BIGSERIAL PRIMARY KEY,
        asset VARCHAR(10) NOT NULL,
        candle_close_ts TIMESTAMP WITH TIME ZONE NOT NULL,
        direction VARCHAR(5) NOT NULL,
        confidence INTEGER NOT NULL,
        reasoning TEXT,
        pattern VARCHAR(50),
        model VARCHAR(50),
        api_elapsed_ms INTEGER,
        best_ask FLOAT,
        fired BOOLEAN DEFAULT FALSE,
        fire_reason TEXT,
        stake FLOAT,
        polymarket_order_id VARCHAR(100),
        order_status VARCHAR(20),
        fill_price FLOAT,
        ptb_open FLOAT,
        next_close FLOAT,
        actual_direction VARCHAR(5),
        outcome VARCHAR(10),
        pnl FLOAT,
        balance_after FLOAT,
        resolved_at TIMESTAMP WITH TIME ZONE,
        shadow_mode BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_p40_asset_ts ON p40_predictions(asset, candle_close_ts DESC);
    CREATE INDEX IF NOT EXISTS idx_p40_unfired ON p40_predictions(candle_close_ts) WHERE fired = FALSE;
    CREATE INDEX IF NOT EXISTS idx_p40_unresolved ON p40_predictions(candle_close_ts) WHERE outcome IS NULL;
    
    CREATE TABLE IF NOT EXISTS p40_balance_state (
        id INTEGER PRIMARY KEY DEFAULT 1,
        balance FLOAT NOT NULL,
        peak_balance FLOAT NOT NULL,
        starting_balance FLOAT NOT NULL,
        floor_balance FLOAT NOT NULL,
        trades_today INTEGER DEFAULT 0,
        wins_today INTEGER DEFAULT 0,
        losses_today INTEGER DEFAULT 0,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        CONSTRAINT singleton CHECK (id = 1)
    );
    """
    try:
        _db = get_db()
        _db.run(sql)
        # Load existing balance if exists
        rows = _db.run("SELECT balance, peak_balance, starting_balance, floor_balance, trades_today, wins_today, losses_today FROM p40_balance_state WHERE id=1")
        existing = list(rows)
        if existing:
            r = existing[0]
            _p40_state["balance"] = r[0]
            _p40_state["peak_balance"] = r[1]
            _p40_state["starting_balance"] = r[2]
            _p40_state["floor_balance"] = r[3]
            _p40_state["trades_today"] = r[4] or 0
            _p40_state["wins_today"] = r[5] or 0
            _p40_state["losses_today"] = r[6] or 0
            print("[P4.0] Loaded existing balance: ${:.2f} (peak ${:.2f})".format(
                _p40_state["balance"], _p40_state["peak_balance"]))
        else:
            # Initialize
            _db.run("""
                INSERT INTO p40_balance_state (id, balance, peak_balance, starting_balance, floor_balance)
                VALUES (1, :b, :p, :s, :f)
            """, b=_p40_state["balance"], p=_p40_state["peak_balance"],
                s=_p40_state["starting_balance"], f=_p40_state["floor_balance"])
            print("[P4.0] Initialized fresh balance: ${:.2f}".format(_p40_state["balance"]))
        _db.close()
        print("[P4.0] p40_predictions table ready")
    except Exception as _e:
        print("[P4.0] Table creation error: {}".format(_e))


def _p40_save_balance():
    """Persist balance state to DB after every change."""
    try:
        _db = get_db()
        _db.run("""
            UPDATE p40_balance_state
            SET balance=:b, peak_balance=:p, trades_today=:t,
                wins_today=:w, losses_today=:l, updated_at=NOW()
            WHERE id=1
        """, b=_p40_state["balance"], p=_p40_state["peak_balance"],
            t=_p40_state["trades_today"], w=_p40_state["wins_today"],
            l=_p40_state["losses_today"])
        _db.close()
    except Exception as e:
        print("[P4.0] Balance save error: {}".format(e))


# ═════════════════════════════════════════════════════════════════════════════
# CLAUDE PROMPT — same approach we used manually
# ═════════════════════════════════════════════════════════════════════════════

def _p40_build_prompt(asset, candles, memory):
    candle_lines = []
    for i, c in enumerate(candles[-100:]):
        candle_lines.append("{:3}: O={:.1f} H={:.1f} L={:.1f} C={:.1f}".format(
            i+1, c[0], c[1], c[2], c[3]))
    
    memory_lines = []
    wins = 0; total = 0
    for m in memory[-20:]:
        outcome = m.get("outcome", "PENDING")
        if outcome == "WIN":
            wins += 1; total += 1
        elif outcome == "LOSS":
            total += 1
        memory_lines.append("  {} c{}: {} → {}".format(
            m["direction"], m["confidence"], (m.get("reasoning") or "")[:50], outcome))
    
    recent_wr = (wins / total * 100) if total else 0
    
    system_prompt = ("You are an expert 15-minute crypto chart reader specializing in " + asset + ". "
"Predict whether the NEXT candle will close HIGHER (UP) or LOWER (DOWN) than current close.\n\n"
"RULES:\n"
"1. Predict EVERY candle - never neutral\n"
"2. Read context: cascade? range? reversal? where in the move?\n"
"3. CASCADE TRAP: 4+ same-direction candles ACCELERATING → continuation likely. "
"DECELERATING (last body shrinking) → reversal likely\n"
"4. Isolated mega candles (>1.5x ATR) → bounce typical UNLESS accelerating cascade\n"
"5. Doji at extremes (top of uptrend / bottom of downtrend) → reversal\n"
"6. Pullback common after 3 same-direction candles\n"
"7. Big counter-trend candles in trends often faded\n\n"
"CONFIDENCE:\n"
"- 1-5: Uncertain (skipped)\n"
"- 6: Clear bias\n"
"- 7: Strong setup\n"
"- 8: Textbook high-conviction\n"
"- 9-10: Extreme (rare)\n\n"
"OUTPUT - ONLY this JSON, no markdown:\n"
'{"direction":"UP"|"DOWN","confidence":1-10,"reasoning":"short","pattern":"short_name"}\n\n'
"Learn from your recent mistakes shown below.")
    
    user_prompt = (
        "ASSET: " + asset + "\n"
        "CURRENT PRICE: $" + "{:,.1f}".format(candles[-1][3]) + "\n\n"
        "LAST 100 CANDLES (15-min, oldest→newest):\n"
        + "\n".join(candle_lines) + "\n\n"
        "YOUR LAST 20 PREDICTIONS ON " + asset + ":\n"
        + ("\n".join(memory_lines) if memory_lines else "  (none yet)") + "\n\n"
        "Recent WR: " + "{:.1f}".format(recent_wr) + "% (" + str(wins) + "/" + str(total) + ")\n\n"
        "Predict the NEXT candle. JSON only."
    )
    
    return system_prompt, user_prompt


def _p40_claude_predict(asset, candles, memory):
    """Calls Claude API via requests (mirrors analyze_match_with_claude pattern)."""
    import json as _json
    import time as _time
    import requests as _req
    import re as _re
    import traceback as _tb
    
    if not ANTHROPIC_KEY:
        print("[P4.0] {} no ANTHROPIC_KEY".format(asset))
        return None
    if not candles or len(candles) < 50:
        return None
    
    try:
        sys_p, user_p = _p40_build_prompt(asset, candles, memory)
        
        t0 = _time.time()
        r = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": P40_CONFIG["model"],
                "max_tokens": 400,
                "system": sys_p,
                "messages": [{"role": "user", "content": user_p}]
            },
            timeout=30
        )
        elapsed_ms = int((_time.time() - t0) * 1000)
        
        if r.status_code != 200:
            print("[P4.0] {} API error {}: {}".format(asset, r.status_code, r.text[:200]))
            return None
        
        data = r.json()
        raw = data["content"][0]["text"].strip()
        # Strip markdown fences
        raw = _re.sub(r'^```(?:json)?\s*', '', raw)
        raw = _re.sub(r'\s*```$', '', raw)
        
        pred = _json.loads(raw)
        if pred.get("direction") not in ("UP", "DOWN"):
            print("[P4.0] {} invalid direction: {}".format(asset, pred))
            return None
        
        conf = int(pred.get("confidence", 0))
        if not (1 <= conf <= 10):
            print("[P4.0] {} invalid conf: {}".format(asset, conf))
            return None
        
        pred["confidence"] = conf
        pred["api_elapsed_ms"] = elapsed_ms
        pred["model"] = P40_CONFIG["model"]
        return pred
    
    except Exception as e:
        print("[P4.0] {} API error: {}".format(asset, e))
        _tb.print_exc()
        return None


def _p40_load_memory(asset, depth=20):
    try:
        _db = get_db()
        rows = _db.run("""
            SELECT direction, confidence, reasoning, pattern, outcome
            FROM p40_predictions
            WHERE asset=:a AND outcome IS NOT NULL
            ORDER BY id DESC LIMIT :d
        """, a=asset, d=depth)
        out = []
        for r in reversed(list(rows)):
            out.append({"direction": r[0], "confidence": r[1],
                       "reasoning": r[2] or "", "pattern": r[3] or "",
                       "outcome": r[4] or "PENDING"})
        _db.close()
        return out
    except Exception as e:
        print("[P4.0] Memory load error: {}".format(e))
        return []


def _p40_next_close():
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    nm = ((now.minute // 15) + 1) * 15
    if nm >= 60:
        return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return now.replace(minute=nm, second=0, microsecond=0)


# ═════════════════════════════════════════════════════════════════════════════
# PREDICT LOOP
# ═════════════════════════════════════════════════════════════════════════════

def _p40_predict_loop():
    import time as _time
    from datetime import datetime, timezone
    
    print("[P4.0] Predict loop started")
    
    while True:
        try:
            if not P40_CONFIG["enabled"] or not ANTHROPIC_KEY:
                _time.sleep(30); continue
            
            now = datetime.now(timezone.utc)
            nxt = _p40_next_close()
            secs_until = (nxt - now).total_seconds()
            lead = P40_CONFIG["lead_time_seconds"]
            
            if secs_until > lead + 5:
                _time.sleep(min(secs_until - lead - 2, 60))
                continue
            
            key = nxt.isoformat()
            if key in _p40_predicted_boundaries:
                _time.sleep(2); continue
            
            print("[P4.0] WAKE T-{}s for {}".format(int(secs_until), nxt))
            _p40_predicted_boundaries.add(key)
            
            for asset in P40_CONFIG["assets"]:
                try:
                    raw = _p29cl_candle_prefetch.get(asset, [])
                    if len(raw) < 50:
                        print("[P4.0] {} insufficient candles ({})".format(asset, len(raw)))
                        continue
                    
                    candles = list(raw[-100:])
                    memory = _p40_load_memory(asset, P40_CONFIG["memory_depth"])
                    
                    pred = _p40_claude_predict(asset, candles, memory)
                    if pred is None:
                        continue
                    
                    try:
                        _db = get_db()
                        _db.run("""
                            INSERT INTO p40_predictions
                            (asset, candle_close_ts, direction, confidence, reasoning, pattern,
                             model, api_elapsed_ms, shadow_mode)
                            VALUES (:a, :cc, :d, :c, :r, :p, :m, :ms, :sh)
                        """,
                            a=asset, cc=nxt, d=pred["direction"], c=pred["confidence"],
                            r=(pred.get("reasoning") or "")[:200],
                            p=(pred.get("pattern") or "")[:50],
                            m=pred["model"], ms=pred["api_elapsed_ms"],
                            sh=P40_CONFIG["shadow_mode"])
                        _db.close()
                    except Exception as se:
                        print("[P4.0] Save error {}: {}".format(asset, se))
                    
                    print("[P4.0] {} {} c{} ({}) '{}' [{}ms]".format(
                        asset, pred["direction"], pred["confidence"],
                        pred.get("pattern", ""), (pred.get("reasoning") or "")[:40],
                        pred["api_elapsed_ms"]))
                
                except Exception as e:
                    print("[P4.0] {} predict error: {}".format(asset, e))
                    import traceback; traceback.print_exc()
            
            if len(_p40_predicted_boundaries) > 100:
                keep = sorted(_p40_predicted_boundaries)[-50:]
                _p40_predicted_boundaries.clear()
                _p40_predicted_boundaries.update(keep)
            
            _time.sleep(max(2, secs_until + 2))
        
        except Exception as e:
            print("[P4.0] Predict loop error: {}".format(e))
            import traceback; traceback.print_exc()
            _time.sleep(10)


# ═════════════════════════════════════════════════════════════════════════════
# FIRE LOOP
# ═════════════════════════════════════════════════════════════════════════════

def _p40_fire_loop():
    import time as _time
    from datetime import datetime, timezone, timedelta
    
    print("[P4.0] Fire loop started")
    
    while True:
        try:
            if not P40_CONFIG["enabled"]:
                _time.sleep(30); continue
            
            now = datetime.now(timezone.utc)
            
            _db = get_db()
            rows = _db.run("""
                SELECT id, asset, candle_close_ts, direction, confidence, reasoning, pattern, shadow_mode
                FROM p40_predictions
                WHERE fired=FALSE AND outcome IS NULL
                  AND candle_close_ts <= :now AND candle_close_ts > :stale
                ORDER BY candle_close_ts LIMIT 10
            """, now=now, stale=now - timedelta(minutes=5))
            pending = list(rows)
            _db.close()
            
            if not pending:
                _time.sleep(2); continue
            
            for r in pending:
                pid, asset, ts, direction, conf, reasoning, pattern, shadow = r
                
                window_ts = ts.replace(tzinfo=None).isoformat()
                trade_key = "p40_{}_{}".format(asset, window_ts)
                if trade_key in _p40_traded_keys:
                    _db = get_db()
                    _db.run("UPDATE p40_predictions SET fired=TRUE, fire_reason='dedup' WHERE id=:i", i=pid)
                    _db.close()
                    continue
                
                try:
                    # Confidence filter
                    if conf < P40_CONFIG["min_confidence_to_fire"]:
                        msg = "conf {} < min {}".format(conf, P40_CONFIG["min_confidence_to_fire"])
                        print("[P4.0] #{} {} {} c{} SKIP — {}".format(pid, asset, direction, conf, msg))
                        _db = get_db()
                        _db.run("UPDATE p40_predictions SET fired=TRUE, fire_reason=:fr WHERE id=:i", fr=msg, i=pid)
                        _db.close()
                        _p40_traded_keys.add(trade_key)
                        continue
                    
                    # Balance check (P4.0's OWN balance)
                    if _p40_state["balance"] - P40_CONFIG["stake_usd"] < _p40_state["floor_balance"]:
                        msg = "balance ${:.2f} - ${:.2f} below floor ${:.2f}".format(
                            _p40_state["balance"], P40_CONFIG["stake_usd"], _p40_state["floor_balance"])
                        print("[P4.0] #{} {} SKIP — {}".format(pid, asset, msg))
                        _db = get_db()
                        _db.run("UPDATE p40_predictions SET fired=TRUE, fire_reason=:fr WHERE id=:i", fr=msg, i=pid)
                        _db.close()
                        continue
                    
                    # Token lookup — use the shared market data, but not P3.0's state
                    entry = _p29cl_token_map.get(asset)
                    if not entry:
                        msg = "no token entry"
                        print("[P4.0] #{} {} SKIP — {}".format(pid, asset, msg))
                        _db = get_db()
                        _db.run("UPDATE p40_predictions SET fired=TRUE, fire_reason=:fr WHERE id=:i", fr=msg, i=pid)
                        _db.close()
                        continue
                    
                    tid = entry.get("up_token") if direction == "UP" else entry.get("down_token")
                    if not tid:
                        msg = "no {} token".format(direction)
                        _db = get_db()
                        _db.run("UPDATE p40_predictions SET fired=TRUE, fire_reason=:fr WHERE id=:i", fr=msg, i=pid)
                        _db.close()
                        continue
                    
                    poly_client = _get_poly_client()
                    if not poly_client:
                        print("[P4.0] #{} no poly client, retry next loop".format(pid))
                        continue
                    
                    best_ask = None
                    try:
                        book = poly_client.get_order_book(str(tid))
                        asks_raw = getattr(book, 'asks', None) or (book.get('asks', []) if isinstance(book, dict) else [])
                        if asks_raw:
                            prices = []
                            for a in asks_raw:
                                ap = float(getattr(a, 'price', None) or a.get('price', 0)) if hasattr(a, 'price') or isinstance(a, dict) else 0
                                if ap > 0:
                                    prices.append(ap)
                            if prices:
                                best_ask = min(prices)
                    except Exception as be:
                        print("[P4.0] #{} {} order book error: {}".format(pid, asset, be))
                    
                    if best_ask is None:
                        msg = "no ask available"
                        _db = get_db()
                        _db.run("UPDATE p40_predictions SET fired=TRUE, fire_reason=:fr WHERE id=:i", fr=msg, i=pid)
                        _db.close()
                        continue
                    
                    stake = P40_CONFIG["stake_usd"]
                    
                    # SHADOW MODE
                    if shadow:
                        print("[P4.0 SHADOW] #{} {} {} c{} WOULD FIRE — ask={:.3f} ${:.2f}".format(
                            pid, asset, direction, conf, best_ask, stake))
                        _db = get_db()
                        _db.run("""
                            UPDATE p40_predictions
                            SET fired=TRUE, fire_reason='shadow_fire',
                                best_ask=:ba, stake=:s, order_status='SHADOW'
                            WHERE id=:i
                        """, ba=best_ask, s=stake, i=pid)
                        _db.close()
                        _p40_traded_keys.add(trade_key)
                        continue
                    
                    # LIVE
                    print("[P4.0 LIVE] #{} {} {} c{} FIRING — ask={:.3f} ${:.2f}".format(
                        pid, asset, direction, conf, best_ask, stake))
                    
                    from py_clob_client_v2 import Side as _Side, MarketOrderArgs as _MOA, OrderType as _OT
                    
                    max_price = round(min(best_ask + 0.01, 0.99), 2)
                    amount = round(stake, 2)
                    args = _MOA(token_id=str(tid), amount=amount, side=_Side.BUY, price=max_price)
                    signed = poly_client.create_market_order(args)
                    resp = poly_client.post_order(signed, _OT.FAK)
                    
                    oid = None; status = "FAILED"; fill_price = None
                    if resp:
                        oid = resp.get("orderID") or resp.get("id")
                        st = (resp.get("status") or "").upper()
                        matched = float(resp.get("sizeMatched") or 0)
                        filled = (st in ("MATCHED", "FILLED") or matched > 0)
                        if filled:
                            fill_price = best_ask
                            status = "FILLED"
                            # Debit P4.0's OWN balance
                            _p40_state["balance"] = round(_p40_state["balance"] - stake, 2)
                            _p40_state["trades_today"] += 1
                            _p40_save_balance()
                        else:
                            status = "UNFILLED"
                    
                    _db = get_db()
                    _db.run("""
                        UPDATE p40_predictions
                        SET fired=TRUE, fire_reason='fired',
                            best_ask=:ba, stake=:s,
                            polymarket_order_id=:o, order_status=:st, fill_price=:fp,
                            balance_after=:bal
                        WHERE id=:i
                    """, ba=best_ask, s=stake, o=oid, st=status, fp=fill_price,
                        bal=_p40_state["balance"], i=pid)
                    _db.close()
                    _p40_traded_keys.add(trade_key)
                    
                    print("[P4.0] #{} {} ORDER {} fill={} bal=${:.2f}".format(
                        pid, asset, status, fill_price, _p40_state["balance"]))
                
                except Exception as e:
                    print("[P4.0] Fire error #{}: {}".format(pid, e))
                    import traceback; traceback.print_exc()
            
            _time.sleep(1)
        
        except Exception as e:
            print("[P4.0] Fire loop error: {}".format(e))
            import traceback; traceback.print_exc()
            _time.sleep(5)


# ═════════════════════════════════════════════════════════════════════════════
# RESOLVE LOOP
# ═════════════════════════════════════════════════════════════════════════════

def _p40_resolve_loop():
    import time as _time
    from datetime import datetime, timezone, timedelta
    
    print("[P4.0] Resolve loop started")
    
    while True:
        try:
            if not P40_CONFIG["enabled"]:
                _time.sleep(60); continue
            
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=16)
            
            _db = get_db()
            rows = _db.run("""
                SELECT id, asset, candle_close_ts, direction, fired, stake, fill_price, shadow_mode, order_status
                FROM p40_predictions
                WHERE outcome IS NULL AND candle_close_ts < :cutoff
                ORDER BY id LIMIT 20
            """, cutoff=cutoff)
            unresolved = list(rows)
            _db.close()
            
            for r in unresolved:
                pid, asset, candle_ts, direction, fired, stake, fill_price, shadow, order_status = r
                
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                age_min = (now - candle_ts).total_seconds() / 60
                
                if age_min < 1:
                    continue
                
                ptb_open = None; next_close = None
                try:
                    candles = _p29cl_candle_prefetch.get(asset, [])
                    if len(candles) >= 2:
                        if 15 <= age_min < 35:
                            ptb_open = candles[-2][3]
                            next_close = candles[-1][3]
                        elif 30 <= age_min < 50:
                            ptb_open = candles[-3][3]
                            next_close = candles[-2][3]
                except Exception as ce:
                    print("[P4.0] Resolve lookup error #{}: {}".format(pid, ce))
                
                if ptb_open is None or next_close is None:
                    continue
                
                actual_dir = "UP" if next_close > ptb_open else "DOWN" if next_close < ptb_open else "FLAT"
                
                if not fired or order_status == "UNFILLED":
                    outcome = "UNFILLED"
                    pnl = 0.0
                elif shadow or order_status == "SHADOW":
                    outcome = "WIN" if direction == actual_dir else "LOSS"
                    pnl = 0.0  # no real money in shadow
                else:
                    won = (direction == actual_dir)
                    if won:
                        shares = (stake or 2.50) / (fill_price or 0.50)
                        pnl = round(shares - stake, 2)
                        # Credit P4.0's OWN balance with full payout
                        _p40_state["balance"] = round(_p40_state["balance"] + shares, 2)
                        _p40_state["wins_today"] += 1
                        if _p40_state["balance"] > _p40_state["peak_balance"]:
                            _p40_state["peak_balance"] = _p40_state["balance"]
                    else:
                        pnl = -(stake or 2.50)
                        _p40_state["losses_today"] += 1
                    outcome = "WIN" if won else "LOSS"
                    _p40_save_balance()
                
                try:
                    _db = get_db()
                    _db.run("""
                        UPDATE p40_predictions
                        SET ptb_open=:po, next_close=:nc, actual_direction=:ad,
                            outcome=:o, pnl=:p, balance_after=:bal, resolved_at=NOW()
                        WHERE id=:i
                    """, po=ptb_open, nc=next_close, ad=actual_dir, o=outcome,
                        p=pnl, bal=_p40_state["balance"], i=pid)
                    _db.close()
                    
                    print("[P4.0] Resolved #{} {} {}: ${:.1f}→${:.1f} actual={} → {} ${:+.2f} bal=${:.2f}".format(
                        pid, asset, direction, ptb_open, next_close, actual_dir, outcome,
                        pnl, _p40_state["balance"]))
                except Exception as ue:
                    print("[P4.0] Resolve update error #{}: {}".format(pid, ue))
            
            _time.sleep(60)
        
        except Exception as e:
            print("[P4.0] Resolve loop error: {}".format(e))
            _time.sleep(60)


# ═════════════════════════════════════════════════════════════════════════════
# FLASK ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/api/p40/status")
def p40_status_endpoint():
    return {
        "enabled": P40_CONFIG["enabled"],
        "shadow_mode": P40_CONFIG["shadow_mode"],
        "model": P40_CONFIG["model"],
        "api_key_set": bool(ANTHROPIC_KEY),
        
        "assets": P40_CONFIG["assets"],
        "min_confidence": P40_CONFIG["min_confidence_to_fire"],
        "stake_usd": P40_CONFIG["stake_usd"],
        "balance": _p40_state["balance"],
        "peak_balance": _p40_state["peak_balance"],
        "trades_today": _p40_state["trades_today"],
        "wins_today": _p40_state["wins_today"],
        "losses_today": _p40_state["losses_today"],
    }

@app.route("/api/p40/enable", methods=["POST"])
def p40_enable_endpoint():
    P40_CONFIG["enabled"] = True
    return {"enabled": True, "shadow_mode": P40_CONFIG["shadow_mode"]}

@app.route("/api/p40/disable", methods=["POST"])
def p40_disable_endpoint():
    P40_CONFIG["enabled"] = False
    return {"enabled": False}

@app.route("/api/p40/go_live", methods=["POST"])
def p40_go_live_endpoint():
    P40_CONFIG["shadow_mode"] = False
    return {"shadow_mode": False, "message": "P4.0 IS NOW LIVE"}

@app.route("/api/p40/go_shadow", methods=["POST"])
def p40_go_shadow_endpoint():
    P40_CONFIG["shadow_mode"] = True
    return {"shadow_mode": True}


@app.route("/app/p40-live")
def p40_live_dashboard():
    try:
        _db = get_db()
        rows = _db.run("""
            SELECT id, asset, candle_close_ts, direction, confidence, reasoning, pattern,
                   fired, best_ask, stake, fill_price, outcome, pnl, shadow_mode,
                   api_elapsed_ms, fire_reason, actual_direction, ptb_open, next_close, balance_after
            FROM p40_predictions
            ORDER BY id DESC LIMIT 200
        """)
        preds = list(rows)
        _db.close()
        
        wins = sum(1 for p in preds if p[11] == "WIN")
        losses = sum(1 for p in preds if p[11] == "LOSS")
        shadow_count = sum(1 for p in preds if p[13])
        live_count = len(preds) - shadow_count
        wr = (wins / (wins + losses) * 100) if (wins + losses) else 0
        live_pnl = sum(p[12] or 0 for p in preds if not p[13] and p[12])
        
        html = """<!DOCTYPE html><html><head><title>P4.0 Live</title>
<meta http-equiv="refresh" content="30">
<style>
body{font-family:monospace;background:#0a0e14;color:#c9d1d9;padding:20px;margin:0;}
h1{color:#58a6ff;margin:0 0 10px 0}
.banner{padding:10px;border-radius:6px;margin:8px 0;font-weight:700}
.live{background:#d29922;color:#000}
.shadow{background:#1f6feb}
.off{background:#6e7681}
.stats{display:flex;gap:8px;margin:10px 0;flex-wrap:wrap}
.stat{padding:8px 14px;background:#161b22;border-radius:6px;border:1px solid #30363d;min-width:90px}
.stat-l{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:1px}
.stat-v{font-size:20px;font-weight:700;margin-top:2px}
table{border-collapse:collapse;width:100%;margin-top:8px;font-size:11px}
th{background:#161b22;padding:5px;text-align:left;border:1px solid #30363d;position:sticky;top:0}
td{padding:4px 6px;border:1px solid #30363d}
.up{color:#3fb950;font-weight:700}
.down{color:#f85149;font-weight:700}
.win{color:#3fb950;font-weight:700}
.loss{color:#f85149;font-weight:700}
.pend{color:#d29922}
.gh{color:#8b949e;font-style:italic}
.btn{padding:6px 14px;border-radius:4px;cursor:pointer;border:none;font-family:inherit;font-weight:600;margin-right:6px}
.bg{background:#238636;color:#fff}
.bb{background:#1f6feb;color:#fff}
.br{background:#da3633;color:#fff}
</style></head><body>
<h1>P4.0 Claude Predictions</h1>"""
        
        if P40_CONFIG["enabled"]:
            if P40_CONFIG["shadow_mode"]:
                html += '<div class="banner shadow">SHADOW MODE — logging only, NO orders</div>'
            else:
                html += '<div class="banner live">LIVE — real orders on Polymarket</div>'
        else:
            html += '<div class="banner off">DISABLED</div>'
        
        html += """<div style="margin:10px 0">
<button class="btn bg" onclick="fetch('/api/p40/enable',{method:'POST'}).then(()=>location.reload())">ENABLE</button>
<button class="btn br" onclick="fetch('/api/p40/disable',{method:'POST'}).then(()=>location.reload())">DISABLE</button>
<button class="btn bb" onclick="fetch('/api/p40/go_shadow',{method:'POST'}).then(()=>location.reload())">SHADOW</button>
<button class="btn bg" onclick="if(confirm('Go LIVE? Real orders.')){fetch('/api/p40/go_live',{method:'POST'}).then(()=>location.reload())}">GO LIVE</button>
</div>
<div class="stats">"""
        html += '<div class="stat"><div class="stat-l">P4.0 Balance</div><div class="stat-v" style="color:#58a6ff">' + "${:.2f}".format(_p40_state["balance"]) + '</div></div>'
        html += '<div class="stat"><div class="stat-l">Peak</div><div class="stat-v">' + "${:.2f}".format(_p40_state["peak_balance"]) + '</div></div>'
        html += '<div class="stat"><div class="stat-l">Total</div><div class="stat-v">' + str(len(preds)) + '</div></div>'
        html += '<div class="stat"><div class="stat-l">Live</div><div class="stat-v">' + str(live_count) + '</div></div>'
        html += '<div class="stat"><div class="stat-l">Shadow</div><div class="stat-v">' + str(shadow_count) + '</div></div>'
        html += '<div class="stat"><div class="stat-l">WR</div><div class="stat-v">' + "{:.1f}%".format(wr) + '</div></div>'
        html += '<div class="stat"><div class="stat-l">W/L</div><div class="stat-v">' + "{}/{}".format(wins, losses) + '</div></div>'
        pc = "#3fb950" if live_pnl >= 0 else "#f85149"
        html += '<div class="stat"><div class="stat-l">Live P&L</div><div class="stat-v" style="color:' + pc + '">' + "${:+.2f}".format(live_pnl) + '</div></div>'
        html += '</div><table><tr><th>#</th><th>Time</th><th>Asset</th><th>Pred</th><th>C</th><th>Pattern</th><th>Reasoning</th><th>Ask</th><th>$</th><th>Fill</th><th>Open</th><th>Close</th><th>Actual</th><th>Outcome</th><th>P&L</th><th>Bal</th><th>Mode</th><th>API</th><th>Reason</th></tr>'
        
        for p in preds:
            pid, asset, ts, direction, conf, reason, pattern, fired, ba, stake, fill, outcome, pnl, shadow, ms, fr, actual, po, nc, bal = p
            dc = "up" if direction == "UP" else "down"
            oc = "win" if outcome == "WIN" else "loss" if outcome == "LOSS" else "pend"
            ac = "up" if actual == "UP" else "down" if actual == "DOWN" else ""
            mode = '<span class="gh">SHADOW</span>' if shadow else "LIVE"
            ts_s = ts.strftime("%m-%d %H:%M") if ts else ""
            ba_s = "{:.3f}".format(ba) if ba else "-"
            st_s = "${:.2f}".format(stake) if stake else "-"
            fp_s = "{:.3f}".format(fill) if fill else "-"
            pnl_s = "${:+.2f}".format(pnl) if pnl else "-"
            po_s = "{:.1f}".format(po) if po else "-"
            nc_s = "{:.1f}".format(nc) if nc else "-"
            bal_s = "${:.2f}".format(bal) if bal else "-"
            
            html += '<tr>'
            html += '<td>{}</td><td>{}</td><td>{}</td>'.format(pid, ts_s, asset)
            html += '<td class="{}">{}</td><td>{}</td>'.format(dc, direction, conf)
            html += '<td>{}</td><td>{}</td>'.format((pattern or "")[:16], (reason or "")[:50])
            html += '<td>{}</td><td>{}</td><td>{}</td>'.format(ba_s, st_s, fp_s)
            html += '<td>{}</td><td>{}</td>'.format(po_s, nc_s)
            html += '<td class="{}">{}</td>'.format(ac, actual or "-")
            html += '<td class="{}">{}</td>'.format(oc, outcome or "PEND")
            html += '<td>{}</td><td>{}</td>'.format(pnl_s, bal_s)
            html += '<td>{}</td><td>{}</td><td>{}</td>'.format(mode, ms or "", (fr or "")[:30])
            html += '</tr>'
        
        html += '</table></body></html>'
        return html
    
    except Exception as e:
        import traceback
        return "<pre>Error: {}\n{}</pre>".format(e, traceback.format_exc()), 500


# ═════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═════════════════════════════════════════════════════════════════════════════

_p40_create_table()

import threading as _p40_threading
if ANTHROPIC_KEY:
    _p40_threading.Thread(target=_p40_predict_loop, daemon=True, name="P4.0-Predict").start()
    _p40_threading.Thread(target=_p40_fire_loop, daemon=True, name="P4.0-Fire").start()
    _p40_threading.Thread(target=_p40_resolve_loop, daemon=True, name="P4.0-Resolve").start()
    print("[P4.0] All threads started (enabled={}, shadow={}, balance=${:.2f})".format(
        P40_CONFIG["enabled"], P40_CONFIG["shadow_mode"], _p40_state["balance"]))
else:
    print("[P4.0] ANTHROPIC_API_KEY not set — threads NOT started")
