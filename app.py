

heroic-quietude

production



Agent








web
Deployments
Variables
Metrics
Settings
web-production-df911d.up.railway.app
3.13.13python@3.13.13
us-west2
1 Replica




History



















web
/
2388552b
Active

Apr 20, 2026, 11:41 AM GMT+1
web-production-df911d.up.railway.app
Details
Build Logs
Deploy Logs
HTTP Logs
Network Flow Logs
Filter and search logs

You reached the start of the range
Apr 20, 2026, 11:41 AM
Starting Container
Football outcomes: 18 pending picks, none ready yet (all < 2hrs past kickoff)
DB initialized OK
Limitless Bot v4 — 5 threads running (signals DB connected)
 * Serving Flask app 'app'
 * Debug mode: off
WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
 * Running on all addresses (0.0.0.0)
 * Running on http://127.0.0.1:8080
 * Running on http://10.190.159.151:8080
Press CTRL+C to quit
100.64.0.2 - - [20/Apr/2026 10:43:03] "GET /bot3/set?balance=20&floor=8 HTTP/1.1" 200 -
100.64.0.3 - - [20/Apr/2026 10:43:07] "GET /bot2/set?balance=50&floor=20 HTTP/1.1" 200 -
[2026-04-20 10:43:09,436] ERROR in app: Exception on /trading/set [GET]
Traceback (most recent call last):
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 1511, in wsgi_app
    response = self.full_dispatch_request()
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 919, in full_dispatch_request
    rv = self.handle_user_exception(e)
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 917, in full_dispatch_request
    rv = self.dispatch_request()
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 902, in dispatch_request
    return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)  # type: ignore[no-any-return]
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "/app/app.py", line 4848, in trading_set
    "current_stake": _calc_bot_stake(_trading_state),
                     ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^
  File "/app/app.py", line 92, in _calc_bot_stake
    balance = state["balance"]
              ~~~~~^^^^^^^^^^^
KeyError: 'balance'
100.64.0.4 - - [20/Apr/2026 10:43:09] "GET /trading/set?balance=13&floor=5 HTTP/1.1" 500 -
BTC trend error: Too Many Requests. Rate limited. Try after a while.
Scan: 25 markets total | BTC=None | Lagos=11:43
KeyError: 'balance'
100.64.0.5 - - [20/Apr/2026 10:43:15] "GET /trading/set?balance=13&floor=5 HTTP/1.1" 500 -
[2026-04-20 10:43:15,203] ERROR in app: Exception on /trading/set [GET]
Traceback (most recent call last):
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 1511, in wsgi_app
    response = self.full_dispatch_request()
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 919, in full_dispatch_request
    rv = self.handle_user_exception(e)
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 917, in full_dispatch_request
    rv = self.dispatch_request()
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 902, in dispatch_request
    return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)  # type: ignore[no-any-return]
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "/app/app.py", line 4848, in trading_set
    "current_stake": _calc_bot_stake(_trading_state),
                     ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^
  File "/app/app.py", line 92, in _calc_bot_stake
    balance = state["balance"]
              ~~~~~^^^^^^^^^^^
Scan done: 0/25 qualified
Bot2 scan: 0 qualified (skip: parse=5 dup=8 odds=2 score=3 bot1=7)
              ~~~~~^^^^^^^^^^^
KeyError: 'balance'
100.64.0.6 - - [20/Apr/2026 10:43:18] "GET /trading/set?balance=13&floor=5 HTTP/1.1" 500 -
[2026-04-20 10:43:18,319] ERROR in app: Exception on /trading/set [GET]
Traceback (most recent call last):
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 1511, in wsgi_app
    response = self.full_dispatch_request()
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 919, in full_dispatch_request
    rv = self.handle_user_exception(e)
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 917, in full_dispatch_request
    rv = self.dispatch_request()
  File "/app/.venv/lib/python3.13/site-packages/flask/app.py", line 902, in dispatch_request
    return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)  # type: ignore[no-any-return]
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "/app/app.py", line 4848, in trading_set
    "current_stake": _calc_bot_stake(_trading_state),
                     ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^
  File "/app/app.py", line 92, in _calc_bot_stake
    balance = state["balance"]
Paper34: 0 signals (indicators: 11ok/0fail, markets: 25)
100.64.0.5 - - [20/Apr/2026 10:43:30] "GET /bot3/set?balance=20&floor=8 HTTP/1.1" 200 -
Signals DB: 20 pairs updated — ADA=SELL, BNB=SELL, BTC=BUY, ETH=BUY, EURCAD=BUY, EURCHF=SELL, EURJPY=BUY, EURNZD=BUY, GBPAUD=SELL, GBPCAD=SELL, GBPJPY=BUY, GBPNZD=BUY, GBP=BUY, HYPE=SELL, MNT=BUY, SOL=BUY, CAD=BUY, XAG=SELL, XAU=SELL, ZEC=BUY


