// src/index.js — Main Bot Engine
const { LunoClient } = require('./luno');
const { TelegramBot } = require('./telegram');
const { StateEngine } = require('./state');
const config = require('./config');

// === INIT ===
const luno = new LunoClient(config.LUNO_API_KEY, config.LUNO_API_SECRET);
const telegram = new TelegramBot(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID);
const state = new StateEngine();

let isRunning = true;
let lastDailyReset = new Date().getDate();

// === HELPER FUNCTIONS ===

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// === CORE: FETCH ORDER BOOK & CALCULATE SPREAD ===

async function getSpread(pair) {
  try {
    const book = await luno.getOrderBook(pair);
    if (!book.asks || !book.bids || book.asks.length === 0 || book.bids.length === 0) {
      return null;
    }
    const bestBid = parseFloat(book.bids[0].price);
    const bestAsk = parseFloat(book.asks[0].price);
    const midPrice = (bestBid + bestAsk) / 2;
    const spreadNgn = bestAsk - bestBid;
    const spreadPct = (spreadNgn / midPrice) * 100;

    return {
      bestBid,
      bestAsk,
      midPrice,
      spreadNgn,
      spreadPct,
      bidVolume: parseFloat(book.bids[0].volume),
      askVolume: parseFloat(book.asks[0].volume),
    };
  } catch (err) {
    log(`Error fetching ${pair} order book: ${err.message}`);
    return null;
  }
}

// === CORE: PAIR SELECTION ===

async function selectBestPair() {
  const usdtSpread = await getSpread(config.PRIMARY_PAIR);
  const btcSpread = await getSpread(config.SECONDARY_PAIR);

  log(`USDT spread: ${usdtSpread ? `₦${usdtSpread.spreadNgn.toFixed(4)} (${usdtSpread.spreadPct.toFixed(4)}%)` : 'unavailable'}`);
  log(`BTC spread:  ${btcSpread ? `₦${btcSpread.spreadNgn.toLocaleString()} (${btcSpread.spreadPct.toFixed(4)}%)` : 'unavailable'}`);

  // NEVER switch away from USDT if we're holding USDT that needs selling
  if (state.usdtBalance > 10) {
    return { pair: config.PRIMARY_PAIR, spread: usdtSpread || { bestBid: 1385, bestAsk: 1387, midPrice: 1386, spreadNgn: 2, spreadPct: 0.14, bidVolume: 0, askVolume: 0 } };
  }

  // NEVER switch away from BTC if we're holding BTC that needs selling
  if (state.btcBalance > 0.0001) {
    return { pair: config.SECONDARY_PAIR, spread: btcSpread || { bestBid: 106000000, bestAsk: 107000000, midPrice: 106500000, spreadNgn: 1000000, spreadPct: 0.94, bidVolume: 0, askVolume: 0 } };
  }

  // If no inventory, pick best spread
  if (usdtSpread) {
    return { pair: config.PRIMARY_PAIR, spread: usdtSpread };
  }

  if (btcSpread && btcSpread.spreadPct >= config.BTC_MIN_SPREAD_PCT) {
    return { pair: config.SECONDARY_PAIR, spread: btcSpread };
  }

  // Default to USDT
  return { pair: config.PRIMARY_PAIR, spread: usdtSpread || { bestBid: 1385, bestAsk: 1387, midPrice: 1386, spreadNgn: 2, spreadPct: 0.14, bidVolume: 0, askVolume: 0 } };
}

// === CORE: UPDATE BALANCES ===

async function updateBalances() {
  try {
    const res = await luno.getBalances();
    
    // Debug: log raw response structure on first call
    if (state.dailyRotations === 0 && state.ngnBalance === 0) {
      log(`DEBUG balance response keys: ${JSON.stringify(Object.keys(res))}`);
      if (res.balance && res.balance.length > 0) {
        log(`DEBUG first account: ${JSON.stringify(res.balance[0])}`);
      }
    }
    
    const accounts = res.balance || [];
    for (const acc of accounts) {
      // Luno API may use 'asset' or 'currency' depending on version
      const currency = acc.asset || acc.currency || '';
      const bal = parseFloat(acc.balance || 0);
      const reserved = parseFloat(acc.reserved || 0);
      const available = bal - reserved;
      
      if (currency === 'NGN') state.ngnBalance = available;
      if (currency === 'USDT') state.usdtBalance = available;
      if (currency === 'XBT') state.btcBalance = available;
    }
    log(`Balances — NGN: ₦${state.ngnBalance.toFixed(2)} | USDT: ${state.usdtBalance.toFixed(4)} | BTC: ${state.btcBalance.toFixed(8)}`);
  } catch (err) {
    log(`Error fetching balances: ${err.message}`);
  }
}

// === CORE: CHECK IF OUR ORDERS WERE FILLED ===

async function checkOrderFills() {
  let buyFilled = false;
  let sellFilled = false;

  if (state.buyOrderId) {
    try {
      const order = await luno.getOrder(state.buyOrderId);
      if (order.state === 'COMPLETE') {
        buyFilled = true;
        state.lastBuyFillPrice = state.buyOrderPrice; // track for smart sell
        log(`✅ BUY FILLED @ ₦${state.buyOrderPrice}`);
        await telegram.fill('BUY', state.activePair, state.buyOrderPrice, state.buyOrderVolume);
        state.buyOrderId = null;
      } else if (order.state === 'CANCELLED' || order.state === 'EXPIRED') {
        state.buyOrderId = null;
      }
    } catch (err) {
      // Order might not exist anymore
      state.buyOrderId = null;
    }
  }

  if (state.sellOrderId) {
    try {
      const order = await luno.getOrder(state.sellOrderId);
      if (order.state === 'COMPLETE') {
        sellFilled = true;
        log(`✅ SELL FILLED @ ₦${state.sellOrderPrice}`);
        await telegram.fill('SELL', state.activePair, state.sellOrderPrice, state.sellOrderVolume);
        state.sellOrderId = null;
      } else if (order.state === 'CANCELLED' || order.state === 'EXPIRED') {
        state.sellOrderId = null;
      }
    } catch (err) {
      state.sellOrderId = null;
    }
  }

  // If both sides completed = full rotation
  if (buyFilled && sellFilled) {
    const profit = (state.sellOrderPrice - state.buyOrderPrice) * state.buyOrderVolume;
    state.recordRotation(profit);
    log(`🔄 ROTATION COMPLETE — Profit: ₦${profit.toFixed(2)} | Daily P&L: ₦${state.dailyPnl.toFixed(2)} | Rotations: ${state.dailyRotations}`);
  }

  return { buyFilled, sellFilled };
}

// === CORE: CANCEL ALL OPEN ORDERS ===

async function cancelAllOrders() {
  if (state.buyOrderId) {
    try {
      await luno.cancelOrder(state.buyOrderId);
      log(`Cancelled buy order ${state.buyOrderId}`);
    } catch (err) { /* already cancelled or filled */ }
    state.buyOrderId = null;
  }
  if (state.sellOrderId) {
    try {
      await luno.cancelOrder(state.sellOrderId);
      log(`Cancelled sell order ${state.sellOrderId}`);
    } catch (err) { /* already cancelled or filled */ }
    state.sellOrderId = null;
  }
}

// === CORE: PLACE ORDERS ===

async function placeOrders(spread) {
  const pair = state.activePair;
  const isUsdt = pair === 'USDTNGN';
  const tick = isUsdt ? config.PRICE_TICK_USDT : config.PRICE_TICK_BTC;

  // === ALWAYS HAVE ORDERS ON THE BOOK ===

  // Price: one tick better than best bid/ask
  let buyPrice = spread.bestBid + tick;
  let sellPrice = spread.bestAsk - tick;

  // === SMART SELL: if we bought cheap, undercut everyone ===
  // If our last buy filled below the current best bid, we can sell at
  // just above our buy price — guaranteed profit, impossible to undercut
  if (state.lastBuyFillPrice && state.lastBuyFillPrice > 0 && isUsdt) {
    const minProfitSell = Math.ceil((state.lastBuyFillPrice + 0.10) * 100) / 100;
    if (minProfitSell < sellPrice && minProfitSell < spread.bestAsk) {
      sellPrice = minProfitSell;
      log(`💡 Smart sell: cost ₦${state.lastBuyFillPrice}, selling @ ₦${sellPrice}`);
    }
  }

  // Never cross mid
  if (buyPrice >= spread.midPrice) buyPrice = spread.midPrice - tick;
  if (sellPrice <= spread.midPrice) sellPrice = spread.midPrice + tick;

  // If buy and sell would cross each other, spread evenly from mid
  if (sellPrice <= buyPrice) {
    buyPrice = spread.midPrice - tick;
    sellPrice = spread.midPrice + tick;
  }

  // === CAUTION MODE ===
  if (state.state === 'CAUTION') {
    buyPrice -= isUsdt ? 1.0 : spread.midPrice * 0.002;
    sellPrice += isUsdt ? 1.0 : spread.midPrice * 0.002;
  }

  // === ROUND ===
  if (isUsdt) {
    buyPrice = Math.floor(buyPrice * 100) / 100;
    sellPrice = Math.ceil(sellPrice * 100) / 100;
  } else {
    buyPrice = Math.floor(buyPrice);
    sellPrice = Math.ceil(sellPrice);
  }

  // === ORDER SIZE — scale with spread ===
  // Tight spread = smaller orders (less risk)
  // Wide spread = bigger orders (more profit per fill)
  const spreadRatio = Math.min(spread.spreadNgn / 2.0, 1.0); // 0 to 1 scale
  const baseCap = 0.48;
  const dynamicCap = Math.max(0.15, baseCap * spreadRatio); // minimum 15% even at tight spreads

  let buyVolume, sellVolume;
  if (isUsdt) {
    buyVolume = Math.floor((state.ngnBalance * dynamicCap / buyPrice) * 100) / 100;
    sellVolume = Math.floor(state.usdtBalance * dynamicCap * 100) / 100;
    buyVolume = Math.min(Math.max(buyVolume, 0), config.MAX_ORDER_USDT);
    sellVolume = Math.min(Math.max(sellVolume, 0), config.MAX_ORDER_USDT);
  } else {
    buyVolume = Math.floor((state.ngnBalance * dynamicCap / buyPrice) * 1000000) / 1000000;
    sellVolume = Math.floor(state.btcBalance * dynamicCap * 1000000) / 1000000;
    buyVolume = Math.min(Math.max(buyVolume, 0), config.MAX_ORDER_BTC);
    sellVolume = Math.min(Math.max(sellVolume, 0), config.MAX_ORDER_BTC);
  }
  const minVol = isUsdt ? config.MIN_ORDER_USDT : config.MIN_ORDER_BTC;

  // === NO INVENTORY BLOCK — always place both sides ===
  // Even at 90% crypto, still place a sell to rebalance
  // Even at 90% NGN, still place a buy
  // Just skip the side that has zero balance

  // === PLACE BUY ===
  if (!state.buyOrderId && buyVolume >= minVol) {
    try {
      const res = await luno.createOrder(pair, 'BID', buyVolume, buyPrice, true);
      state.buyOrderId = res.order_id;
      state.buyOrderPrice = buyPrice;
      state.buyOrderVolume = buyVolume;
      state._buyFails = 0;
      log('📗 BUY ' + buyVolume + ' @ ₦' + buyPrice);
    } catch (err) {
      if (err.message.includes('ErrOrderCanceled')) {
        state._buyFails = (state._buyFails || 0) + 1;
        if (state._buyFails >= 5) {
          state.buyOrderId = 'COOLDOWN';
          setTimeout(() => { state.buyOrderId = null; }, 5000);
          state._buyFails = 0;
        }
      } else {
        state.consecutiveErrors++;
      }
    }
  }

  // === PLACE SELL ===
  if (!state.sellOrderId && sellVolume >= minVol) {
    try {
      const res = await luno.createOrder(pair, 'ASK', sellVolume, sellPrice, true);
      state.sellOrderId = res.order_id;
      state.sellOrderPrice = sellPrice;
      state.sellOrderVolume = sellVolume;
      state._sellFails = 0;
      log('📕 SELL ' + sellVolume + ' @ ₦' + sellPrice);
    } catch (err) {
      if (err.message.includes('ErrOrderCanceled')) {
        state._sellFails = (state._sellFails || 0) + 1;
        if (state._sellFails >= 5) {
          state.sellOrderId = 'COOLDOWN';
          setTimeout(() => { state.sellOrderId = null; }, 5000);
          state._sellFails = 0;
        }
      } else {
        state.consecutiveErrors++;
      }
    }
  }
}

// === CORE: CHECK IF ORDERS NEED REPLACING (stay on top) ===

async function refreshOrders(spread) {
  const pair = state.activePair;
  const isUsdt = pair === 'USDTNGN';
  const tick = isUsdt ? config.PRICE_TICK_USDT : config.PRICE_TICK_BTC;

  const idealBuy = spread.bestBid + tick;
  const idealSell = spread.bestAsk - tick;
  const minSpread = isUsdt ? config.USDT_MIN_SPREAD_NGN : spread.midPrice * config.BTC_MIN_SPREAD_PCT / 100;

  // Don't refresh if spread is too tight — both sides stay cancelled
  if (spread.spreadNgn < minSpread) return;

  // Safety: our buy and sell must maintain minimum distance
  if (idealSell - idealBuy < minSpread) return;

  // BUY SIDE: aggressive — if we're not top bid, refresh
  if (state.buyOrderId && state.buyOrderPrice < idealBuy - 0.005) {
    try {
      await luno.cancelOrder(state.buyOrderId);
      state.buyOrderId = null;
    } catch (err) { state.buyOrderId = null; }
  }

  // SELL SIDE: aggressive — if we're not top ask, refresh
  // But only if our new sell price would still be ABOVE our buy price by minSpread
  if (state.sellOrderId && state.sellOrderPrice > idealSell + 0.005) {
    const newSellWouldBeProfit = idealSell > (state.buyOrderPrice || idealBuy - tick) + minSpread * 0.5;
    if (newSellWouldBeProfit) {
      try {
        await luno.cancelOrder(state.sellOrderId);
        state.sellOrderId = null;
      } catch (err) { state.sellOrderId = null; }
    }
  }
}

// === MAIN LOOP ===

async function mainLoop() {
  await telegram.startup();
  log('🚀 Bot starting...');
  log(`Primary pair: ${config.PRIMARY_PAIR} | Secondary: ${config.SECONDARY_PAIR}`);

  // Cancel any leftover orders from previous run
  log('Cancelling any leftover orders from previous run...');
  try {
    const usdtOrders = await luno.listOrders('USDTNGN', 'PENDING');
    if (usdtOrders.orders) {
      for (const order of usdtOrders.orders) {
        await luno.cancelOrder(order.order_id);
        log(`Cancelled leftover USDT order: ${order.order_id}`);
      }
    }
    const btcOrders = await luno.listOrders('XBTNGN', 'PENDING');
    if (btcOrders.orders) {
      for (const order of btcOrders.orders) {
        await luno.cancelOrder(order.order_id);
        log(`Cancelled leftover BTC order: ${order.order_id}`);
      }
    }
  } catch (err) {
    log(`Warning: Could not cancel leftover orders: ${err.message}`);
  }

  // Wait for orders to settle
  await sleep(2000);

  // Initial balance fetch
  await updateBalances();
  state.startingCapitalNgn = state.ngnBalance + (state.usdtBalance * 1385) + (state.btcBalance * 106000000);
  log(`Starting capital estimate: ₦${state.startingCapitalNgn.toFixed(0)}`);

  while (isRunning) {
    try {
      // === DAILY RESET ===
      const today = new Date().getDate();
      if (today !== lastDailyReset) {
        await telegram.dailySummary({
          rotations: state.dailyRotations,
          totalPnl: state.dailyPnl,
          avgPnl: state.dailyRotations > 0 ? state.dailyPnl / state.dailyRotations : 0,
          activePair: state.activePair,
          inventoryRatio: `${((1 - state.getInventoryRatio()) * 100).toFixed(0)}/${(state.getInventoryRatio() * 100).toFixed(0)} NGN/Crypto`,
        });
        await updateBalances();
        state.resetDaily(state.ngnBalance + (state.usdtBalance * 1385));
        lastDailyReset = today;
      }

      // === NEVER SLEEP — always active ===

      // === CHECK DAILY LOSS LIMIT ===
      if (state.isDailyLossLimitHit()) {
        await cancelAllOrders();
        await telegram.shutdown(`Daily loss limit hit: ₦${state.dailyPnl.toFixed(2)}`);
        log('⛔ Daily loss limit hit. Stopping.');
        isRunning = false;
        break;
      }

      // === CHECK CONSECUTIVE ERRORS ===
      if (state.consecutiveErrors >= config.MAX_CONSECUTIVE_ERRORS) {
        await cancelAllOrders();
        await telegram.error(`${state.consecutiveErrors} consecutive errors. Pausing 60s.`);
        state.consecutiveErrors = 0;
        await sleep(60000);
        continue;
      }

      // === PAIR SELECTION (every 60s) ===
      if (state.shouldCheckPairSwitch()) {
        state.markPairChecked();
        const best = await selectBestPair();

        if (best.pair !== state.activePair) {
          const oldPair = state.activePair;
          await cancelAllOrders();
          state.activePair = best.pair;
          state.priceHistory = [];
          log(`🔄 Switched to ${best.pair}`);
        }
      }

      // === FETCH CURRENT SPREAD ===
      const spread = await getSpread(state.activePair);
      if (!spread) {
        state.consecutiveErrors++;
        await sleep(config.LOOP_INTERVAL_MS);
        continue;
      }
      state.consecutiveErrors = 0; // reset on success

      // === UPDATE PRICE HISTORY & STATE ===
      state.addPrice(spread.midPrice);
      const stateResult = state.updateState();

      if (stateResult.changed) {
        await telegram.stateChange(
          state.state === stateResult.state ? 'CALM' : state.state,
          stateResult.state,
          `Rate change: ${stateResult.rateChange.toFixed(4)}%`
        );
      }

      // === DANGER MODE: PULL EVERYTHING ===
      if (state.state === 'DANGER') {
        await cancelAllOrders();
        log(`🔴 DANGER — Rate change: ${stateResult.rateChange.toFixed(4)}%. Orders cancelled. Waiting...`);
        await sleep(config.LOOP_INTERVAL_MS * 3);
        continue;
      }

      // === CHECK FOR FILLS ===
      await checkOrderFills();

      // === UPDATE BALANCES (every 5 seconds at 1s loop speed) ===
      if (!state._balanceCounter) state._balanceCounter = 0;
      state._balanceCounter++;
      if (state._balanceCounter >= 5 || !state.buyOrderId || !state.sellOrderId) {
        await updateBalances();
        state._balanceCounter = 0;

        // === AUTO-REBALANCE: when heavy on one side, use BIGGER orders on that side ===
        const invRatio = state.getInventoryRatio();
        
        if (invRatio > 0.70) {
          // Heavy on USDT — make sell orders BIGGER and more aggressive
          // Cancel current sell and place a larger one at top of ask
          if (state.sellOrderId && state.sellOrderId !== 'COOLDOWN') {
            try { await luno.cancelOrder(state.sellOrderId); } catch(e) {}
            state.sellOrderId = null;
          }
          // Use 80% of USDT balance for the sell (not 48%)
          const bigSellVol = Math.floor(state.usdtBalance * 0.80 * 100) / 100;
          const sellPrice = Math.ceil((spread.bestAsk - 0.01) * 100) / 100;
          if (bigSellVol >= 5) {
            try {
              const res = await luno.createOrder('USDTNGN', 'ASK', bigSellVol, sellPrice, true);
              state.sellOrderId = res.order_id;
              state.sellOrderPrice = sellPrice;
              state.sellOrderVolume = bigSellVol;
              log(`⚡ BIG SELL ${bigSellVol} @ ₦${sellPrice} (inv ${(invRatio*100).toFixed(0)}%)`);
            } catch(err) {}
          }
        } else if (invRatio < 0.30) {
          // Heavy on NGN — make buy orders BIGGER and more aggressive
          if (state.buyOrderId && state.buyOrderId !== 'COOLDOWN') {
            try { await luno.cancelOrder(state.buyOrderId); } catch(e) {}
            state.buyOrderId = null;
          }
          const bigBuyVol = Math.floor((state.ngnBalance * 0.80 / (spread.bestBid + 0.01)) * 100) / 100;
          const buyPrice = Math.floor((spread.bestBid + 0.01) * 100) / 100;
          if (bigBuyVol >= 5) {
            try {
              const res = await luno.createOrder('USDTNGN', 'BID', bigBuyVol, buyPrice, true);
              state.buyOrderId = res.order_id;
              state.buyOrderPrice = buyPrice;
              state.buyOrderVolume = bigBuyVol;
              log(`⚡ BIG BUY ${bigBuyVol} @ ₦${buyPrice} (inv ${(invRatio*100).toFixed(0)}%)`);
            } catch(err) {}
          }
        }
      }

      // === REFRESH ORDERS TO STAY ON TOP ===
      await refreshOrders(spread);

      // === PLACE NEW ORDERS IF NEEDED ===
      await placeOrders(spread);

      // === LOG STATUS (every 10 seconds to reduce noise) ===
      if (Date.now() % 10000 < config.LOOP_INTERVAL_MS) {
        log(
          `${state.state} | ${state.activePair} | ` +
          `Spread: ₦${spread.spreadNgn.toFixed(2)} | ` +
          `Inv: ${(state.getInventoryRatio() * 100).toFixed(0)}% | ` +
          `P&L: ₦${state.dailyPnl.toFixed(2)} | ` +
          `Rot: ${state.dailyRotations} | ` +
          `Buy: ${state.buyOrderId ? '✅' : '❌'} Sell: ${state.sellOrderId ? '✅' : '❌'}`
        );
      }

    } catch (err) {
      log(`❌ Loop error: ${err.message}`);
      state.consecutiveErrors++;
      await telegram.error(err.message);
    }

    await sleep(config.LOOP_INTERVAL_MS);
  }
}

// === GRACEFUL SHUTDOWN ===

async function shutdown(signal) {
  log(`\n${signal} received. Shutting down gracefully...`);
  isRunning = false;
  await cancelAllOrders();
  await telegram.shutdown(`Bot stopped (${signal})`);
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// === START ===

if (!config.LUNO_API_KEY || !config.LUNO_API_SECRET) {
  console.error('❌ Missing LUNO_API_KEY_ID or LUNO_API_SECRET environment variables.');
  console.error('Set them in your Railway dashboard or .env file.');
  process.exit(1);
}

// Keep-alive HTTP server so Railway doesn't kill the container
const http = require('http');
const PORT = process.env.PORT || 3000;
http.createServer((req, res) => {
  res.writeHead(200);
  res.end(JSON.stringify({
    status: 'running',
    pair: state.activePair,
    state: state.state,
    pnl: state.dailyPnl,
    rotations: state.dailyRotations,
  }));
}).listen(PORT, () => {
  console.log(`Health check server on port ${PORT}`);
});

mainLoop().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
