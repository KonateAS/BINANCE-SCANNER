"""Public Binance USDⓈ-M REST and WebSocket market data."""
import asyncio
import json
import logging
import random
from collections import defaultdict, deque
from datetime import datetime, timezone
import websockets
from .binance_rest import BinanceRestClient
from .config import HISTORY_CANDLES, REST_URL, TIMEFRAMES, WS_GROUP_SIZE, WS_SILENCE_TIMEOUT_SECONDS, WS_URL

log = logging.getLogger(__name__)

class MarketData:
    def __init__(self):
        self.bars = defaultdict(lambda: defaultdict(lambda: deque(maxlen=300)))
        # Revisions let the scanner skip unchanged series and distinguish a
        # finalized-bar change from an in-progress kline update.
        self.bar_revisions = defaultdict(int)
        self.closed_revisions = defaultdict(int)
        self.prices, self.changes_24h, self.symbols = {}, {}, []
        self.updated_at, self.errors = None, []
        self.ws_groups = {}
        self.kline_subscribers = defaultdict(set)
        self._resync_sem = asyncio.Semaphore(4)
        self._ws_connect_lock = asyncio.Lock()
        self._last_ws_connect = 0.0
        self._rest = BinanceRestClient(REST_URL, timeout=20)

    async def close(self): await self._rest.aclose()

    async def rest_status(self): return await self._rest.limiter.status()

    async def acknowledge_rest_suspension(self): await self._rest.limiter.operator_acknowledge()

    async def resume_rest(self): await self._rest.limiter.operator_resume()

    def _mark_bar_revision(self, symbol, timeframe, closed=False):
        key = (symbol, timeframe)
        self.bar_revisions[key] += 1
        if closed:
            self.closed_revisions[key] += 1

    def _publish_kline(self, symbol, timeframe, full=False):
        key = (symbol, timeframe)
        subscribers = self.kline_subscribers.get(key)
        if not subscribers:
            return
        series = self.bars[symbol][timeframe]
        message = {"type": "history", "data": list(series)} if full else {
            "type": "bar", "data": dict(series[-1])
        }
        for queue in tuple(subscribers):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                # Replace any dropped updates with a complete, current series.
                queue.put_nowait({"type": "history", "data": list(series)})
            else:
                queue.put_nowait(message)

    async def discover(self):
        r = await self._rest.get("/fapi/v1/exchangeInfo"); r.raise_for_status()
        self.symbols = sorted(s["symbol"] for s in r.json()["symbols"] if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT")
        return self.symbols

    async def history(self):
        sem = asyncio.Semaphore(8)
        async def one(symbol, interval):
            async with sem:
                try:
                    r = await self._rest.get("/fapi/v1/klines", params={"symbol":symbol,"interval":interval,"limit":HISTORY_CANDLES}); r.raise_for_status()
                    now = int(datetime.now(timezone.utc).timestamp()*1000)
                    rows = r.json()
                    loaded = [{"t":x[0],"o":float(x[1]),"h":float(x[2]),"l":float(x[3]),"c":float(x[4]),"v":float(x[5]),"closed":x[6]<now} for x in rows]
                    self.bars[symbol][interval].extend(loaded)
                    if loaded:
                        self._mark_bar_revision(symbol, interval, closed=any(bar["closed"] for bar in loaded))
                except Exception as exc:
                    self.errors.append(f"{symbol}/{interval}: {exc}"); log.warning("History failed %s %s: %s",symbol,interval,exc)
        # Pace each batch to keep the full-universe bootstrap within Binance request limits.
        for start in range(0,len(self.symbols),30):
            batch=self.symbols[start:start+30]
            await asyncio.gather(*(one(s,tf) for s in batch for tf in TIMEFRAMES)); await asyncio.sleep(1)

    async def refresh_tickers(self):
        while True:
            try:
                r=await self._rest.get("/fapi/v1/ticker/24hr"); r.raise_for_status()
                for item in r.json():
                    symbol=item.get("symbol")
                    if symbol in self.symbols:
                        self.prices[symbol]=float(item["lastPrice"])
                        self.changes_24h[symbol]=float(item["priceChangePercent"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Ticker refresh failed: %s",exc)
            await asyncio.sleep(60)

    async def _fill_kline_gap(self, symbol, timeframe, current_open_ms):
        """Restore missed closed candles before applying the resumed WebSocket update."""
        interval_ms = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}[timeframe]
        series = self.bars[symbol][timeframe]
        if not series:
            return True
        request_start_ms = series[-1]["t"] + interval_ms
        if request_start_ms >= current_open_ms:
            return True

        async with self._resync_sem:
            while request_start_ms < current_open_ms:
                remaining_slots = (current_open_ms - request_start_ms + interval_ms - 1) // interval_ms
                limit = min(1000, remaining_slots)
                response = await self._rest.get("/fapi/v1/klines", params={
                    "symbol": symbol, "interval": timeframe, "startTime": request_start_ms,
                    "endTime": current_open_ms - 1, "limit": limit,
                })
                response.raise_for_status()
                raw_rows = response.json()
                if not isinstance(raw_rows, list):
                    raise RuntimeError(f"Invalid kline resync payload for {symbol} {timeframe}")
                rows = sorted((row for row in raw_rows
                               if request_start_ms <= int(row[0]) < current_open_ms), key=lambda row: int(row[0]))
                if not rows:
                    if raw_rows:
                        log.warning("Kline resync returned only out-of-range rows for %s %s", symbol, timeframe)
                        return False
                    # Empty ranges can be valid when Binance has no kline records for that period.
                    return True
                restored_rows = []
                last_seen_ms = series[-1]["t"]
                for row in rows:
                    if not isinstance(row, (list, tuple)) or len(row) < 6:
                        raise RuntimeError(f"Incomplete kline row during resync for {symbol} {timeframe}")
                    row_open_ms = int(row[0])
                    if row_open_ms <= last_seen_ms:
                        continue
                    restored_rows.append({"t": row_open_ms, "o": float(row[1]), "h": float(row[2]),
                                          "l": float(row[3]), "c": float(row[4]), "v": float(row[5]),
                                          "closed": True})
                    last_seen_ms = row_open_ms
                series.extend(restored_rows)
                if restored_rows:
                    self._mark_bar_revision(symbol, timeframe, closed=True)
                if len(rows) < limit:
                    return True
                next_start_ms = int(rows[-1][0]) + interval_ms
                if next_start_ms <= request_start_ms:
                    log.warning("Kline resync made no progress for %s %s at %s", symbol, timeframe, request_start_ms)
                    return False
                request_start_ms = next_start_ms
        return True

    async def _handle_ws_message(self, message, expected_streams, group_state):
        if message.get("code") is not None:
            raise RuntimeError(f"Binance WebSocket error {message.get('code')}: {message.get('msg', 'unknown error')}")
        # Raw and combined stream envelopes are both accepted during Binance routing transitions.
        data = message.get("data", message)
        k = data.get("k") if isinstance(data, dict) else None
        if not k:
            return False
        symbol, timeframe = k.get("s"), k.get("i")
        if not symbol or not timeframe or f"{symbol.lower()}@kline_{timeframe}" not in expected_streams:
            log.warning("Ignoring unexpected Binance WebSocket event in %s", group_state["id"])
            return False
        bar = {"t": int(k["t"]), "o": float(k["o"]), "h": float(k["h"]),
               "l": float(k["l"]), "c": float(k["c"]), "v": float(k["v"]), "closed": bool(k["x"])}
        series = self.bars[symbol][timeframe]
        if series and bar["t"] < series[-1]["t"]:
            # Correct only an already retained, closed candle. Do not insert old bars.
            for index in range(len(series) - 1, -1, -1):
                existing = series[index]
                if existing["t"] != bar["t"]:
                    continue
                if not existing.get("closed") or not bar["closed"]:
                    break
                if existing == bar:
                    return False
                series[index] = bar
                self._mark_bar_revision(symbol, timeframe, closed=True)
                self._publish_kline(symbol, timeframe, full=True)
                return True
            log.debug("Ignoring out-of-order kline for %s %s at %s", symbol, timeframe, bar["t"])
            return False
        if series and bar["t"] > series[-1]["t"]:
            restored = await self._fill_kline_gap(symbol, timeframe, bar["t"])
            if not restored:
                raise RuntimeError(f"Could not restore missing candles for {symbol} {timeframe}")
        previous_time = series[-1]["t"] if series else None
        changed = not series or series[-1] != bar
        if series and series[-1]["t"] == bar["t"]:
            # A late/open update for a candle already finalized must never
            # roll the closed value back to an in-progress state.
            if series[-1].get("closed") and not bar["closed"]:
                return False
            series[-1] = bar
        elif not series or bar["t"] > series[-1]["t"]:
            series.append(bar)
        if changed:
            self._mark_bar_revision(symbol, timeframe, closed=bar["closed"])
            self._publish_kline(symbol, timeframe, full=bar["closed"] or previous_time != bar["t"])
        self.prices[symbol] = bar["c"]
        self.updated_at = datetime.now(timezone.utc).isoformat()
        group_state["last_data_at"] = self.updated_at
        group_state["status"] = "live"
        return True

    async def stream_group(self, streams, group_state):
        expected_streams = set(streams)
        # A 1.25s interval leaves margin under Binance's 300 attempts / 5 minute quota,
        # including when many groups reconnect at once.
        async with self._ws_connect_lock:
            loop = asyncio.get_running_loop()
            wait = 1.25 - (loop.time() - self._last_ws_connect)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_ws_connect = loop.time()
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5,
                                      open_timeout=15, max_size=2**22) as ws:
            await ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))
            group_state["status"] = "awaiting_ack"
            deadline = asyncio.get_running_loop().time() + 10
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for Binance SUBSCRIBE acknowledgement")
                message = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining))
                if message.get("id") == 1:
                    if message.get("code") is not None or message.get("result", object()) is not None:
                        raise RuntimeError(f"Binance rejected WebSocket subscriptions: {message}")
                    break
                await self._handle_ws_message(message, expected_streams, group_state)

            group_state["connected_at"] = datetime.now(timezone.utc).isoformat()
            group_state["status"] = "awaiting_data"
            last_market_event = asyncio.get_running_loop().time()
            while True:
                # websockets handles protocol PING/PONG; this detects a live but silent data stream.
                remaining = WS_SILENCE_TIMEOUT_SECONDS - (asyncio.get_running_loop().time() - last_market_event)
                if remaining <= 0:
                    raise TimeoutError("No valid kline data received before the WebSocket silence deadline")
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                if await self._handle_ws_message(json.loads(raw), expected_streams, group_state):
                    last_market_event = asyncio.get_running_loop().time()

    async def run_streams(self):
        names = list(dict.fromkeys(f"{s.lower()}@kline_{tf}" for s in self.symbols for tf in TIMEFRAMES))
        groups = [names[i:i+WS_GROUP_SIZE] for i in range(0, len(names), WS_GROUP_SIZE)]

        async def supervise(group, group_number):
            state = {"id": f"group-{group_number}", "status": "connecting", "streams": len(group),
                     "reconnects": 0, "connected_at": None, "last_data_at": None, "last_error": None}
            self.ws_groups[state["id"]] = state
            delay = 1
            while True:
                try:
                    state["status"] = "connecting"
                    await self.stream_group(group, state)
                    state["status"] = "disconnected"
                    delay = 1
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    state["status"] = "reconnecting"
                    state["last_error"] = str(exc)
                    state["reconnects"] += 1
                    log.warning("Binance WebSocket %s lost (%s); reconnecting in about %ss", state["id"], exc, delay)
                    await asyncio.sleep(delay + random.uniform(0, min(5, delay * 0.2)))
                    delay = min(delay * 2, 60)

        await asyncio.gather(*(supervise(group, index+1) for index, group in enumerate(groups)))
