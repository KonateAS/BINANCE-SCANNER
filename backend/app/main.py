import asyncio
import hmac
import ipaddress
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from .config import REST_OPERATOR_TOKEN, TIMEFRAMES
from .binance_rest import BinanceRateLimitError
from .market import MarketData
from .signals import analyze_closed, preview_signal
from .store import Store

logging.basicConfig(level=logging.INFO)
market=MarketData(); store=Store(); clients=set()
_signal_cache = {}
_persisted_signal_signatures = {}
_snapshot_lock = asyncio.Lock()
_PERSISTED_SIGNAL_FIELDS = ("timestamp", "side", "score", "confirmed", "pre_signal", "price", "rsi", "ema9", "ema21",
                            "macd_hist", "atr", "volume_ratio", "divergence", "divergence_detected",
                            "divergence_candidate", "divergence_confirmed")

def _analyze_changed_series(batch):
    return [(key, revisions, timeframe, analyze_closed(bars, timeframe))
            for key, revisions, timeframe, bars in batch]

async def _wait_for_thread(worker):
    """Wait for a shielded to_thread task despite repeated caller cancellation."""
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            continue
    return worker.result()

async def _snapshot_bundle():
    # Capture changed candle histories synchronously on the event loop, then
    # run NumPy/Python indicator work off-loop using immutable snapshots.
    async with _snapshot_lock:
        batch=[]
        open_updates=[]
        for symbol in market.symbols:
            for timeframe in TIMEFRAMES:
                key=(symbol,timeframe)
                revision=market.bar_revisions.get(key,0)
                closed_revision=market.closed_revisions.get(key,0)
                cached=_signal_cache.get(key)
                if cached is None or cached["closed_revision"] != closed_revision:
                    bars=tuple(market.bars[symbol][timeframe])
                    batch.append((key,(revision,closed_revision),timeframe,bars))
                elif cached["revision"] != revision:
                    series=market.bars[symbol][timeframe]
                    latest_open=series[-1] if series and not series[-1].get("closed",False) else None
                    # Candle dictionaries are replaced, never mutated in place.
                    open_updates.append((key,revision,timeframe,latest_open))

        if batch:
            worker=asyncio.create_task(asyncio.to_thread(_analyze_changed_series,batch))
            try:
                computed=await asyncio.shield(worker)
            except asyncio.CancelledError:
                # Threads cannot be force-cancelled; wait for this bounded batch
                # to finish before allowing application shutdown to continue.
                await _wait_for_thread(worker)
                raise
            for key,(revision,closed_revision),timeframe,(result,state,open_bar) in computed:
                _signal_cache[key]={"revision":revision,"closed_revision":closed_revision,
                                    "timeframe":timeframe,"result":result,"state":state,
                                    "open_bar":open_bar,
                                    "pre_signal":preview_signal(state,open_bar,timeframe)}
        for key,revision,timeframe,open_bar in open_updates:
            cached=_signal_cache[key]
            previous_open=cached["open_bar"]
            cached["revision"]=revision
            cached["open_bar"]=open_bar
            previous_input=(previous_open.get("t"),previous_open.get("c")) if previous_open else None
            current_input=(open_bar.get("t"),open_bar.get("c")) if open_bar else None
            if previous_input != current_input:
                cached["pre_signal"]=preview_signal(cached["state"],open_bar,timeframe)

        rows=[]
        signal_prices={}
        for symbol in market.symbols:
            for timeframe in TIMEFRAMES:
                key=(symbol,timeframe)
                cached=_signal_cache.get(key)
                result=cached["result"] if cached else None
                if result is None:
                    continue
                row={"symbol":symbol,"timeframe":timeframe,**result,
                     "pre_signal":cached["pre_signal"],
                     "price":market.prices.get(symbol,result["price"]),
                     "change_24h":market.changes_24h.get(symbol)}
                rows.append(row)
                signal_prices[key]=result["price"]
        return rows,signal_prices

async def snapshot():
    rows,_=await _snapshot_bundle()
    return rows

async def persist_new_confirmed(rows,signal_prices=None):
    """Keep one DB row per candle and update it only when signal content changes."""
    for row in rows:
        key=(row["symbol"],row["timeframe"])
        timestamp=row["timestamp"]
        if not row["confirmed"]:
            invalid_signature=("invalidated",timestamp)
            if _persisted_signal_signatures.get(key)==invalid_signature:
                continue
            if await store.invalidate(row["symbol"],row["timeframe"],timestamp):
                _persisted_signal_signatures[key]=invalid_signature
            continue
        persisted_row=dict(row)
        if signal_prices and key in signal_prices:
            # Persist the analyzed candle close, not the independently refreshed ticker.
            persisted_row["price"]=signal_prices[key]
        signature=tuple(persisted_row.get(field) for field in _PERSISTED_SIGNAL_FIELDS)
        if _persisted_signal_signatures.get(key) == signature:
            continue
        if await store.save(row["symbol"],row["timeframe"],persisted_row):
            _persisted_signal_signatures[key]=signature

async def broadcaster():
    while True:
        rows,signal_prices=await _snapshot_bundle()
        await persist_new_confirmed(rows,signal_prices)
        stale=set()
        for ws in clients:
            try: await ws.send_json({"type":"snapshot","data":rows,"updated_at":market.updated_at})
            except Exception: stale.add(ws)
        clients.difference_update(stale)
        await asyncio.sleep(3)

async def scanner():
    delay=2
    while True:
        try:
            await market.discover()
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Binance REST unavailable; discovery retry in %ss: %s",delay,exc)
            await asyncio.sleep(delay)
            delay=min(delay*2,60)
    logging.info("Discovered %d active USDT perpetuals",len(market.symbols))
    await market.history()
    async with asyncio.TaskGroup() as group:
        group.create_task(market.run_streams()); group.create_task(market.refresh_tickers()); group.create_task(broadcaster())

@asynccontextmanager
async def lifespan(app):
    task = None
    try:
        await store.connect()
        task = asyncio.create_task(scanner())
        yield
    finally:
        try:
            if task:
                task.cancel()
                try: await task
                except asyncio.CancelledError: pass
        finally:
            try:
                await market.close()
            finally:
                if store.pool: await store.pool.close()

app=FastAPI(title="Binance Futures Scanner",version="1.0.0",lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["http://localhost:3000","http://127.0.0.1:3000"],allow_methods=["GET"],allow_headers=["*"])

@app.get("/api/health")
async def health(): return {"status":"ok","symbols":len(market.symbols),"database":store.pool is not None,"updated_at":market.updated_at,"websocket_groups":list(market.ws_groups.values()),"errors":market.errors[-5:],"rest":await market.rest_status()}

def require_rest_operator(request: Request, supplied_token: str | None):
    if not REST_OPERATOR_TOKEN:
        raise HTTPException(status_code=503, detail="REST operator recovery is disabled; configure REST_OPERATOR_TOKEN")
    try:
        remote = ipaddress.ip_address(request.client.host if request.client else "")
    except ValueError:
        raise HTTPException(status_code=403, detail="Loopback access is required")
    if isinstance(remote, ipaddress.IPv6Address) and remote.ipv4_mapped is not None:
        remote = remote.ipv4_mapped
    if (not remote.is_loopback or not supplied_token or
            not hmac.compare_digest(supplied_token.encode("utf-8"), REST_OPERATOR_TOKEN.encode("utf-8"))):
        raise HTTPException(status_code=403, detail="Valid operator token and loopback access are required")

@app.post("/api/admin/rest-suspension/acknowledge")
async def acknowledge_rest_suspension(request: Request, x_rest_operator_token: str | None = Header(default=None)):
    require_rest_operator(request, x_rest_operator_token)
    try:
        await market.acknowledge_rest_suspension()
    except BinanceRateLimitError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "operator_acknowledgement_recorded; REST remains suspended", "rest": await market.rest_status()}

@app.post("/api/admin/rest-suspension/resume")
async def resume_rest_after_operator_action(request: Request, x_rest_operator_token: str | None = Header(default=None)):
    require_rest_operator(request, x_rest_operator_token)
    try:
        await market.resume_rest()
    except BinanceRateLimitError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "operator_resume_recorded", "rest": await market.rest_status()}

@app.get("/api/markets")
async def markets(timeframe:str|None=None,side:str|None=None,confirmed:bool|None=None,limit:int=200):
    rows=await snapshot()
    if timeframe in TIMEFRAMES: rows=[r for r in rows if r["timeframe"]==timeframe]
    if side in ("LONG","SHORT"): rows=[r for r in rows if r["side"]==side]
    if confirmed is not None: rows=[r for r in rows if r["confirmed"]==confirmed]
    rows.sort(key=lambda r:(r["confirmed"],r["score"]),reverse=True)
    return {"data":rows[:max(1,min(limit,1000))],"total":len(rows),"symbols":len(market.symbols)}

@app.get("/api/klines/{symbol}")
async def klines(symbol:str,timeframe:str="15m"):
    symbol=symbol.upper()
    if timeframe not in TIMEFRAMES or symbol not in market.symbols:return {"data":[]}
    return {"data":list(market.bars[symbol][timeframe])}

@app.websocket("/ws/klines/{symbol}/{timeframe}")
async def kline_websocket(ws: WebSocket, symbol: str, timeframe: str):
    symbol = symbol.upper()
    if timeframe not in TIMEFRAMES or symbol not in market.symbols:
        await ws.close(code=1008)
        return
    key = (symbol, timeframe)
    queue = asyncio.Queue(maxsize=1)
    await ws.accept()
    subscribers = market.kline_subscribers[key]
    subscribers.add(queue)
    receiver = asyncio.create_task(ws.receive())
    queue_task = None
    try:
        await ws.send_json({"type": "history", "data": list(market.bars[symbol][timeframe])})
        while True:
            queue_task = asyncio.create_task(queue.get())
            done, _ = await asyncio.wait((receiver, queue_task), return_when=asyncio.FIRST_COMPLETED)
            if receiver in done:
                event = receiver.result()
                if event["type"] == "websocket.disconnect":
                    break
                receiver = asyncio.create_task(ws.receive())
            if queue_task in done:
                await ws.send_json(queue_task.result())
                queue_task = None
            else:
                queue_task.cancel()
                await asyncio.gather(queue_task, return_exceptions=True)
                queue_task = None
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        if queue_task and not queue_task.done():
            queue_task.cancel()
            await asyncio.gather(queue_task, return_exceptions=True)
        if not receiver.done():
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)
        subscribers.discard(queue)
        if not subscribers:
            market.kline_subscribers.pop(key, None)

@app.websocket("/ws")
async def websocket_endpoint(ws:WebSocket):
    await ws.accept(); clients.add(ws)
    try:
        await ws.send_json({"type":"snapshot","data":await snapshot(),"updated_at":market.updated_at})
        while True: await ws.receive_text()
    except WebSocketDisconnect: clients.discard(ws)
