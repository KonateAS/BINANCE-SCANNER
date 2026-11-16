"""Deterministic tests for scanner caching, async execution, and shutdown."""
import asyncio
import time
import unittest
import threading
from collections import defaultdict
from unittest.mock import AsyncMock, patch

from app import main


def make_bar(timestamp, close=100.0, closed=True):
    return {"t": timestamp, "o": close, "h": close + 1, "l": close - 1,
            "c": close, "v": 10.0, "closed": closed}


class FakeMarket:
    def __init__(self, symbols=("AAAUSDT", "BBBUSDT"), timeframes=("5m", "15m")):
        self.symbols = list(symbols)
        self.bars = defaultdict(dict)
        self.bar_revisions = {}
        self.closed_revisions = {}
        self.prices = {symbol: 101.0 for symbol in symbols}
        self.changes_24h = {symbol: 1.0 for symbol in symbols}
        self.updated_at = None
        for si, symbol in enumerate(symbols):
            for ti, timeframe in enumerate(timeframes):
                key = (symbol, timeframe)
                closed = make_bar((si * 10 + ti + 1) * 300_000, 100.0 + si + ti)
                opened = make_bar(closed["t"] + 300_000, 100.0, False)
                self.bars[symbol][timeframe] = [closed, opened]
                self.bar_revisions[key] = 1
                self.closed_revisions[key] = 1


def fake_analysis(bars, timeframe):
    closed = [bar for bar in bars if bar["closed"]]
    last = closed[-1]
    result = {"side": "LONG", "score": 50, "confirmed": False,
              "pre_signal": False, "price": last["c"], "rsi": 50.0,
              "ema9": 11.0, "ema21": 10.0, "macd_hist": 0.0,
              "atr": 1.0, "volume_ratio": 1.0, "divergence": None,
              "divergence_detected": False, "divergence_candidate": None,
              "divergence_confirmed": False, "timestamp": last["t"]}
    state = {"close": last["c"], "ema9": 11.0, "ema21": 10.0,
             "ema12": 11.0, "ema26": 10.0, "macd_signal": 0.0,
             "macd_hist": 7.0, "side": "LONG", "confirmed": False,
             "timestamp": last["t"]}
    opened = bars[-1] if bars and not bars[-1]["closed"] else None
    return result, state, opened


class ScannerCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.market = FakeMarket()
        self.cache = main._signal_cache
        self.cache.clear()
        main._persisted_signal_signatures.clear()
        self.patches = [
            patch.object(main, "market", self.market),
            patch.object(main, "TIMEFRAMES", ("5m", "15m")),
            patch.object(main, "analyze_closed", side_effect=fake_analysis),
        ]
        for item in self.patches:
            item.start()
        self.addCleanup(self._stop_patches)

    def _stop_patches(self):
        for item in reversed(self.patches):
            item.stop()
        self.cache.clear()
        main._persisted_signal_signatures.clear()

    async def test_unchanged_series_are_not_reanalyzed_and_snapshot_contract_is_kept(self):
        with patch.object(main, "analyze_closed", side_effect=fake_analysis) as analyze:
            first = await main.snapshot()
            calls = analyze.call_count
            self.assertEqual(calls, 4)
            second = await main.snapshot()
            self.assertEqual(analyze.call_count, calls)
        self.assertEqual(len(first), 4)
        self.assertEqual(first, second)
        self.assertEqual({(row["symbol"], row["timeframe"]) for row in first},
                         {(symbol, timeframe) for symbol in self.market.symbols for timeframe in ("5m", "15m")})
        for row in first:
            self.assertIn(row["symbol"], self.market.symbols)
            self.assertIn(row["timeframe"], ("5m", "15m"))
            self.assertIn("change_24h", row)
            self.assertIsInstance(row["confirmed"], bool)
            self.assertIsInstance(row["pre_signal"], bool)

    async def test_open_updates_refresh_preview_without_recalculating_closed_indicators(self):
        with patch.object(main, "analyze_closed", side_effect=fake_analysis) as analyze:
            initial = await main.snapshot()
            self.assertFalse(next(row for row in initial if row["symbol"] == "AAAUSDT" and row["timeframe"] == "5m")["pre_signal"])
            key = ("AAAUSDT", "5m")
            old_open = self.market.bars[key[0]][key[1]][-1]
            self.market.bars[key[0]][key[1]][-1] = dict(old_open, c=130.0, h=131.0)
            self.market.bar_revisions[key] += 1
            updated = await main.snapshot()
            self.assertEqual(analyze.call_count, 4)
            target = next(row for row in updated if row["symbol"] == key[0] and row["timeframe"] == key[1])
            self.assertTrue(target["pre_signal"])

    async def test_open_volume_only_update_does_not_recalculate_preview(self):
        await main.snapshot()
        key = ("AAAUSDT", "5m")
        series = self.market.bars[key[0]][key[1]]
        series[-1] = dict(series[-1], v=series[-1]["v"] + 1)
        self.market.bar_revisions[key] += 1
        with patch.object(main, "preview_signal", wraps=main.preview_signal) as preview:
            await main.snapshot()
            preview.assert_not_called()

    async def test_closed_update_reanalyzes_only_its_symbol_timeframe(self):
        with patch.object(main, "analyze_closed", side_effect=fake_analysis) as analyze:
            await main.snapshot()
            key = ("BBBUSDT", "15m")
            series = self.market.bars[key[0]][key[1]]
            series[-1] = dict(series[-1], closed=True)
            self.market.bar_revisions[key] += 1
            self.market.closed_revisions[key] += 1
            await main.snapshot()
            self.assertEqual(analyze.call_count, 5)

    async def test_historical_closed_correction_invalidates_analysis_cache(self):
        with patch.object(main, "analyze_closed", side_effect=fake_analysis) as analyze:
            await main.snapshot()
            key = ("AAAUSDT", "5m")
            series = self.market.bars[key[0]][key[1]]
            series[0] = dict(series[0], c=series[0]["c"] + 1)
            self.market.bar_revisions[key] += 1
            self.market.closed_revisions[key] += 1
            await main.snapshot()
            self.assertEqual(analyze.call_count, 5)
            self.assertEqual(analyze.call_args_list[-1].args[0][0]["c"], series[0]["c"])

    async def test_concurrent_snapshot_calls_do_not_duplicate_work(self):
        with patch.object(main, "analyze_closed", side_effect=fake_analysis) as analyze:
            first, second = await asyncio.gather(main.snapshot(), main.snapshot())
            self.assertEqual(analyze.call_count, 4)
        self.assertEqual(first, second)

    async def test_confirmed_persistence_retries_and_updates_changed_same_timestamp_content(self):
        row = {"symbol": "AAAUSDT", "timeframe": "5m", "timestamp": 123,
               "confirmed": True, "score": 70, "price": 110.0}
        ticker_changed = dict(row, price=120.0)
        corrected = dict(row, side="SHORT", score=65, price=130.0)
        with patch.object(main.store, "save", new_callable=AsyncMock, side_effect=[False, True, True]) as save:
            await main.persist_new_confirmed([row], {("AAAUSDT", "5m"): 100.0})
            await main.persist_new_confirmed([row], {("AAAUSDT", "5m"): 100.0})
            await main.persist_new_confirmed([ticker_changed], {("AAAUSDT", "5m"): 100.0})
            await main.persist_new_confirmed([corrected], {("AAAUSDT", "5m"): 100.0})
            await main.persist_new_confirmed([corrected], {("AAAUSDT", "5m"): 100.0})
        self.assertEqual(save.await_count, 3)

    async def test_unconfirmed_correction_invalidation_retry_and_reconfirmation(self):
        confirmed = {"symbol": "AAAUSDT", "timeframe": "5m", "timestamp": 123,
                     "confirmed": True, "score": 70, "price": 110.0}
        unconfirmed = dict(confirmed, confirmed=False, side="SHORT")
        with patch.object(main.store, "save", new_callable=AsyncMock, return_value=True) as save, \
             patch.object(main.store, "invalidate", new_callable=AsyncMock,
                          side_effect=[False, True]) as invalidate:
            await main.persist_new_confirmed([confirmed])
            await main.persist_new_confirmed([unconfirmed])
            await main.persist_new_confirmed([unconfirmed])
            await main.persist_new_confirmed([unconfirmed])
            await main.persist_new_confirmed([confirmed])
        self.assertEqual(save.await_count, 2)
        self.assertEqual(invalidate.await_count, 2)
        self.assertEqual(invalidate.await_args_list[0].args, ("AAAUSDT", "5m", 123))

    async def test_indicator_batch_does_not_block_event_loop(self):
        def slow_batch(batch):
            time.sleep(0.15)
            return [(key, revisions, timeframe, (None, None, None))
                    for key, revisions, timeframe, _ in batch]

        ready = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.call_later(0.02, ready.set)
        with patch.object(main, "_analyze_changed_series", side_effect=slow_batch):
            work = asyncio.create_task(main.snapshot())
            await asyncio.wait_for(ready.wait(), timeout=0.08)
            await work

    async def test_cancelled_snapshot_waits_for_thread_batch_to_finish(self):
        finished = threading.Event()
        started = threading.Event()
        release = threading.Event()

        def slow_batch(batch):
            started.set()
            release.wait(timeout=2)
            finished.set()
            return [(key, revisions, timeframe, (None, None, None))
                    for key, revisions, timeframe, _ in batch]

        with patch.object(main, "_analyze_changed_series", side_effect=slow_batch):
            work = asyncio.create_task(main.snapshot())
            while not started.is_set():
                await asyncio.sleep(0.001)
            work.cancel()
            await asyncio.sleep(0)
            work.cancel()
            await asyncio.sleep(0)
            self.assertFalse(work.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await work
        self.assertTrue(finished.is_set())


class ScannerShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan_cancels_scanner_and_closes_market(self):
        stopped = asyncio.Event()

        class FakeStore:
            pool = None
            async def connect(self): pass

        class ClosingMarket:
            closed = False
            async def close(self): self.closed = True

        async def waiting_scanner():
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        fake_market = ClosingMarket()
        with patch.object(main, "store", FakeStore()), \
             patch.object(main, "market", fake_market), \
             patch.object(main, "scanner", waiting_scanner):
            async with main.lifespan(main.app):
                await asyncio.sleep(0)
        self.assertTrue(stopped.is_set())
        self.assertTrue(fake_market.closed)


if __name__ == "__main__":
    unittest.main()
