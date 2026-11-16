"""Deterministic, offline unit tests for Binance market-stream handling."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from app.market import MarketData


def candle(symbol="BTCUSDT", timeframe="5m", open_ms=300_000, close="101", closed=False):
    return {"stream": f"{symbol.lower()}@kline_{timeframe}", "data": {"e": "kline", "k": {
        "t": open_ms, "s": symbol, "i": timeframe, "o": "100", "h": "102",
        "l": "99", "c": close, "v": "12", "x": closed,
    }}}


class FakeResponse:
    def __init__(self, rows):
        self.rows = rows

    def raise_for_status(self):
        return None

    def json(self):
        return self.rows


class FakeSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []
        self.closed = False
        self.data_seen = asyncio.Event()
        self.block = asyncio.Event()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.closed = True

    async def send(self, value):
        self.sent.append(json.loads(value))

    async def recv(self):
        if self.messages:
            message = self.messages.pop(0)
            if isinstance(message, Exception):
                raise message
            decoded = message if isinstance(message, dict) else json.loads(message)
            if "k" in decoded.get("data", {}):
                self.data_seen.set()
            return json.dumps(decoded)
        await self.block.wait()
        raise AssertionError("unreachable")


class MarketWebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.market = MarketData()
        self.state = {"id": "group-1", "status": "connecting", "last_data_at": None}
        self.streams = ["btcusdt@kline_5m"]

    async def asyncTearDown(self):
        await self.market.close()

    async def test_normal_subscription_and_kline_message(self):
        socket = FakeSocket([{"result": None, "id": 1}, candle()])
        with patch("app.market.websockets.connect", return_value=socket):
            task = asyncio.create_task(self.market.stream_group(self.streams, self.state))
            await asyncio.wait_for(socket.data_seen.wait(), timeout=1)
            for _ in range(10):
                if self.state["status"] == "live":
                    break
                await asyncio.sleep(0)
            self.assertEqual(self.state["status"], "live")
            self.assertEqual(socket.sent[0]["method"], "SUBSCRIBE")
            self.assertEqual(self.market.bars["BTCUSDT"]["5m"][0]["t"], 300_000)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(socket.closed)

    async def test_disconnect_closes_socket_and_propagates(self):
        socket = FakeSocket([{"result": None, "id": 1}, ConnectionError("network disconnected")])
        with patch("app.market.websockets.connect", return_value=socket):
            with self.assertRaisesRegex(ConnectionError, "disconnected"):
                await self.market.stream_group(self.streams, self.state)
        self.assertTrue(socket.closed)

    async def test_silent_connection_times_out_and_closes(self):
        socket = FakeSocket([{"result": None, "id": 1}])
        with patch("app.market.WS_SILENCE_TIMEOUT_SECONDS", 0.01), \
             patch("app.market.websockets.connect", return_value=socket):
            with self.assertRaises(TimeoutError):
                await self.market.stream_group(self.streams, self.state)
        self.assertTrue(socket.closed)

    async def test_unexpected_events_do_not_keep_a_silent_group_alive(self):
        socket = FakeSocket([{"result": None, "id": 1}, candle(symbol="ETHUSDT")])
        with patch("app.market.WS_SILENCE_TIMEOUT_SECONDS", 0.01), \
             patch("app.market.websockets.connect", return_value=socket):
            with self.assertRaises(TimeoutError):
                await self.market.stream_group(self.streams, self.state)
        self.assertTrue(socket.closed)

    async def test_subscription_error_is_rejected(self):
        socket = FakeSocket([{"id": 1, "code": 2, "msg": "invalid subscription"}])
        with patch("app.market.websockets.connect", return_value=socket):
            with self.assertRaisesRegex(RuntimeError, "rejected"):
                await self.market.stream_group(self.streams, self.state)
        self.assertTrue(socket.closed)

    async def test_supervisor_retries_after_disconnect(self):
        self.market.symbols = ["BTCUSDT"]
        stream_group = AsyncMock(side_effect=[ConnectionError("lost"), asyncio.CancelledError()])
        with patch.object(self.market, "stream_group", stream_group):
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(self.market.run_streams(), timeout=3)
        self.assertEqual(stream_group.await_count, 2)
        self.assertEqual(self.market.ws_groups["group-1"]["reconnects"], 1)

    async def test_streams_are_deduplicated_and_keep_all_timeframes(self):
        self.market.symbols = ["BTCUSDT", "BTCUSDT"]
        seen = []

        async def stop_after_capture(streams, state):
            seen.extend(streams)
            raise asyncio.CancelledError()

        with patch.object(self.market, "stream_group", AsyncMock(side_effect=stop_after_capture)):
            with self.assertRaises(asyncio.CancelledError):
                await self.market.run_streams()
        self.assertEqual(len(seen), 4)
        self.assertEqual(len(set(seen)), 4)
        self.assertEqual({item.rsplit("_", 1)[1] for item in seen}, {"5m", "15m", "1h", "4h"})

    async def test_missing_closed_candles_are_restored_in_order(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        series.append({"t": 0, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "closed": True})
        response = FakeResponse([
            [600_000, "101", "103", "100", "102", "11", 899_999],
            [300_000, "100", "102", "99", "101", "10", 599_999],
        ])
        self.market._rest.get = AsyncMock(return_value=response)
        restored = await self.market._fill_kline_gap("BTCUSDT", "5m", 900_000)
        self.assertTrue(restored)
        self.assertEqual([bar["t"] for bar in series], [0, 300_000, 600_000])
        self.assertEqual(self.market.bar_revisions[("BTCUSDT", "5m")], 1)
        self.assertEqual(self.market.closed_revisions[("BTCUSDT", "5m")], 1)

    async def test_sparse_valid_resync_response_is_kept_in_timestamp_order(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        series.append({"t": 0, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "closed": True})
        self.market._rest.get = AsyncMock(return_value=FakeResponse([
            [600_000, "101", "103", "100", "102", "11", 899_999],
        ]))
        self.assertTrue(await self.market._fill_kline_gap("BTCUSDT", "5m", 900_000))
        self.assertEqual([bar["t"] for bar in series], [0, 600_000])

    async def test_empty_resync_range_does_not_block_the_live_stream(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        series.append({"t": 0, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "closed": True})
        self.market._rest.get = AsyncMock(return_value=FakeResponse([]))
        self.assertTrue(await self.market._fill_kline_gap("BTCUSDT", "5m", 600_000))

    async def test_out_of_range_resync_payload_is_not_silently_accepted(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        series.append({"t": 0, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "closed": True})
        self.market._rest.get = AsyncMock(return_value=FakeResponse([
            [600_000, "101", "103", "100", "102", "11", 899_999],
        ]))
        self.assertFalse(await self.market._fill_kline_gap("BTCUSDT", "5m", 600_000))

    async def test_malformed_partial_page_does_not_partially_mutate_candles(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        series.append({"t": 0, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "closed": True})
        self.market._rest.get = AsyncMock(return_value=FakeResponse([
            [300_000, "100", "102", "99", "101", "10", 599_999],
            [600_000, "101"],
        ]))
        with self.assertRaisesRegex(RuntimeError, "Incomplete kline row"):
            await self.market._fill_kline_gap("BTCUSDT", "5m", 900_000)
        self.assertEqual([bar["t"] for bar in series], [0])

    async def test_long_resync_is_paginated_at_binance_limit(self):
        interval = 300_000
        current_open = 1002 * interval
        first_page = [[i * interval, "100", "101", "99", "100", "1", i * interval + interval - 1]
                      for i in range(1, 1001)]
        second_page = [[1001 * interval, "100", "101", "99", "100", "1", 1002 * interval - 1]]
        self.market.bars["BTCUSDT"]["5m"].append(
            {"t": 0, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "closed": True})
        self.market._rest.get = AsyncMock(side_effect=[FakeResponse(first_page), FakeResponse(second_page)])
        self.assertTrue(await self.market._fill_kline_gap("BTCUSDT", "5m", current_open))
        self.assertEqual(self.market._rest.get.await_count, 2)
        self.assertEqual(self.market.bars["BTCUSDT"]["5m"][-1]["t"], 1001 * interval)

    async def test_duplicate_candle_updates_in_place(self):
        message1 = candle(open_ms=300_000, close="101")
        message2 = candle(open_ms=300_000, close="102")
        await self.market._handle_ws_message(message1, set(self.streams), self.state)
        await self.market._handle_ws_message(message2, set(self.streams), self.state)
        series = self.market.bars["BTCUSDT"]["5m"]
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["c"], 102)

    async def test_revisions_change_only_when_series_data_changes(self):
        message = candle(open_ms=300_000, close="101", closed=False)
        await self.market._handle_ws_message(message, set(self.streams), self.state)
        key = ("BTCUSDT", "5m")
        self.assertEqual(self.market.bar_revisions[key], 1)
        self.assertEqual(self.market.closed_revisions[key], 0)
        await self.market._handle_ws_message(message, set(self.streams), self.state)
        self.assertEqual(self.market.bar_revisions[key], 1)
        await self.market._handle_ws_message(candle(open_ms=300_000, close="102", closed=False), set(self.streams), self.state)
        self.assertEqual(self.market.bar_revisions[key], 2)
        self.assertEqual(self.market.closed_revisions[key], 0)
        await self.market._handle_ws_message(candle(open_ms=300_000, close="102", closed=True), set(self.streams), self.state)
        self.assertEqual(self.market.bar_revisions[key], 3)
        self.assertEqual(self.market.closed_revisions[key], 1)

    async def test_open_duplicate_cannot_replace_closed_candle(self):
        closed = candle(open_ms=300_000, close="101", closed=True)
        await self.market._handle_ws_message(closed, set(self.streams), self.state)
        self.market.prices["BTCUSDT"] = 101
        accepted = await self.market._handle_ws_message(
            candle(open_ms=300_000, close="99", closed=False), set(self.streams), self.state)
        series = self.market.bars["BTCUSDT"]["5m"]
        self.assertFalse(accepted)
        self.assertTrue(series[0]["closed"])
        self.assertEqual(series[0]["c"], 101)
        self.assertEqual(self.market.prices["BTCUSDT"], 101)

    async def test_corrects_retained_closed_historical_candle_and_revisions(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        series.extend([
            {"t": 300_000, "o": 100, "h": 102, "l": 99, "c": 101, "v": 10, "closed": True},
            {"t": 600_000, "o": 101, "h": 103, "l": 100, "c": 102, "v": 11, "closed": True},
        ])
        self.market.prices["BTCUSDT"] = 102
        accepted = await self.market._handle_ws_message(
            candle(open_ms=300_000, close="100.5", closed=True), set(self.streams), self.state)
        self.assertTrue(accepted)
        self.assertEqual([bar["t"] for bar in series], [300_000, 600_000])
        self.assertEqual(series[0]["c"], 100.5)
        self.assertEqual(series[1]["c"], 102)
        self.assertEqual(self.market.bar_revisions[("BTCUSDT", "5m")], 1)
        self.assertEqual(self.market.closed_revisions[("BTCUSDT", "5m")], 1)
        self.assertEqual(self.market.prices["BTCUSDT"], 102)

    async def test_identical_historical_closed_candle_does_not_increment_revisions(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        original = {"t": 300_000, "o": 100, "h": 102, "l": 99, "c": 101, "v": 12, "closed": True}
        series.extend([original, {"t": 600_000, "o": 101, "h": 103, "l": 100, "c": 102, "v": 11, "closed": True}])
        accepted = await self.market._handle_ws_message(
            candle(open_ms=300_000, close="101", closed=True), set(self.streams), self.state)
        self.assertFalse(accepted)
        self.assertEqual(self.market.bar_revisions[("BTCUSDT", "5m")], 0)
        self.assertEqual(self.market.closed_revisions[("BTCUSDT", "5m")], 0)

    async def test_old_open_and_unknown_historical_candles_are_ignored(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        series.extend([
            {"t": 300_000, "o": 100, "h": 102, "l": 99, "c": 101, "v": 12, "closed": True},
            {"t": 600_000, "o": 101, "h": 103, "l": 100, "c": 102, "v": 11, "closed": True},
        ])
        for event in (candle(open_ms=300_000, close="99", closed=False),
                      candle(open_ms=450_000, close="99", closed=True)):
            self.assertFalse(await self.market._handle_ws_message(event, set(self.streams), self.state))
        self.assertEqual(series[0]["c"], 101)
        self.assertEqual([bar["t"] for bar in series], [300_000, 600_000])
        self.assertEqual(self.market.bar_revisions[("BTCUSDT", "5m")], 0)
        self.assertEqual(self.market.closed_revisions[("BTCUSDT", "5m")], 0)

    async def test_out_of_order_candle_cannot_replace_newer_data_or_price(self):
        series = self.market.bars["BTCUSDT"]["5m"]
        series.append({"t": 600_000, "o": 100, "h": 103, "l": 99, "c": 102, "v": 12, "closed": True})
        self.market.prices["BTCUSDT"] = 102
        self.state["last_data_at"] = "newer"
        await self.market._handle_ws_message(candle(open_ms=300_000, close="95"), set(self.streams), self.state)
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["t"], 600_000)
        self.assertEqual(self.market.prices["BTCUSDT"], 102)
        self.assertEqual(self.state["last_data_at"], "newer")

    async def test_health_exposes_group_status_and_reconnect_errors(self):
        from app import main as api

        api.market.ws_groups = {"group-1": {"id": "group-1", "status": "reconnecting",
                                             "reconnects": 2, "last_data_at": "2026-11-14T00:00:00+00:00",
                                             "last_error": "silence timeout"}}
        try:
            health = await api.health()
            self.assertEqual(health["websocket_groups"][0]["status"], "reconnecting")
            self.assertEqual(health["websocket_groups"][0]["reconnects"], 2)
            self.assertEqual(health["websocket_groups"][0]["last_error"], "silence timeout")
        finally:
            await api.market.close()


if __name__ == "__main__":
    unittest.main()
