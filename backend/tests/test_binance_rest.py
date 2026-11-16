"""Offline deterministic tests for the shared Binance REST limiter."""
import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from app.binance_rest import BinanceRateLimiter, BinanceRestClient, BinanceRateLimitError, _http_date, _retry_after, _weight
from app.market import MarketData


RULES = {"serverTime": 120000, "rateLimits": [
    {"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 10},
    {"rateLimitType": "RAW_REQUEST", "interval": "MINUTE", "intervalNum": 1, "limit": 20},
]}


class FakeTime:
    def __init__(self):
        self.now = 100.0
        self.delays = []

    def clock(self):
        return self.now

    async def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay
        await asyncio.sleep(0)


class BinanceRestLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.time = FakeTime()
        self.limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await self.limiter.configure(RULES)

    async def test_endpoint_weights_follow_documented_parameters(self):
        self.assertEqual(_weight("/fapi/v1/exchangeInfo", None), 1)
        self.assertEqual([_weight("/fapi/v1/klines", {"limit": n}) for n in (99, 100, 499, 500, 1000, 1001)], [1, 2, 2, 5, 5, 10])
        self.assertEqual(_weight("/fapi/v1/ticker/24hr", None), 40)
        self.assertEqual(_weight("/fapi/v1/ticker/24hr", {"symbol": "BTCUSDT"}), 1)

    async def test_maximum_and_shared_quota_across_concurrent_callers(self):
        limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await limiter.configure({"serverTime":120000,"rateLimits": [{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":5}]})
        await asyncio.gather(*(limiter._reserve(1) for _ in range(5)))
        self.assertEqual(len(limiter._events), 5)
        await limiter._reserve(1)
        self.assertGreaterEqual(self.time.now, 160)

    async def test_reservation_and_header_reconciliation(self):
        await self.limiter._reserve(2)
        await self.limiter.reconcile({"X-MBX-USED-WEIGHT-1M": "5"})
        self.assertEqual(self.limiter._server[("REQUEST_WEIGHT", 60.0, 2)][0], 5)
        await self.limiter._reserve(2)
        self.assertEqual(len(self.limiter._events), 2)

    async def test_out_of_order_or_invalid_headers_cannot_lower_observed_usage(self):
        await self.limiter.reconcile({"X-MBX-USED-WEIGHT-1M": "9"})
        await self.limiter.reconcile({"X-MBX-USED-WEIGHT-1M": "4"})
        await self.limiter.reconcile({"X-MBX-USED-WEIGHT-1M": "-1", "X-MBX-USED-WEIGHT-1H": "bad"})
        self.assertEqual(self.limiter._server[("REQUEST_WEIGHT", 60.0, 2)][0], 9)
        self.assertNotIn(("REQUEST_WEIGHT", 3600.0, 0), self.limiter._server)

    async def test_shared_429_cooldown_drains_waiters_in_spaced_admissions(self):
        await self.limiter.suspend(1)
        await asyncio.gather(*(self.limiter._reserve(1) for _ in range(3)))
        timestamps = [event[0] for event in self.limiter._events]
        self.assertEqual(len(timestamps), 3)
        self.assertGreaterEqual(timestamps[1] - timestamps[0], 6)
        self.assertGreaterEqual(timestamps[2] - timestamps[1], 6)

    async def test_waits_when_next_reservation_would_exceed_limit(self):
        limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await limiter.configure({"serverTime":120000,"rateLimits": [{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2}]})
        await limiter._reserve(2)
        await limiter._reserve(1)
        self.assertGreaterEqual(self.time.now, 160)
        self.assertTrue(self.time.delays)

    async def test_independent_rate_windows_are_both_enforced(self):
        limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await limiter.configure({"rateLimits": [
            {"rateLimitType":"REQUEST_WEIGHT","interval":"SECOND","intervalNum":1,"limit":1},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":3},
        ], "serverTime":12000})
        await limiter._reserve(1)
        await limiter._reserve(1)
        self.assertGreaterEqual(self.time.now, 101)
        self.assertLess(self.time.now, 160)

    async def test_fixed_window_boundary_releases_quota_immediately(self):
        limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await limiter.configure({"serverTime":59900,"rateLimits": [
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2},
        ]})
        await limiter._reserve(2)
        await limiter._reserve(2)
        self.assertGreaterEqual(self.time.now, 100.1)
        self.assertLess(self.time.now, 100.2)
        self.assertEqual([int(event[1] // 60000) for event in limiter._events], [0, 1])

    async def test_server_header_is_reconciled_in_the_window_it_describes(self):
        limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await limiter.configure({"serverTime":59900,"rateLimits": [
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":10},
        ]})
        old_date = format_datetime(datetime.fromtimestamp(59, timezone.utc), usegmt=True)
        new_date = format_datetime(datetime.fromtimestamp(60, timezone.utc), usegmt=True)
        await limiter.reconcile({"X-MBX-USED-WEIGHT-1M":"9", "Date":old_date})
        self.time.now += 0.1
        await limiter.reconcile({"X-MBX-USED-WEIGHT-1M":"1", "Date":new_date})
        self.assertEqual(limiter._server[("REQUEST_WEIGHT", 60.0, 0)][0], 9)
        await limiter._reserve(9)
        self.assertNotIn(("REQUEST_WEIGHT", 60.0, 0), limiter._server)
        self.assertEqual(limiter._server[("REQUEST_WEIGHT", 60.0, 1)][0], 1)
        self.assertEqual(int(limiter._events[-1][1] // 60000), 1)

    async def test_missing_date_uses_server_anchor_not_local_wall_clock(self):
        limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await limiter.configure(RULES)
        with patch("app.binance_rest.time.time", return_value=9_999_999_999_000):
            self.assertEqual(limiter._server_now_ms(), 120000)
            await limiter.reconcile({"X-MBX-USED-WEIGHT-1M":"4"})
            self.assertIn(("REQUEST_WEIGHT", 60.0, 2), limiter._server)
            self.time.now += 1
            self.assertEqual(limiter._server_now_ms(), 121000)

    async def test_incoherent_response_date_falls_back_to_estimated_server_window(self):
        limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await limiter.configure(RULES)
        await limiter.reconcile({
            "X-MBX-USED-WEIGHT-1M": "8",
            "Date": "Thu Jan  1 00:00:00 1970",
        })
        self.assertEqual(limiter._server[("REQUEST_WEIGHT", 60.0, 2)][0], 8)

    async def test_cancelled_quota_wait_does_not_create_reservation_or_hold_lock(self):
        limiter = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await limiter.configure({"serverTime":120000,"rateLimits": [{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":1}]})
        await limiter._reserve(1)
        waiting = asyncio.Event()
        release = asyncio.Event()
        async def blocked_sleep(delay):
            waiting.set()
            await release.wait()
        limiter._sleep = blocked_sleep
        task = asyncio.create_task(limiter._reserve(1))
        await waiting.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(limiter._events), 1)
        async with limiter._lock:
            self.assertTrue(limiter._lock.locked())

    async def test_temporary_suspension_recovers_and_permanent_suspension_is_shared(self):
        temporary = BinanceRateLimiter(clock=self.time.clock, sleep=self.time.sleep)
        await temporary.configure(RULES)
        await temporary.suspend(2)
        await temporary._reserve(1)
        self.assertGreaterEqual(self.time.now, 102)

        await self.limiter.suspend(None)
        results = await asyncio.gather(*(self.limiter._reserve(1) for _ in range(3)), return_exceptions=True)
        self.assertTrue(all(isinstance(item, BinanceRateLimitError) for item in results))

    async def _client(self, handler, *, sleep=None):
        transport = httpx.MockTransport(handler)
        client = BinanceRestClient("https://unit.test", transport=transport,
                                   limiter=self.limiter,
                                   sleep=sleep or self.time.sleep, jitter=lambda a, b: 0)
        return client

    async def test_429_retry_after_and_without_header(self):
        calls = 0
        def handler(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(429, headers={"Retry-After":"3"})
            return httpx.Response(200, json={"symbols":[], **RULES})
        client = await self._client(handler)
        await client.get("/fapi/v1/exchangeInfo")
        self.assertEqual(calls, 2)
        self.assertIn(3, self.time.delays)
        await client.aclose()

        self.time.delays.clear()
        calls = 0
        def handler_no_header(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(429)
            return httpx.Response(200, json={"symbols":[], **RULES})
        client = await self._client(handler_no_header)
        await client.get("/fapi/v1/exchangeInfo")
        self.assertTrue(self.time.delays)
        await client.aclose()

    async def test_429_retry_count_is_bounded(self):
        calls = 0
        def handler(request):
            nonlocal calls
            calls += 1
            return httpx.Response(429)
        client = await self._client(handler)
        with self.assertRaises(httpx.HTTPStatusError):
            await client.get("/fapi/v1/exchangeInfo")
        self.assertEqual(calls, client.MAX_ATTEMPTS)
        await client.aclose()

    async def test_retry_after_supports_http_date_and_ignores_invalid_value(self):
        future = datetime.now(timezone.utc) + timedelta(seconds=20)
        response = httpx.Response(429, headers={"Retry-After": format_datetime(future, usegmt=True)})
        self.assertGreaterEqual(_retry_after(response), 18)
        self.assertLessEqual(_retry_after(response), 20)
        invalid = httpx.Response(429, headers={"Retry-After":"not-a-date"})
        self.assertIsNone(_retry_after(invalid))
        self.assertEqual(_http_date("Thu Jan  1 00:01:00 1970").tzinfo, timezone.utc)

    async def test_418_ignores_undocumented_retry_after_and_suspends_permanently(self):
        client = await self._client(lambda req: httpx.Response(418, headers={"Retry-After":"20"}))
        with self.assertRaises(BinanceRateLimitError):
            await client.get("/fapi/v1/klines", params={"limit": 100})
        state = await self.limiter.status()
        self.assertTrue(state["suspended"])
        self.assertTrue(state["permanent"])
        self.assertEqual(state["status_code"], 418)
        await client.aclose()

    async def test_418_without_expiry_suspends_indefinitely_without_retrying(self):
        calls = 0
        def handler(request):
            nonlocal calls
            calls += 1
            return httpx.Response(418, json={"msg":"banned"})
        client = await self._client(handler)
        with self.assertRaises(BinanceRateLimitError):
            await client.get("/fapi/v1/klines", params={"limit": 100})
        with self.assertRaises(BinanceRateLimitError):
            await client.get("/fapi/v1/klines", params={"limit": 100})
        self.assertEqual(calls, 1)
        await client.aclose()

    async def test_manual_acknowledgement_keeps_suspension_until_explicit_resume(self):
        await self.limiter._reserve(1)
        reservations_before = len(self.limiter._events)
        await self.limiter.suspend(None, status_code=418)
        self.assertEqual((await self.limiter.status())["state"], "waiting_for_operator_acknowledgement")
        await self.limiter.operator_acknowledge()
        status = await self.limiter.status()
        self.assertTrue(status["permanent"])
        self.assertEqual(status["state"], "waiting_for_operator_resume")
        self.assertIsNotNone(status["operator_acknowledged_at"])
        with self.assertRaises(BinanceRateLimitError):
            await self.limiter._reserve(1)
        transitions = await asyncio.gather(
            self.limiter.operator_resume(), self.limiter.operator_resume(), return_exceptions=True
        )
        self.assertEqual(sum(item is None for item in transitions), 1)
        self.assertEqual(sum(isinstance(item, BinanceRateLimitError) for item in transitions), 1)
        status = await self.limiter.status()
        self.assertFalse(status["suspended"])
        self.assertEqual(status["state"], "active")
        self.assertIsNotNone(status["operator_reset_at"])
        self.assertEqual(len(self.limiter._events), reservations_before)
        await self.limiter._reserve(1)

    async def test_concurrent_requests_cannot_bypass_418_suspension(self):
        calls = 0
        def handler(request):
            nonlocal calls
            calls += 1
            return httpx.Response(418, json={"until": 9999999999999})
        client = await self._client(handler)
        with self.assertRaises(BinanceRateLimitError):
            await client.get("/fapi/v1/klines", params={"limit": 100})
        results = await asyncio.gather(
            *(client.get("/fapi/v1/klines", params={"limit": 100}) for _ in range(4)),
            return_exceptions=True,
        )
        self.assertTrue(all(isinstance(item, BinanceRateLimitError) for item in results))
        self.assertEqual(calls, 1)
        await client.aclose()

    async def test_network_backoff_is_bounded_and_loop_remains_responsive(self):
        calls = 0
        def handler(request):
            nonlocal calls
            calls += 1
            raise httpx.ConnectError("offline", request=request)
        client = await self._client(handler)
        # exchangeInfo bootstrap is the only endpoint permitted before configuration.
        with self.assertRaises(httpx.ConnectError):
            await client.get("/fapi/v1/exchangeInfo")
        self.assertEqual(calls, client.MAX_ATTEMPTS)
        self.assertEqual(len(self.time.delays), client.MAX_ATTEMPTS - 1)
        self.assertEqual(self.time.delays, [0.5, 1.0])
        self.assertEqual(len(self.limiter._events), client.MAX_ATTEMPTS)
        await client.aclose()

    async def test_retry_wait_does_not_block_event_loop(self):
        client = await self._client(lambda req: httpx.Response(429))
        progressed = asyncio.Event()
        async def heartbeat():
            await asyncio.sleep(0)
            progressed.set()
        request = asyncio.create_task(client.get("/fapi/v1/exchangeInfo"))
        await heartbeat()
        self.assertTrue(progressed.is_set())
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        await client.aclose()

    async def test_client_can_close_while_a_request_is_in_flight(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        async def handler(request):
            entered.set()
            await release.wait()
            return httpx.Response(200, json=RULES)
        client = await self._client(handler)
        request = asyncio.create_task(client.get("/fapi/v1/exchangeInfo"))
        await entered.wait()
        await client.aclose()
        release.set()
        response = await request
        self.assertEqual(response.status_code, 200)

    async def test_initial_history_and_ws_gap_calls_use_shared_rest_client(self):
        market = MarketData()
        try:
            market.symbols = ["BTCUSDT"]
            await market._rest.limiter.configure(RULES)
            seen = []
            def handler(request):
                seen.append(request.url.path)
                return httpx.Response(200, json=[])
            await market.close()
            market._rest = BinanceRestClient("https://unit.test", transport=httpx.MockTransport(handler),
                                             limiter=market._rest.limiter)
            with unittest.mock.patch("app.market.TIMEFRAMES", ("5m",)), \
                 unittest.mock.patch("app.market.asyncio.sleep", new=AsyncMock()):
                await market.history()
            market.bars["BTCUSDT"]["5m"].append({"t":0,"o":1,"h":1,"l":1,"c":1,"v":1,"closed":True})
            await market._fill_kline_gap("BTCUSDT", "5m", 600_000)
            self.assertEqual(seen, ["/fapi/v1/klines"] * 2)
            self.assertEqual(len(market._rest.limiter._events), 2)
        finally:
            await market.close()

    async def test_fastapi_lifespan_closes_rest_client_when_startup_fails(self):
        from app import main as api

        with patch.object(api.store, "connect", AsyncMock(side_effect=RuntimeError("DB unavailable"))), \
             patch.object(api.store, "pool", None), \
             patch.object(api.market, "close", AsyncMock()) as close_rest:
            with self.assertRaisesRegex(RuntimeError, "DB unavailable"):
                async with api.lifespan(None):
                    self.fail("Lifespan should not yield when store startup fails")
        close_rest.assert_awaited_once()

    async def test_operator_recovery_requires_loopback_and_configured_token(self):
        from app import main as api

        local_request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"),
                                        headers={"x-forwarded-for":"192.0.2.10"})
        ipv6_loopback_request = SimpleNamespace(client=SimpleNamespace(host="::1"), headers={})
        mapped_loopback_request = SimpleNamespace(client=SimpleNamespace(host="::ffff:127.0.0.1"), headers={})
        mapped_remote_request = SimpleNamespace(client=SimpleNamespace(host="::ffff:192.0.2.10"), headers={})
        remote_request = SimpleNamespace(client=SimpleNamespace(host="192.0.2.10"),
                                         headers={"x-forwarded-for":"127.0.0.1"})
        with patch("app.main.REST_OPERATOR_TOKEN", "secret-token"):
            api.require_rest_operator(local_request, "secret-token")
            api.require_rest_operator(ipv6_loopback_request, "secret-token")
            api.require_rest_operator(mapped_loopback_request, "secret-token")
            with self.assertRaises(HTTPException) as missing_token:
                api.require_rest_operator(local_request, None)
            self.assertEqual(missing_token.exception.status_code, 403)
            with self.assertRaises(HTTPException) as denied:
                with self.assertNoLogs("app.main", level="WARNING"):
                    api.require_rest_operator(local_request, "wrong-token")
            self.assertEqual(denied.exception.status_code, 403)
            self.assertNotIn("wrong-token", str(denied.exception.detail))
            with self.assertRaises(HTTPException) as denied_remote:
                api.require_rest_operator(remote_request, "secret-token")
            self.assertEqual(denied_remote.exception.status_code, 403)
            with self.assertRaises(HTTPException) as denied_mapped_remote:
                api.require_rest_operator(mapped_remote_request, "secret-token")
            self.assertEqual(denied_mapped_remote.exception.status_code, 403)
            with self.assertRaises(HTTPException) as disabled:
                with patch("app.main.REST_OPERATOR_TOKEN", ""):
                    api.require_rest_operator(local_request, "secret-token")
            self.assertEqual(disabled.exception.status_code, 503)
            health = await api.health()
            self.assertNotIn("secret-token", repr(health))
            self.assertNotIn("secret-token", repr(denied.exception.detail))
            routes = {(route.path, method) for route in api.app.routes for method in getattr(route, "methods", set())}
            self.assertIn(("/api/admin/rest-suspension/acknowledge", "POST"), routes)
            self.assertIn(("/api/admin/rest-suspension/resume", "POST"), routes)


if __name__ == "__main__":
    unittest.main()
