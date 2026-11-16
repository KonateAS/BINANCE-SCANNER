"""Shared, bounded REST access to public Binance USDⓈ-M endpoints."""
import asyncio
import email.utils
import logging
import math
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)
_MAX_RESPONSE_DATE_SKEW_MS = 30_000  # 20s HTTP timeout plus a clock/date-resolution margin


class BinanceRestError(RuntimeError):
    pass


class BinanceRateLimitError(BinanceRestError):
    pass


@dataclass(frozen=True)
class RateRule:
    kind: str
    limit: int
    seconds: float


def _duration(interval: str, count: int) -> float:
    units = {"SECOND": 1, "MINUTE": 60, "HOUR": 3600, "DAY": 86400}
    if interval not in units or count < 1:
        raise BinanceRestError(f"Unsupported Binance rate-limit interval: {count} {interval}")
    return units[interval] * count


def _http_date(value):
    parsed = email.utils.parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class BinanceRateLimiter:
    """Atomic reservations aligned to Binance server-time rate-limit windows."""
    def __init__(self, *, clock=time.monotonic, sleep=asyncio.sleep):
        self._clock, self._sleep = clock, sleep
        self._lock = asyncio.Lock()
        self._rules = ()
        self._events = []  # (monotonic time, estimated server milliseconds, weight, raw count)
        self._server = {}  # (kind, duration, server-window index) -> (used, observed monotonic time, server date)
        self._pending_bootstrap = 0
        self._server_anchor_ms = None
        self._monotonic_anchor = None
        self._clock_source = "unconfigured"
        self._cooldown_until = 0.0
        self._permanent_suspend = False
        self._suspended_at = None
        self._suspended_at_utc = None
        self._suspension_status = None
        self._operator_acknowledged_at = None
        self._operator_reset_at = None
        self._recovery_admissions = 0
        self._next_recovery_admission = 0.0

    @property
    def configured(self):
        return bool(self._rules)

    def _server_now_ms(self, monotonic_now=None):
        if self._server_anchor_ms is None:
            return time.time() * 1000
        now = self._clock() if monotonic_now is None else monotonic_now
        return self._server_anchor_ms + (now - self._monotonic_anchor) * 1000

    async def configure(self, payload):
        rules = []
        for item in payload.get("rateLimits", []):
            kind = item.get("rateLimitType")
            if kind not in ("REQUEST_WEIGHT", "RAW_REQUEST", "RAW_REQUESTS"):
                continue
            if kind == "RAW_REQUESTS":
                kind = "RAW_REQUEST"
            rules.append(RateRule(kind, int(item["limit"]),
                                  _duration(item["interval"], int(item["intervalNum"]))))
        if not rules:
            raise BinanceRestError("exchangeInfo did not provide REQUEST_WEIGHT or RAW_REQUEST limits")
        try:
            server_ms = float(payload["serverTime"])
            if not math.isfinite(server_ms) or server_ms <= 0:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            server_ms = time.time() * 1000
            clock_source = "local_utc_fallback"
            log.warning("exchangeInfo did not provide a usable serverTime; local UTC is used for window alignment")
        else:
            clock_source = "binance_exchange_info_server_time"
        async with self._lock:
            now = self._clock()
            self._server_anchor_ms, self._monotonic_anchor = server_ms, now
            self._clock_source = clock_source
            if self._pending_bootstrap:
                self._events.append((now, server_ms, self._pending_bootstrap, self._pending_bootstrap))
                self._pending_bootstrap = 0
            self._rules = tuple(rules)

    async def _reserve(self, weight, *, bootstrap=False):
        if not self._rules:
            if not bootstrap:
                raise BinanceRestError("Binance REST limiter is not configured; call exchangeInfo first")
            while True:
                async with self._lock:
                    now = self._clock()
                    if self._permanent_suspend:
                        raise BinanceRateLimitError("REST suspended after HTTP 418 without a known expiry")
                    delay = self._cooldown_until - now
                    if delay <= 0:
                        self._pending_bootstrap += 1
                        return
                await self._sleep(delay)

        while True:
            async with self._lock:
                now = self._clock()
                if self._permanent_suspend:
                    raise BinanceRateLimitError("REST suspended after HTTP 418 without a known expiry")
                server_now = self._server_now_ms(now)
                max_duration = max(rule.seconds for rule in self._rules)
                self._events = [event for event in self._events
                                if event[1] > server_now - max_duration * 1000]
                self._server = {key: value for key, value in self._server.items()
                                if key[2] >= int(server_now // (key[1] * 1000))}
                wake_monotonic = max(now, self._cooldown_until, self._next_recovery_admission
                                     if self._recovery_admissions else now)
                next_boundary_ms = server_now
                # Binance publishes interval lengths and serverTime, but not a
                # formal local synchronization algorithm; epoch-floor alignment
                # is this limiter's explicit approximation of those windows.
                for rule in self._rules:
                    if rule.limit <= 0:
                        raise BinanceRestError(f"Invalid Binance rate-limit value for {rule.kind}")
                    cost = weight if rule.kind == "REQUEST_WEIGHT" else 1
                    if cost > rule.limit:
                        raise BinanceRateLimitError(f"Request cost {cost} exceeds Binance limit {rule.limit}")
                    window = int(server_now // (rule.seconds * 1000))
                    relevant = [event for event in self._events
                                if int(event[1] // (rule.seconds * 1000)) == window]
                    local = sum(event[2] if rule.kind == "REQUEST_WEIGHT" else event[3]
                                for event in relevant)
                    server = self._server.get((rule.kind, rule.seconds, window))
                    observed = 0
                    if server:
                        post_snapshot = sum(
                            event[2] if rule.kind == "REQUEST_WEIGHT" else event[3]
                            for event in relevant if event[0] >= server[1]
                        )
                        observed = server[0] + post_snapshot
                    if max(local, observed) + cost > rule.limit:
                        next_boundary_ms = max(next_boundary_ms,
                                               (window + 1) * rule.seconds * 1000)
                delay_to_boundary = max(0.0, (next_boundary_ms - server_now) / 1000)
                wake_monotonic = max(wake_monotonic, now + delay_to_boundary)
                if wake_monotonic <= now:
                    if self._recovery_admissions:
                        spacing = max(rule.seconds * (weight if rule.kind == "REQUEST_WEIGHT" else 1) /
                                      rule.limit for rule in self._rules)
                        self._next_recovery_admission = now + spacing
                        self._recovery_admissions -= 1
                    self._events.append((now, server_now, weight, 1))
                    return
                delay = max(0.001, wake_monotonic - now)
            await self._sleep(delay)

    async def reconcile(self, headers):
        now = self._clock()
        estimated_server_ms = self._server_now_ms(now)
        server_ms = estimated_server_ms
        date_value = headers.get("Date") or headers.get("date")
        if date_value:
            try:
                candidate_ms = _http_date(date_value).timestamp() * 1000
                if abs(candidate_ms - estimated_server_ms) <= _MAX_RESPONSE_DATE_SKEW_MS:
                    server_ms = candidate_ms
            except (TypeError, ValueError, OverflowError):
                pass
        async with self._lock:
            for name, value in headers.items():
                match = re.fullmatch(r"x-mbx-used-weight-(\d+)([smhd])", name.lower())
                if not match:
                    continue
                duration = int(match.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match.group(2)]
                try:
                    used = int(value)
                except (TypeError, ValueError):
                    continue
                if used < 0:
                    continue
                window = int(server_ms // (duration * 1000))
                key = ("REQUEST_WEIGHT", float(duration), window)
                previous = self._server.get(key)
                if previous:
                    if server_ms < previous[2] or used < previous[0]:
                        continue
                    if server_ms == previous[2]:
                        # HTTP Date has one-second precision. Same-second
                        # responses may be reordered; retain the earlier local
                        # snapshot point so concurrent reservations stay counted.
                        self._server[key] = (max(used, previous[0]), previous[1], previous[2])
                        continue
                self._server[key] = (used, now, server_ms)

    async def suspend(self, seconds=None, *, status_code=None):
        async with self._lock:
            self._suspended_at = self._clock()
            self._suspended_at_utc = datetime.now(timezone.utc).isoformat()
            self._suspension_status = status_code
            if seconds is None:
                self._permanent_suspend = True
                self._operator_acknowledged_at = None
                self._operator_reset_at = None
            else:
                self._cooldown_until = max(self._cooldown_until, self._clock() + max(0, seconds))
                self._recovery_admissions = max(self._recovery_admissions, 32)
                self._next_recovery_admission = max(self._next_recovery_admission, self._cooldown_until)

    async def operator_acknowledge(self):
        async with self._lock:
            if not self._permanent_suspend:
                raise BinanceRateLimitError("No unresolved permanent REST suspension exists")
            self._operator_acknowledged_at = datetime.now(timezone.utc).isoformat()

    async def operator_resume(self):
        async with self._lock:
            if not self._permanent_suspend or not self._operator_acknowledged_at:
                raise BinanceRateLimitError("A permanent suspension must be acknowledged before manual resume")
            reset_at = datetime.now(timezone.utc).isoformat()
            self._permanent_suspend = False
            self._cooldown_until = max(self._cooldown_until, self._clock())
            self._recovery_admissions = max(self._recovery_admissions, 32)
            self._next_recovery_admission = max(self._next_recovery_admission, self._clock())
            self._operator_reset_at = reset_at

    async def status(self):
        async with self._lock:
            now = self._clock()
            if self._permanent_suspend:
                state = "waiting_for_operator_acknowledgement" if not self._operator_acknowledged_at else "waiting_for_operator_resume"
            elif now < self._cooldown_until:
                state = "temporary_cooldown"
            else:
                state = "active"
            return {
                "state": state,
                "configured": bool(self._rules),
                "window_clock": self._clock_source,
                "suspended": self._permanent_suspend or now < self._cooldown_until,
                "permanent": self._permanent_suspend,
                "status_code": self._suspension_status,
                "suspended_at": self._suspended_at_utc,
                "cooldown_remaining_seconds": max(0.0, self._cooldown_until - now),
                "operator_acknowledged_at": self._operator_acknowledged_at,
                "operator_reset_at": self._operator_reset_at,
            }


def _weight(path, params):
    params = params or {}
    if path == "/fapi/v1/exchangeInfo":
        return 1
    if path == "/fapi/v1/klines":
        limit = int(params.get("limit", 500))
        return 1 if limit < 100 else 2 if limit < 500 else 5 if limit <= 1000 else 10
    if path == "/fapi/v1/ticker/24hr":
        return 1 if params.get("symbol") else 40
    raise BinanceRestError(f"No documented weight mapping for Binance endpoint {path}")


def _retry_after(response):
    value = response.headers.get("Retry-After")
    if value:
        # RFC 9110 delay-seconds is a non-negative decimal integer, not a float.
        if value.isascii() and value.isdigit():
            return int(value)
        try:
            return max(0, (_http_date(value) - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            pass
    return None


class BinanceRestClient:
    MAX_ATTEMPTS = 3

    def __init__(self, base_url, *, transport=None, timeout=20, limiter=None,
                 sleep=asyncio.sleep, jitter=random.uniform):
        self.limiter = limiter or BinanceRateLimiter(sleep=sleep)
        self._sleep, self._jitter = sleep, jitter
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout, transport=transport)
        self._bootstrap_lock = asyncio.Lock()

    async def get(self, path, params=None):
        if path == "/fapi/v1/exchangeInfo" and not self.limiter.configured:
            # Serialize the only request that can teach this process its limits.
            async with self._bootstrap_lock:
                if not self.limiter.configured:
                    return await self._get(path, params, bootstrap=True)
        return await self._get(path, params)

    async def _get(self, path, params=None, *, bootstrap=False):
        weight = _weight(path, params)
        if path != "/fapi/v1/exchangeInfo" and not self.limiter.configured:
            raise BinanceRestError("exchangeInfo must configure the shared limiter before REST market calls")
        last_error = None
        for attempt in range(self.MAX_ATTEMPTS):
            await self.limiter._reserve(weight, bootstrap=bootstrap)
            try:
                response = await self._client.get(path, params=params)
            except httpx.TransportError as exc:
                last_error = exc
                if attempt + 1 == self.MAX_ATTEMPTS:
                    raise
                await self._sleep(min(8, 0.5 * 2**attempt) + self._jitter(0, 0.25))
                continue
            await self.limiter.reconcile(response.headers)
            if response.status_code == 418:
                # Binance's USDⓈ-M docs do not define a ban-expiry field. Keep
                # this unresolved until an operator explicitly resets it.
                await self.limiter.suspend(None, status_code=418)
                raise BinanceRateLimitError("Binance returned HTTP 418; shared REST client suspended")
            if response.status_code == 429:
                delay = _retry_after(response)
                if delay is None:
                    delay = min(30, 1 * 2**attempt) + self._jitter(0, 0.5)
                await self.limiter.suspend(delay, status_code=429)
                if attempt + 1 == self.MAX_ATTEMPTS:
                    response.raise_for_status()
                await self._sleep(delay)
                continue
            if response.status_code >= 500 and attempt + 1 < self.MAX_ATTEMPTS:
                await self._sleep(min(8, 0.5 * 2**attempt) + self._jitter(0, 0.25))
                continue
            response.raise_for_status()
            if path == "/fapi/v1/exchangeInfo" and not self.limiter.configured:
                await self.limiter.configure(response.json())
            return response
        if last_error:
            raise last_error
        raise BinanceRestError("Binance REST attempts exhausted")

    async def aclose(self):
        await self._client.aclose()
