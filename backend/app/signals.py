"""Causal technical indicators and closed-candle signal analysis."""
from __future__ import annotations

import math
from typing import Iterable, Mapping, Any

import numpy as np

_TIMEFRAME_MS = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}


def _ema(values: Iterable[float], period: int) -> np.ndarray:
    """EMA with an SMA seed; invalid values break the series until a new seed."""
    x = np.asarray(list(values), dtype=float)
    out = np.full(x.shape, np.nan, dtype=float)
    if period < 1:
        raise ValueError("period must be positive")
    alpha = 2.0 / (period + 1.0)
    start = None
    for i, value in enumerate(x):
        if not math.isfinite(float(value)):
            start = None
            continue
        if start is None:
            start = i
        if i - start + 1 == period:
            out[i] = float(np.mean(x[start:i + 1]))
        elif i - start + 1 > period:
            out[i] = out[i - 1] + alpha * (value - out[i - 1])
    return out


def _wilder(values: Iterable[float], period: int) -> np.ndarray:
    """Wilder smoothing with an arithmetic-mean seed after `period` values."""
    x = np.asarray(list(values), dtype=float)
    out = np.full(x.shape, np.nan, dtype=float)
    if period < 1:
        raise ValueError("period must be positive")
    start = None
    for i, value in enumerate(x):
        if not math.isfinite(float(value)):
            start = None
            continue
        if start is None:
            start = i
        count = i - start + 1
        if count == period:
            out[i] = float(np.mean(x[start:i + 1]))
        elif count > period:
            out[i] = (out[i - 1] * (period - 1) + value) / period
    return out


def _rsi(closes: Iterable[float], period: int = 14) -> np.ndarray:
    close = np.asarray(list(closes), dtype=float)
    out = np.full(close.shape, np.nan, dtype=float)
    if period < 1:
        raise ValueError("period must be positive")
    delta = np.diff(close, prepend=np.nan)
    gains = np.where(np.isfinite(delta), np.maximum(delta, 0), np.nan)
    losses = np.where(np.isfinite(delta), np.maximum(-delta, 0), np.nan)
    avg_gain, avg_loss = _wilder(gains, period), _wilder(losses, period)
    for i in range(len(close)):
        if not (math.isfinite(avg_gain[i]) and math.isfinite(avg_loss[i])):
            continue
        if avg_gain[i] == 0 and avg_loss[i] == 0:
            out[i] = 50.0
        elif avg_loss[i] == 0:
            out[i] = 100.0
        elif avg_gain[i] == 0:
            out[i] = 0.0
        else:
            out[i] = 100.0 - 100.0 / (1.0 + avg_gain[i] / avg_loss[i])
    return out


def _macd_components(closes: Iterable[float]) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    close = np.asarray(list(closes), dtype=float)
    fast, slow = _ema(close, 12), _ema(close, 26)
    line = fast - slow
    line[~(np.isfinite(fast) & np.isfinite(slow))] = np.nan
    signal = _ema(line, 9)
    hist = line - signal
    hist[~(np.isfinite(line) & np.isfinite(signal))] = np.nan
    return line, signal, hist, fast, slow


def _macd(closes: Iterable[float]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    line, signal, hist, _, _ = _macd_components(closes)
    return line, signal, hist


def _atr(bars: list[dict[str, Any]], period: int = 14) -> np.ndarray:
    tr = np.full(len(bars), np.nan, dtype=float)
    previous_close = math.nan
    for i, bar in enumerate(bars):
        high, low, close = bar["h"], bar["l"], bar["c"]
        tr[i] = high - low if i == 0 else max(high - low, abs(high - previous_close), abs(low - previous_close))
        previous_close = close
    return _wilder(tr, period)


def _normalize(bars: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_time: dict[int, dict[str, Any]] = {}
    for source in bars:
        try:
            t = int(source["t"])
            values = {key: float(source[key]) for key in ("o", "h", "l", "c", "v")}
            if t < 0 or not all(math.isfinite(v) for v in values.values()):
                continue
            o, h, low, c, v = (values[k] for k in ("o", "h", "l", "c", "v"))
            if min(o, h, low, c) <= 0 or v < 0 or h < max(o, c, low) or low > min(o, c):
                continue
            closed = source.get("closed", True) is True
            bar = {"t": t, **values, "closed": closed}
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        old = by_time.get(t)
        if old is None or (bar["closed"] and not old["closed"]) or bar["closed"] == old["closed"]:
            by_time[t] = bar
    return [by_time[t] for t in sorted(by_time)]


def _volume_ratio(bars: list[dict[str, Any]], index: int, period: int = 20) -> float | None:
    """Current volume divided by the mean of the preceding closed observations."""
    if index < period:
        return None
    reference = [b["v"] for b in bars[index - period:index]]
    mean = float(np.mean(reference))
    if not math.isfinite(mean) or mean <= 0:
        return None
    ratio = bars[index]["v"] / mean
    return ratio if math.isfinite(ratio) else None


def series_status(raw_bars: Iterable[Mapping[str, Any]], timeframe: str) -> str:
    """Classify the closed series used by the indicator warm-up.

    `insufficient` means the latest contiguous run cannot satisfy the analyzer's
    35-bar minimum. `gapped` means the run is usable but older data has a gap.
    """
    interval = _TIMEFRAME_MS.get(timeframe)
    if interval is None:
        return "unknown_timeframe"
    closed = [bar for bar in _normalize(raw_bars) if bar["closed"]]
    if len(closed) < 35:
        return "insufficient"
    last_gap = None
    for i in range(len(closed) - 1, 0, -1):
        if closed[i]["t"] - closed[i - 1]["t"] != interval:
            last_gap = i
            break
    if last_gap is not None and len(closed) - last_gap < 35:
        return "insufficient"
    return "gapped" if last_gap is not None else "continuous"


def _divergences(bars: list[dict[str, Any]], rsi: np.ndarray, left: int = 3, right: int = 3) -> tuple[str | None, str | None]:
    """Return (confirmed divergence, detected-but-unconfirmed candidate).

    A pivot is confirmed only after `right` later closed bars. Candidate pivots
    use only left-side observations and are explicitly never treated as confirmed.
    """
    n = len(bars)
    lows = [i for i in range(left, max(left, n - right))
            if all(bars[i]["l"] < bars[j]["l"] for j in range(i-left, i))
            and all(bars[i]["l"] <= bars[j]["l"] for j in range(i+1, i+right+1))
            and math.isfinite(rsi[i])]
    highs = [i for i in range(left, max(left, n - right))
             if all(bars[i]["h"] > bars[j]["h"] for j in range(i-left, i))
             and all(bars[i]["h"] >= bars[j]["h"] for j in range(i+1, i+right+1))
             and math.isfinite(rsi[i])]
    confirmed = None
    if len(lows) >= 2:
        a, b = lows[-2:]
        if bars[b]["l"] < bars[a]["l"] and rsi[b] > rsi[a]:
            confirmed = "BULLISH"
    if len(highs) >= 2:
        a, b = highs[-2:]
        if bars[b]["h"] > bars[a]["h"] and rsi[b] < rsi[a]:
            confirmed = "BEARISH"

    candidate = None
    i = n - 1
    if n > left and math.isfinite(rsi[i]):
        if all(bars[i]["l"] < bars[j]["l"] for j in range(i-left, i)) and len(lows) >= 1:
            a = lows[-1]
            if bars[i]["l"] < bars[a]["l"] and rsi[i] > rsi[a]:
                candidate = "BULLISH"
        if all(bars[i]["h"] > bars[j]["h"] for j in range(i-left, i)) and len(highs) >= 1:
            a = highs[-1]
            if bars[i]["h"] > bars[a]["h"] and rsi[i] < rsi[a]:
                candidate = "BEARISH"
    return confirmed, candidate


def analyze_closed(raw_bars: Iterable[Mapping[str, Any]], timeframe: str | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Analyze finalized candles and return result, preview state, latest open bar."""
    bars = _normalize(raw_bars)
    closed = [bar for bar in bars if bar["closed"]]
    opens = [bar for bar in bars if not bar["closed"]]
    interval = _TIMEFRAME_MS.get(timeframe) if timeframe is not None else None
    if timeframe is not None and interval is None:
        return None, None, None
    if interval is not None:
        last_gap = None
        for i in range(len(closed) - 1, 0, -1):
            if closed[i]["t"] - closed[i - 1]["t"] != interval:
                last_gap = i
                break
        if last_gap is not None:
            closed = closed[last_gap:]
    if len(closed) < 35:
        return None, None, None
    c = np.asarray([b["c"] for b in closed], dtype=float)
    e9, e21 = _ema(c, 9), _ema(c, 21)
    rsi = _rsi(c, 14)
    macd, macd_signal, hist, ema12, ema26 = _macd_components(c)
    atr = _atr(closed, 14)
    ix = len(closed) - 1
    if not all(math.isfinite(x[ix]) for x in (e9, e21, rsi, macd, macd_signal, hist, atr)):
        return None, None, None
    bullish = e9[ix] > e21[ix]
    side = "LONG" if bullish else "SHORT"
    momentum = hist[ix] > 0 if bullish else hist[ix] < 0
    cross = (e9[ix-1] <= e21[ix-1] and e9[ix] > e21[ix]) if bullish else (e9[ix-1] >= e21[ix-1] and e9[ix] < e21[ix])
    ratio = _volume_ratio(closed, ix)
    score = 30 + (25 if cross else 0) + (20 if momentum else 0) + (15 if ratio is not None and ratio >= 1.5 else 0) + (10 if 30 <= rsi[ix] <= 70 else 0)
    confirmed = score >= 55 and (rsi[ix] < 75 if bullish else rsi[ix] > 25)
    divergence, candidate = _divergences(closed, rsi)
    result = {
        "side": side, "score": int(score), "confirmed": bool(confirmed), "pre_signal": False,
        "price": float(closed[-1]["c"]), "rsi": float(rsi[ix]), "ema9": float(e9[ix]), "ema21": float(e21[ix]),
        "macd_hist": float(hist[ix]), "atr": float(atr[ix]), "volume_ratio": ratio,
        "divergence": divergence, "divergence_detected": candidate is not None or divergence is not None,
        "divergence_candidate": candidate, "divergence_confirmed": divergence is not None,
        "timestamp": int(closed[-1]["t"]),
    }
    state = {"close": float(c[ix]), "ema9": float(e9[ix]), "ema21": float(e21[ix]),
             "ema12": float(ema12[ix]), "ema26": float(ema26[ix]),
             "macd_signal": float(macd_signal[ix]), "macd_hist": float(hist[ix]),
             "side": side, "confirmed": bool(confirmed), "timestamp": int(closed[-1]["t"])}
    latest_open = opens[-1] if opens and opens[-1]["t"] > closed[-1]["t"] else None
    return result, state, latest_open


def preview_signal(state: Mapping[str, Any] | None, open_bar: Mapping[str, Any] | None,
                   timeframe: str | None = None) -> bool:
    """Update the pre-signal using one open candle without recalculating history."""
    if not state or not open_bar or open_bar.get("closed", False):
        return False
    try:
        timestamp = int(open_bar["t"])
        close = float(open_bar["c"])
        if not math.isfinite(close):
            return False
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    interval = _TIMEFRAME_MS.get(timeframe) if timeframe is not None else None
    if timeframe is not None and interval is None:
        return False
    expected = state["timestamp"] + interval if interval is not None else None
    if timestamp <= state["timestamp"] or (expected is not None and timestamp != expected):
        return False

    ema9 = state["ema9"] + (2.0 / 10.0) * (close - state["ema9"])
    ema21 = state["ema21"] + (2.0 / 22.0) * (close - state["ema21"])
    ema12 = state["ema12"] + (2.0 / 13.0) * (close - state["ema12"])
    ema26 = state["ema26"] + (2.0 / 27.0) * (close - state["ema26"])
    macd = ema12 - ema26
    macd_signal = state["macd_signal"] + (2.0 / 10.0) * (macd - state["macd_signal"])
    hist = macd - macd_signal
    if state["side"] == "LONG":
        return ema9 > ema21 and hist > state["macd_hist"] and not state["confirmed"]
    return ema9 < ema21 and hist < state["macd_hist"] and not state["confirmed"]


def analyze(raw_bars: Iterable[Mapping[str, Any]], timeframe: str | None = None) -> dict[str, Any] | None:
    """Backward-compatible full analysis entry point."""
    result, state, open_bar = analyze_closed(raw_bars, timeframe)
    if result is None:
        return None
    result["pre_signal"] = preview_signal(state, open_bar, timeframe)
    return result
