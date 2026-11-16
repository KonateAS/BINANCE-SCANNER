"""Offline deterministic tests for indicator math and candle semantics."""
import math
import unittest

import numpy as np

from app.signals import _atr, _divergences, _ema, _macd, _macd_components, _normalize, _rsi, _volume_ratio, analyze, analyze_closed, preview_signal, series_status


def bars_from_closes(closes, volumes=None, start=0):
    volumes = volumes or [100.0] * len(closes)
    return [{"t": start + i, "o": float(c), "h": float(c) + 1,
             "l": float(c) - 1, "c": float(c), "v": float(volumes[i]), "closed": True}
            for i, c in enumerate(closes)]


class IndicatorMathTests(unittest.TestCase):
    def test_ema_sma_seed_constant_and_rising(self):
        np.testing.assert_allclose(_ema([7] * 4, 3)[2:], [7, 7])
        got = _ema([1, 2, 3, 4], 3)
        self.assertTrue(math.isnan(got[1]))
        self.assertEqual(got[2], 2)
        self.assertAlmostEqual(got[3], 3)
        for period in (9, 21):
            series = [float(i) for i in range(1, period + 2)]
            values = _ema(series, period)
            seed = (period + 1) / 2
            self.assertTrue(np.isnan(values[period - 2]))
            self.assertAlmostEqual(values[period - 1], seed)
            self.assertAlmostEqual(values[period], seed + (2 / (period + 1)) * (period + 1 - seed))

    def test_rsi_wilder_flat_rising_falling(self):
        self.assertEqual(_rsi([5] * 20, 14)[-1], 50)
        self.assertEqual(_rsi(list(range(20)), 14)[-1], 100)
        self.assertEqual(_rsi(list(range(20, 0, -1)), 14)[-1], 0)
        self.assertTrue(np.isnan(_rsi([1] * 14, 14)[-1]))
        # Three deltas: +1, -1, +2 => mean gain 1, mean loss 1/3 => RSI 75.
        self.assertAlmostEqual(_rsi([10, 11, 10, 12], 3)[-1], 75)

    def test_macd_warmup_constant_and_progressive(self):
        line, signal, hist = _macd([10] * 50)
        self.assertAlmostEqual(line[-1], 0)
        self.assertAlmostEqual(signal[-1], 0)
        self.assertAlmostEqual(hist[-1], 0)
        _, _, rising = _macd([float(i * i) for i in range(80)])
        self.assertTrue(np.isfinite(rising[-1]))
        self.assertGreater(rising[-1], 0)
        rising_line, _, _ = _macd([float(i * i) for i in range(80)])
        _, rising_signal, _ = _macd([float(i * i) for i in range(80)])
        self.assertTrue(np.isnan(rising_line[24]))
        self.assertTrue(np.isfinite(rising_line[25]))
        self.assertTrue(np.isnan(rising_signal[32]))
        self.assertTrue(np.isfinite(rising_signal[33]))

    def test_atr_true_range_and_wilder_seed(self):
        synthetic = [
            {"h": 12., "l": 9., "c": 10.},  # TR 3
            {"h": 13., "l": 10., "c": 12.}, # TR 3
            {"h": 14., "l": 11., "c": 13.}, # TR 3
            {"h": 18., "l": 12., "c": 17.}, # TR 6
        ]
        atr = _atr(synthetic, 3)
        self.assertTrue(np.isnan(atr[1]))
        self.assertAlmostEqual(atr[2], 3)
        self.assertAlmostEqual(atr[3], 4)
        self.assertTrue(np.isnan(_atr(synthetic[:2], 3)[-1]))

    def test_volume_reference_excludes_current_and_zero_mean(self):
        candles = bars_from_closes([10] * 21, [100] * 20 + [1000])
        self.assertEqual(_volume_ratio(candles, 20), 10)
        future_changed = [dict(bar) for bar in candles]
        future_changed[20]["v"] = 2000
        future_changed.append(dict(candles[-1], t=21, v=1_000_000))
        self.assertEqual(_volume_ratio(future_changed, 20), 20)
        candles[:20] = bars_from_closes([10] * 20, [0] * 20)
        self.assertIsNone(_volume_ratio(candles, 20))

    def test_divergence_requires_two_pivots_and_is_confirmed_by_right_bars(self):
        lows = [10, 8, 10, 9, 7, 9, 10]
        candles = [{"l": x, "h": 20 + i, "c": 15} for i, x in enumerate(lows)]
        rsi = np.array([40, 30, 40, 45, 35, 45, 50], dtype=float)
        confirmed, candidate = _divergences(candles, rsi, left=1, right=1)
        self.assertEqual(confirmed, "BULLISH")
        self.assertIsNone(candidate)

        hi = [10, 12, 10, 11, 13, 11, 10]
        candles = [{"l": 5, "h": x, "c": 10} for x in hi]
        rsi = np.array([60, 70, 60, 55, 65, 50, 45], dtype=float)
        confirmed, _ = _divergences(candles, rsi, left=1, right=1)
        self.assertEqual(confirmed, "BEARISH")

    def test_latest_unconfirmed_pivot_is_candidate_not_confirmation(self):
        candles = [{"l": x, "h": 20, "c": 10} for x in [10, 8, 10, 9, 7]]
        rsi = np.array([40., 30., 40., 45., 40.])
        confirmed, candidate = _divergences(candles, rsi, left=1, right=1)
        self.assertIsNone(confirmed)
        self.assertEqual(candidate, "BULLISH")

    def test_insufficient_data_invalid_values_and_nonfinite_outputs(self):
        self.assertIsNone(analyze(bars_from_closes([100 + i for i in range(34)])))
        source = bars_from_closes([100 + i for i in range(40)])
        source[4]["c"] = float("nan")
        source[5]["h"] = float("inf")
        source[6]["v"] = -1
        result = analyze(source)
        self.assertIsNotNone(result)
        for key in ("price", "rsi", "ema9", "ema21", "macd_hist", "atr"):
            self.assertTrue(math.isfinite(result[key]), key)

    def test_bars_sort_deduplicate_and_prefer_closed(self):
        canonical = bars_from_closes([100 + i for i in range(40)])
        unordered = list(reversed(canonical))
        open_duplicate = dict(canonical[10], c=999, closed=False)
        unordered.append(open_duplicate)
        normalized = _normalize(unordered)
        self.assertEqual([b["t"] for b in normalized], list(range(40)))
        self.assertEqual(normalized[10]["c"], canonical[10]["c"])
        self.assertEqual(analyze(unordered), analyze(canonical))

    def test_confirmed_result_ignores_open_bar_but_preview_can_use_it(self):
        closed = bars_from_closes([100 + i * 0.1 for i in range(60)])
        baseline = analyze(closed)
        opened = dict(closed[-1], t=60, o=106, c=120, h=121, l=105, closed=False)
        with_open = analyze(closed + [opened])
        for field in ("side", "score", "confirmed", "price", "rsi", "ema9", "ema21", "macd_hist", "atr", "timestamp"):
            self.assertEqual(with_open[field], baseline[field], field)
        self.assertFalse(baseline["pre_signal"])
        self.assertTrue(with_open["pre_signal"])
        closed_result, state, open_bar = analyze_closed(closed + [opened])
        self.assertTrue(preview_signal(state, open_bar))
        self.assertFalse(closed_result["pre_signal"])
        closes = np.array([bar["c"] for bar in closed + [opened]])
        full_ema9, full_ema21 = _ema(closes, 9), _ema(closes, 21)
        _, _, full_hist = _macd(closes)
        expected = full_ema9[-1] > full_ema21[-1] and full_hist[-1] > _macd([bar["c"] for bar in closed])[2][-1]
        self.assertEqual(with_open["pre_signal"], bool(expected and not with_open["confirmed"]))

    def test_incremental_preview_matches_full_ema_and_macd_across_candle_lifecycle(self):
        interval = 300_000
        closes = [100 + i * 0.07 + ((i % 7) - 3) * 0.04 for i in range(60)]
        closed = bars_from_closes(closes)
        for i, bar in enumerate(closed):
            bar["t"] = i * interval

        def compare(open_bar):
            result, state, normalized_open = analyze_closed(closed + [open_bar], "5m")
            self.assertIsNotNone(result)
            self.assertIsNotNone(state)
            full_closes = np.asarray(closes + [open_bar["c"]], dtype=float)
            full_ema9, full_ema21 = _ema(full_closes, 9), _ema(full_closes, 21)
            full_macd, full_signal, full_hist, full_ema12, full_ema26 = _macd_components(full_closes)
            _, closed_signal, closed_hist, closed_ema12, closed_ema26 = _macd_components(closes)

            self.assertEqual(state["ema9"], float(_ema(closes, 9)[-1]))
            self.assertEqual(state["ema21"], float(_ema(closes, 21)[-1]))
            self.assertEqual(state["ema12"], float(closed_ema12[-1]))
            self.assertEqual(state["ema26"], float(closed_ema26[-1]))
            self.assertEqual(state["macd_signal"], float(closed_signal[-1]))
            self.assertEqual(state["macd_hist"], float(closed_hist[-1]))
            self.assertEqual(float(full_ema9[-1]), state["ema9"] + (2.0 / 10.0) * (open_bar["c"] - state["ema9"]))
            self.assertEqual(float(full_ema21[-1]), state["ema21"] + (2.0 / 22.0) * (open_bar["c"] - state["ema21"]))
            self.assertEqual(float(full_ema12[-1]), state["ema12"] + (2.0 / 13.0) * (open_bar["c"] - state["ema12"]))
            self.assertEqual(float(full_ema26[-1]), state["ema26"] + (2.0 / 27.0) * (open_bar["c"] - state["ema26"]))
            open_macd = float(full_macd[-1])
            expected_signal = state["macd_signal"] + (2.0 / 10.0) * (open_macd - state["macd_signal"])
            self.assertEqual(float(full_signal[-1]), expected_signal)
            self.assertEqual(float(full_hist[-1]), open_macd - expected_signal)

            if open_bar["t"] != state["timestamp"] + interval:
                self.assertFalse(preview_signal(state, normalized_open, "5m"))
                return
            bullish = state["side"] == "LONG"
            expected_preview = (
                (float(full_ema9[-1]) > float(full_ema21[-1]) and float(full_hist[-1]) > state["macd_hist"])
                if bullish else
                (float(full_ema9[-1]) < float(full_ema21[-1]) and float(full_hist[-1]) < state["macd_hist"])
            ) and not state["confirmed"]
            self.assertEqual(preview_signal(state, normalized_open, "5m"), expected_preview)

        opened = dict(closed[-1], t=closed[-1]["t"] + interval, c=closes[-1],
                      h=closes[-1] + 1, l=closes[-1] - 1, closed=False)
        for close in (closes[-1] * 0.99, closes[-1] * 1.01, closes[-1] * 1.08):
            opened = dict(opened, c=close, h=max(opened["o"], close) + 1,
                          l=min(opened["o"], close) - 1)
            compare(opened)

        opened = dict(opened, t=opened["t"] + interval)
        compare(opened)

        finalized = dict(opened, t=closed[-1]["t"] + interval, closed=True)
        final_result, final_state, final_open = analyze_closed(closed + [finalized], "5m")
        self.assertIsNotNone(final_result)
        self.assertIsNone(final_open)
        self.assertFalse(preview_signal(final_state, finalized, "5m"))
        closed.append(finalized)
        closes.append(finalized["c"])
        next_open = dict(finalized, t=finalized["t"] + interval, c=finalized["c"] * 1.02,
                         h=finalized["c"] * 1.02 + 1, l=min(finalized["o"], finalized["c"] * 1.02) - 1, closed=False)
        compare(next_open)

    def test_incremental_preview_warmup_and_insufficient_closed_data(self):
        for count in (34, 35):
            closed = bars_from_closes([100 + i * 0.2 for i in range(count)])
            interval = 300_000
            for i, bar in enumerate(closed):
                bar["t"] = i * interval
            opened = dict(closed[-1], t=closed[-1]["t"] + interval,
                          c=closed[-1]["c"] + 0.1, h=closed[-1]["c"] + 1,
                          l=closed[-1]["c"] - 1, closed=False)
            result, state, open_bar = analyze_closed(closed + [opened], "5m")
            if count == 34:
                self.assertIsNone(result)
                self.assertIsNone(state)
                self.assertFalse(preview_signal(state, open_bar, "5m"))
            else:
                self.assertIsNotNone(result)
                self.assertIsNotNone(state)
                self.assertEqual(state["ema9"], float(_ema([bar["c"] for bar in closed], 9)[-1]))
                self.assertEqual(state["ema21"], float(_ema([bar["c"] for bar in closed], 21)[-1]))

    def test_timeframe_gap_classification_and_contiguous_suffix(self):
        interval = 300_000
        candles = bars_from_closes([100 + i * 0.1 for i in range(70)])
        for i, bar in enumerate(candles):
            bar["t"] = i * interval
        self.assertEqual(series_status(candles, "5m"), "continuous")
        self.assertIsNotNone(analyze(candles, timeframe="5m"))

        # One missing timestamp leaves a sufficient latest contiguous run.
        gapped = [dict(bar) for bar in candles]
        for bar in gapped[20:]:
            bar["t"] += interval
        self.assertEqual(series_status(gapped, "5m"), "gapped")
        self.assertEqual(analyze(gapped, timeframe="5m"), analyze(gapped[20:], timeframe="5m"))

        # A recent gap leaves fewer than 35 consecutive closed candles.
        recent_gap = [dict(bar) for bar in candles]
        for bar in recent_gap[40:]:
            bar["t"] += interval
        self.assertEqual(series_status(recent_gap, "5m"), "insufficient")
        self.assertIsNone(analyze(recent_gap, timeframe="5m"))
        self.assertEqual(series_status(candles, "2m"), "unknown_timeframe")

    def test_nonadjacent_open_bar_does_not_create_preview(self):
        interval = 300_000
        closed = bars_from_closes([100 + i * 0.1 for i in range(60)])
        for i, bar in enumerate(closed):
            bar["t"] = i * interval
        stale_open = dict(closed[-1], t=61 * interval, o=106, c=120, h=121, l=105, closed=False)
        result = analyze(closed + [stale_open], timeframe="5m")
        self.assertFalse(result["pre_signal"])

    def test_result_contract_matches_api_frontend_consumed_fields(self):
        result = analyze(bars_from_closes([100 + i * 0.1 for i in range(60)]), timeframe=None)
        self.assertIsNotNone(result)
        for key in ("side", "score", "confirmed", "pre_signal", "price", "rsi", "atr", "volume_ratio", "divergence", "timestamp"):
            self.assertIn(key, result)
        self.assertIn(result["side"], ("LONG", "SHORT"))
        self.assertIsInstance(result["score"], int)
        self.assertIsInstance(result["confirmed"], bool)
        self.assertIsInstance(result["pre_signal"], bool)
        self.assertIsInstance(result["price"], float)
        self.assertTrue(result["volume_ratio"] is None or isinstance(result["volume_ratio"], float))
        self.assertTrue(result["divergence"] is None or isinstance(result["divergence"], str))


if __name__ == "__main__":
    unittest.main()
