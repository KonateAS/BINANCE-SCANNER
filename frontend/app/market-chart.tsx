'use client';

import { useEffect, useRef, useState } from 'react';
import {
  CandlestickSeries, createChart, HistogramSeries, LineStyle,
  type IPriceLine, type MouseEventParams, type Time,
} from 'lightweight-charts';

type Bar = { t: number; o: number; h: number; l: number; c: number; v: number; closed?: boolean };
type HoverBar = { t: number; o: number; h: number; l: number; c: number; v: number };
type ChartState = 'loading' | 'ready' | 'error' | 'empty';
type StreamState = 'connecting' | 'live' | 'reconnecting';
type TradePlan = { entry: number; stop: number; target1: number; target2: number };
const API_WS = 'ws://127.0.0.1:8000';
const TIMEFRAME_MS: Record<string, number> = { '5m': 300_000, '15m': 900_000, '1h': 3_600_000, '4h': 14_400_000 };

function countdownLabel(milliseconds: number | null): string {
  if (milliseconds === null) return '--:--';
  const seconds = Math.ceil(Math.max(0, milliseconds) / 1000);
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainder = seconds % 60;
  const pad = (value: number) => String(value).padStart(2, '0');
  return hours > 0 ? `${pad(hours)}:${pad(minutes)}:${pad(remainder)}` : `${pad(minutes)}:${pad(remainder)}`;
}

function formatPrice(value: number): string {
  return new Intl.NumberFormat('fr-FR', { maximumSignificantDigits: 8 }).format(value);
}

function timestampSeconds(time: Time): number {
  if (typeof time === 'number') return time;
  if (typeof time === 'string') return Math.floor(Date.parse(time) / 1000);
  return Math.floor(Date.UTC(time.year, time.month - 1, time.day) / 1000);
}

function toHoverBar(bar: Bar): HoverBar {
  return { t: Math.floor(bar.t / 1000), o: bar.o, h: bar.h, l: bar.l, c: bar.c, v: bar.v };
}

function candlePoint(bar: Bar) {
  return { time: Math.floor(bar.t / 1000) as any, open: bar.o, high: bar.h, low: bar.l, close: bar.c };
}

function volumePoint(bar: Bar) {
  return { time: Math.floor(bar.t / 1000) as any, value: bar.v, color: bar.c >= bar.o ? '#31563d' : '#5a3338' };
}

export default function MarketChart({
  symbol, timeframe, plan,
}: { symbol: string; timeframe: string; plan: TradePlan | null }) {
  const host = useRef<HTMLDivElement>(null);
  const candleCloseAt = useRef<number | null>(null);
  const [state, setState] = useState<ChartState>('loading');
  const [streamState, setStreamState] = useState<StreamState>('connecting');
  const [countdown, setCountdown] = useState('--:--');
  const [latestBar, setLatestBar] = useState<HoverBar | null>(null);
  const [hoveredBar, setHoveredBar] = useState<HoverBar | null>(null);
  const entry = plan?.entry ?? null;
  const stop = plan?.stop ?? null;
  const target1 = plan?.target1 ?? null;
  const target2 = plan?.target2 ?? null;

  useEffect(() => {
    const tick = () => {
      const closeAt = candleCloseAt.current;
      setCountdown(countdownLabel(closeAt === null ? null : closeAt - Date.now()));
    };
    tick();
    const timer = setInterval(tick, 1000);
    return () => clearInterval(timer);
  }, [timeframe]);

  useEffect(() => {
    const element = host.current;
    if (!element) return;
    candleCloseAt.current = null;
    setCountdown('--:--');
    setLatestBar(null);
    setHoveredBar(null);
    setState('loading');
    setStreamState('connecting');
    const chart = createChart(element, {
      height: 300,
      layout: { background: { color: '#111614' }, textColor: '#9aa99f' },
      grid: { vertLines: { color: '#26312b' }, horzLines: { color: '#26312b' } },
      rightPriceScale: { borderColor: '#26312b' },
      timeScale: { borderColor: '#26312b', timeVisible: true },
      crosshair: { vertLine: { color: '#52634f' }, horzLine: { color: '#52634f' } },
    });
    const candles = chart.addSeries(CandlestickSeries, {
      upColor: '#7ce0a0', downColor: '#ff8e8e', borderVisible: false,
      wickUpColor: '#7ce0a0', wickDownColor: '#ff8e8e', priceLineVisible: false,
    });
    let currentPriceLine: IPriceLine | undefined;
    const addPlanLine = (price: number | null, title: string, color: string, lineStyle: LineStyle) => {
      if (price === null || !Number.isFinite(price) || price <= 0) return;
      candles.createPriceLine({ price, title, color, lineWidth: 1, lineStyle, axisLabelVisible: true });
    };
    if (entry !== null) {
      addPlanLine(entry, 'ENTRÉE', '#b7f77a', LineStyle.Dashed);
      addPlanLine(stop, 'STOP', '#ff8e8e', LineStyle.Dashed);
      addPlanLine(target1, 'TP1', '#7ce0a0', LineStyle.Dashed);
      addPlanLine(target2, 'TP2', '#b7f77a', LineStyle.Dotted);
    }
    const volume = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' }, priceScaleId: '', lastValueVisible: false, priceLineVisible: false,
    });
    chart.priceScale('').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

    const updatePriceAndClock = (bar: Bar) => {
      const interval = TIMEFRAME_MS[timeframe];
      candleCloseAt.current = bar.t + interval * (bar.closed ? 2 : 1);
      if (currentPriceLine) currentPriceLine.applyOptions({ price: bar.c });
      else currentPriceLine = candles.createPriceLine({
        price: bar.c, title: 'PRIX', color: '#b7f77a', lineWidth: 2,
        lineStyle: LineStyle.Solid, axisLabelVisible: true,
      });
      setLatestBar(toHoverBar(bar));
    };

    let alive = true;
    let hasFittedContent = false;
    let socket: WebSocket | undefined;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    const renderHistory = (bars: Bar[]) => {
      if (bars.length === 0) {
        candleCloseAt.current = null;
        setCountdown('--:--');
        setLatestBar(null);
        setState('empty');
        return;
      }
      candles.setData(bars.map(candlePoint));
      volume.setData(bars.map(volumePoint));
      updatePriceAndClock(bars[bars.length - 1]);
      if (!hasFittedContent) {
        chart.timeScale().fitContent();
        hasFittedContent = true;
      }
      setState('ready');
    };
    const handleCrosshairMove = (param: MouseEventParams<Time>) => {
      if (param.time === undefined) {
        setHoveredBar(null);
        return;
      }
      const candle = param.seriesData.get(candles);
      if (!candle || !('open' in candle)) {
        setHoveredBar(null);
        return;
      }
      const volumeData = param.seriesData.get(volume);
      setHoveredBar({
        t: timestampSeconds(param.time),
        o: candle.open, h: candle.high, l: candle.low, c: candle.close,
        v: volumeData && 'value' in volumeData ? volumeData.value : 0,
      });
    };
    chart.subscribeCrosshairMove(handleCrosshairMove);

    const connect = () => {
      if (!alive) return;
      setStreamState('connecting');
      socket = new WebSocket(`${API_WS}/ws/klines/${encodeURIComponent(symbol)}/${encodeURIComponent(timeframe)}`);
      socket.onopen = () => { if (alive) setStreamState('live'); };
      socket.onmessage = (event) => {
        if (!alive) return;
        try {
          const message: { type?: string; data?: Bar[] | Bar } = JSON.parse(event.data);
          if (message.type === 'history' && Array.isArray(message.data)) {
            renderHistory(message.data);
          } else if (message.type === 'bar' && message.data && !Array.isArray(message.data)) {
            const bar = message.data;
            candles.update(candlePoint(bar));
            volume.update(volumePoint(bar));
            updatePriceAndClock(bar);
            setState('ready');
          }
        } catch {
          // Ignore malformed frames; the next full history frame resynchronizes the chart.
        }
      };
      socket.onerror = () => socket?.close();
      socket.onclose = () => {
        if (!alive) return;
        setStreamState('reconnecting');
        reconnectTimer = setTimeout(connect, 1500);
      };
    };
    connect();

    const resize = new ResizeObserver((entries) => {
      const width = entries[0]?.contentRect.width;
      if (width) chart.applyOptions({ width });
    });
    resize.observe(element);
    return () => {
      alive = false;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      socket?.close();
      chart.unsubscribeCrosshairMove(handleCrosshairMove);
      resize.disconnect();
      chart.remove();
    };
  }, [symbol, timeframe, entry, stop, target1, target2]);

  const displayBar = hoveredBar ?? latestBar;
  const displayTime = displayBar ? new Date(displayBar.t * 1000).toLocaleString('fr-FR') : '—';
  return (
    <div className="chart-frame">
      <div ref={host} className="chart-host" />
      <div className="chart-ohlc" aria-live="off">
        <span>{hoveredBar ? displayTime : 'DERNIÈRE BOUGIE'}</span>
        {displayBar && <>
          <span>O&nbsp;{formatPrice(displayBar.o)}</span>
          <span>H&nbsp;{formatPrice(displayBar.h)}</span>
          <span>B&nbsp;{formatPrice(displayBar.l)}</span>
          <span>C&nbsp;{formatPrice(displayBar.c)}</span>
          <span>V&nbsp;{formatPrice(displayBar.v)}</span>
        </>}
      </div>
      <div className="chart-countdown" aria-live="off">Clôture&nbsp; {countdown}</div>
      <div className={`chart-stream ${streamState}`}>
        <i />{streamState === 'live' ? 'BINANCE LIVE' : 'RECONNEXION DU FLUX'}
      </div>
      {state !== 'ready' && (
        <div className="chart-state" role="status">
          {state === 'loading' && 'Connexion au flux Binance et chargement des bougies…'}
          {state === 'empty' && 'Aucune bougie disponible pour cette paire.'}
          {state === 'error' && 'Graphique indisponible — reconnexion automatique.'}
        </div>
      )}
    </div>
  );
}
