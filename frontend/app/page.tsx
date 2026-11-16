'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import MarketChart from './market-chart';

type Row = {
  symbol: string; timeframe: string; side: 'LONG' | 'SHORT'; score: number;
  confirmed: boolean; pre_signal: boolean; price: number; change_24h: number | null;
  rsi: number; atr: number | null; volume_ratio: number | null; divergence: string | null;
  timestamp: number;
};
type Health = { status?: string; symbols?: number; database?: boolean };
type ApiState = 'connecting' | 'online' | 'offline';
type TradePlan = { entry: number; stop: number; target1: number; target2: number };

const API = 'http://127.0.0.1:8000';
const TIMEFRAMES = ['5m', '15m', '1h', '4h'];
const MODES = [
  ['ALL', 'Tout'], ['CONFIRMED', 'Confirmés'], ['PRE', 'Pré-signaux'],
  ['LONG', 'Long'], ['SHORT', 'Short'],
];

function buildTradePlan(row: Row): TradePlan | null {
  const atr = row.atr;
  if ((!row.confirmed && !row.pre_signal) || !Number.isFinite(row.price) || row.price <= 0 ||
      typeof atr !== 'number' || !Number.isFinite(atr) || atr <= 0) return null;
  const direction = row.side === 'LONG' ? 1 : -1;
  const risk = atr * 1.5;
  return {
    entry: row.price,
    stop: row.price - direction * risk,
    target1: row.price + direction * risk,
    target2: row.price + direction * risk * 2,
  };
}

function formatPrice(value: number): string {
  return new Intl.NumberFormat('fr-FR', { maximumSignificantDigits: 8 }).format(value);
}

export default function Home() {
  const [rows, setRows] = useState<Row[]>([]);
  const [health, setHealth] = useState<Health>({});
  const [apiState, setApiState] = useState<ApiState>('connecting');
  const [timeframe, setTimeframe] = useState('ALL');
  const [mode, setMode] = useState('ALL');
  const [query, setQuery] = useState('');
  const [connected, setConnected] = useState(false);
  const [selected, setSelected] = useState<Row | null>(null);
  const [snapshotAt, setSnapshotAt] = useState<string | null>(null);
  const chartRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (selected) chartRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, [selected]);

  useEffect(() => {
    let disposed = false;
    let socket: WebSocket | undefined;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

    const pollHealth = async () => {
      try {
        const response = await fetch(`${API}/api/health`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data: Health = await response.json();
        if (!disposed) {
          setHealth(data);
          setApiState('online');
        }
      } catch {
        if (!disposed) setApiState('offline');
      }
    };
    void pollHealth();
    const healthTimer = setInterval(() => void pollHealth(), 5000);

    const connect = () => {
      if (disposed) return;
      socket = new WebSocket('ws://127.0.0.1:8000/ws');
      socket.onopen = () => { if (!disposed) setConnected(true); };
      socket.onmessage = (event) => {
        if (disposed) return;
        try {
          const message = JSON.parse(event.data);
          if (message.type === 'snapshot' && Array.isArray(message.data)) {
            setRows(message.data);
            setSnapshotAt(message.updated_at ?? null);
          }
        } catch {
          // Ignore malformed frames; the close handler reports a disconnect.
        }
      };
      socket.onerror = () => socket?.close();
      socket.onclose = () => {
        if (disposed) return;
        setConnected(false);
        reconnectTimer = setTimeout(connect, 2000);
      };
    };
    connect();

    return () => {
      disposed = true;
      clearInterval(healthTimer);
      if (reconnectTimer) clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, []);

  const visible = useMemo(() => rows
    .filter((row) =>
      (timeframe === 'ALL' || row.timeframe === timeframe) &&
      (mode === 'ALL' ||
        (mode === 'CONFIRMED' && row.confirmed) ||
        (mode === 'PRE' && row.pre_signal) ||
        (mode === 'LONG' && row.side === 'LONG') ||
        (mode === 'SHORT' && row.side === 'SHORT')) &&
      row.symbol.includes(query.trim().toUpperCase()))
    .sort((a, b) => Number(b.confirmed) - Number(a.confirmed) || b.score - a.score),
  [rows, timeframe, mode, query]);

  const confirmedCount = rows.filter((row) => row.confirmed).length;
  const previewCount = rows.filter((row) => row.pre_signal && !row.confirmed).length;
  const selectedPlan = selected ? buildTradePlan(selected) : null;
  const connectionLabel = apiState === 'offline'
    ? 'API LOCALE INDISPONIBLE'
    : connected ? 'FLUX TEMPS RÉEL' : 'CONNEXION…';

  return (
    <main className="shell">
      <header className="topbar">
        <div className="topbar-inner">
          <div className="brand"><span className="brand-mark" aria-hidden="true">⌁</span> BINANCE SCANNER <small>USDⓈ-M</small></div>
          <div className="status" aria-live="polite">
            <i style={{ background: apiState === 'offline' ? '#ff8e8e' : connected ? '#7ce0a0' : '#b7f77a' }} />
            {connectionLabel}　·　{apiState === 'online' && (health.database ? 'POSTGRESQL' : 'BASE HORS LIGNE')}
          </div>
        </div>
      </header>

      <section className="wrap">
        <div className="heading">
          <div>
            <h1>Scanner de marché</h1>
            <p>Contrats perpétuels USDⓈ-M · données publiques · analyse multi-timeframe</p>
          </div>
          <div className="stats">
            <div className="stat"><small>Paires actives</small><strong>{health.symbols || '—'}</strong></div>
            <div className="stat"><small>Confirmés</small><strong style={{ color: '#7ce0a0' }}>{confirmedCount}</strong></div>
            <div className="stat"><small>Pré-signaux</small><strong style={{ color: '#b7f77a' }}>{previewCount}</strong></div>
          </div>
        </div>

        <div className="toolbar">
          <select aria-label="Filtrer par timeframe" value={timeframe} onChange={(event) => setTimeframe(event.target.value)}>
            <option value="ALL">Tous timeframes</option>
            {TIMEFRAMES.map((value) => <option key={value}>{value}</option>)}
          </select>
          {MODES.map(([key, label]) => (
            <button type="button" className={mode === key ? 'active' : ''} key={key} onClick={() => setMode(key)}>
              {label}
            </button>
          ))}
          <input
            aria-label="Rechercher un symbole"
            placeholder="Rechercher une paire…"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </div>

        <div className="tablebox">
          <table>
            <thead><tr>
              <th>Paire</th><th>Direction</th><th>Timeframe</th><th>État</th><th>Score</th>
              <th>Prix actuel</th><th>24 h</th><th>RSI 14</th><th>Volume / moy.</th><th>ATR 14</th><th>Divergence</th>
            </tr></thead>
            <tbody>
              {visible.map((row) => {
                const isSelected = selected?.symbol === row.symbol && selected.timeframe === row.timeframe;
                return (
                  <tr
                    key={`${row.symbol}-${row.timeframe}`}
                    tabIndex={0}
                    aria-selected={isSelected}
                    onClick={() => setSelected(row)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault();
                        setSelected(row);
                      }
                    }}
                    className={isSelected ? 'selected' : ''}
                  >
                    <td className="symbol">{row.symbol}</td>
                    <td><span className={`badge ${row.side === 'LONG' ? 'long' : 'short'}`}>{row.side}</span></td>
                    <td className="mono">{row.timeframe}</td>
                    <td>{row.confirmed
                      ? <span className="badge long">CONFIRMÉ</span>
                      : row.pre_signal
                        ? <span className="badge preview">PRÉ-SIGNAL</span>
                        : <span className="muted">Surveillance</span>}</td>
                    <td className="mono score">{row.score}/100</td>
                    <td className="mono">{row.price.toPrecision(7)}</td>
                    <td className="mono" style={{ color: (row.change_24h ?? 0) >= 0 ? '#7ce0a0' : '#ff8e8e' }}>
                      {row.change_24h == null ? '—' : `${row.change_24h.toFixed(2)}%`}
                    </td>
                    <td className="mono">{row.rsi.toFixed(1)}</td>
                    <td className="mono">{row.volume_ratio ? `${row.volume_ratio.toFixed(2)}×` : '—'}</td>
                    <td className="mono">{row.atr ? row.atr.toPrecision(4) : '—'}</td>
                    <td className={row.divergence ? 'warn' : 'muted'}>{row.divergence || '—'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {visible.length === 0 && (
            <div className="empty" role="status">
              {apiState === 'offline'
                ? 'API locale indisponible. Vérifiez le backend, puis la reconnexion sera automatique.'
                : !connected
                  ? 'Connexion au flux de marché en cours…'
                  : rows.length === 0
                    ? 'Chargement initial des données du scanner…'
                    : 'Aucun signal ne correspond à ces filtres.'}
            </div>
          )}
        </div>

        {snapshotAt && <div className="snapshot-note">Dernière activité marché : {new Date(snapshotAt).toLocaleTimeString('fr-FR')}</div>}

        {selected && (
          <section ref={chartRef} className="chart-panel" aria-label={`Graphique ${selected.symbol} ${selected.timeframe}`}>
            <div className="chart-title">
              <div>
                {selected.symbol} · {selected.timeframe}
                <span className="muted"> · chandeliers Binance Futures · flux live</span>
              </div>
              <label className="chart-timeframe-control">
                Unité
                <select
                  aria-label={`Unité de temps du graphique ${selected.symbol}`}
                  value={selected.timeframe}
                  onChange={(event) => {
                    const nextTimeframe = event.target.value;
                    const matchingRow = rows.find((row) => row.symbol === selected.symbol && row.timeframe === nextTimeframe);
                    setSelected(matchingRow ?? {
                      ...selected, timeframe: nextTimeframe, confirmed: false, pre_signal: false, atr: null,
                    });
                  }}
                >
                  {TIMEFRAMES.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
              </label>
            </div>
            <div className="trade-plan" aria-label="Plan indicatif de niveaux">
              <div className="trade-plan-heading">
                <strong>PLAN INDICATIF · {selected.side}</strong>
                <span className={selected.confirmed ? 'plan-confirmed' : 'plan-preview'}>
                  {selected.confirmed ? 'Signal confirmé' : selected.pre_signal ? 'Pré-signal · à confirmer' : 'Surveillance'}
                </span>
              </div>
              {selectedPlan ? (
                <div className="trade-levels">
                  <div><small>Entrée de référence</small><strong>{formatPrice(selectedPlan.entry)}</strong></div>
                  <div><small>Stop · 1,5 × ATR</small><strong className="plan-stop">{formatPrice(selectedPlan.stop)}</strong></div>
                  <div><small>Objectif 1 · 1R</small><strong>{formatPrice(selectedPlan.target1)}</strong></div>
                  <div><small>Objectif 2 · 2R</small><strong>{formatPrice(selectedPlan.target2)}</strong></div>
                </div>
              ) : (
                <p className="plan-unavailable">Pas de niveaux calculés : signal non confirmé ou ATR indisponible.</p>
              )}
              <p className="plan-disclaimer">
                Estimation basée sur le prix affiché lors de la sélection et un risque de 1,5 ATR. Objectifs à 1R et 2R.
                Ce n’est pas un conseil ni un ordre : frais, glissement, financement et règles de prix Binance ne sont pas inclus.
              </p>
            </div>
            <MarketChart
              symbol={selected.symbol}
              timeframe={selected.timeframe}
              plan={selectedPlan}
            />
          </section>
        )}

        <div className="foot">
          Les signaux sont des détections techniques et ne constituent pas un conseil financier.
          Les signaux confirmés reposent sur bougies clôturées; les pré-signaux utilisent la bougie en formation.
        </div>
      </section>
    </main>
  );
}
