"""Compact, read-only operating view. Detailed telemetry stays in the full console."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from html import escape

from intraday_monitoring import candidate_views
from regime_intraday_ledger import performance_views


def esc(value):
    return escape(str(value if value is not None else "—"))


def render_overview(*, scan, ledger, readiness, scanner):
    now = datetime.now(timezone.utc)
    stamp = scan.get("ts_utc")
    try:
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        age = (now - parsed).total_seconds()
        last_scan = parsed.astimezone(ZoneInfo("America/Chicago")).strftime("%I:%M:%S %p %Z · %b %d")
        stale = age > 180 or age < 0
    except (ValueError, TypeError, AttributeError):
        stale, last_scan = True, "No scan received"
    blockers = readiness.get("paper_blockers") or []
    candidates = candidate_views(ledger, now=now, blocker=", ".join(blockers))["active"]
    orders = ledger.get("orders") or {}
    terminal = {"canceled", "cancelled", "expired", "rejected", "filled_closed"}
    active = {key: row for key, row in orders.items() if row.get("status") not in terminal}
    attention = any(any(word in str(row.get("status", "")) + str((row.get("close_order") or {}).get("status", "")) for word in ("attention", "failed", "error")) for row in active.values())
    regime = scan.get("regime") or {}
    config = scan.get("config") or {}
    perf = performance_views(ledger)["broker_paper"]
    tone, title, detail = "calm", "Watching for a qualifying setup", "No new signal in the latest scan. Waiting is part of the strategy."
    if scan.get("signals"):
        title, detail = "Setup detected", "A signal is not a fill. Review candidate and order status below."
    if candidates:
        title, detail = "Candidate awaiting submission", "Review the detailed view for eligibility and submission status."
    if regime.get("name") == "not_ready":
        tone, title, detail = "warn", "Waiting for market data readiness", "The signal inputs are not ready. See the detailed view for warm-up and freshness checks."
    if active:
        title, detail = "Paper order activity", "An order is being tracked. Confirm fills and remaining positions in Alpaca paper."
    if not readiness.get("paper_ready"):
        tone, title, detail = "warn", "New paper entries paused", ", ".join(blockers) or "Paper readiness has not been confirmed."
    if stale:
        tone, title, detail = "warn", "Scan needs checking", "The latest scan is missing or more than three minutes old. Check system health."
    if attention:
        tone, title, detail = "warn", "Paper order needs attention", "Review order recovery details and verify positions in Alpaca paper."
    market = str(regime.get("name") or "not ready").replace("_", " ").title()
    direction = str(regime.get("direction") or "No directional bias").capitalize()
    tiles = []
    for symbol, row in (scan.get("features") or {}).items():
        state = row.get("freshness") or "not assessed"
        tiles.append(f"<span class='data-chip'><b>{esc(symbol)}</b> {esc('scan outdated' if stale else state)}</span>")
    activity = []
    for identity, row in active.items():
        signal = row.get("signal") or {}
        activity.append(f"<article class='order'><div><strong>{esc(row.get('symbol') or signal.get('symbol') or 'Paper order')}</strong><p>{esc(identity)}</p></div><span class='tag'>{esc(row.get('status'))}</span></article>")
    for identity, row in candidates.items():
        activity.append(f"<article class='order'><div><strong>Candidate</strong><p>{esc(identity)}</p></div><span class='tag'>Awaiting submission</span></article>")
    activity_html = "".join(activity) or "<div class='empty'><span class='empty-icon' aria-hidden='true'>—</span><h3>No active orders recorded</h3><p>Canceled and completed orders are in the detailed view.<br>This is the system ledger, not an account inventory check.</p></div>"
    strategies = " · ".join(name for name, enabled in (("Mean reversion", config.get("mean_reversion_enabled")), ("Momentum", config.get("momentum_enabled"))) if enabled) or "No strategy enabled"
    sleeves = ", ".join(str(name).replace("_", " ") for name in (scan.get("sleeves") or {})) or ", ".join(config.get("trade_symbols") or []) or "Not reported"
    proximity = []
    for sleeve in (scan.get("sleeves") or {}).values():
        proximity.extend(sleeve.get("setup_proximity") or [])
    proximity = proximity or list(scan.get("setup_proximity") or [])
    proximity_html = "".join(
        f"<article class='proximity'><div class='heading'><strong>{esc(row.get('symbol'))} mean reversion</strong><span class='tag'>{esc('SIGNAL READY' if row.get('underlying_signal_ready') else row.get('next_gate'))}</span></div><div class='gate-grid'><span class={'pass' if row.get('data_ready') else 'fail'}>Data</span><span class={'pass' if row.get('regime_ready') else 'fail'}>Range regime</span><span class={'pass' if row.get('stretch_ready') else 'fail'}>VWAP stretch</span><span class={'pass' if row.get('reversal_ready') else 'fail'}>Reversal bar</span></div><p>{esc(row.get('vwap_distance_atr'))} ATR from VWAP · required {esc((row.get('required_vwap_atr_band') or ['—','—'])[0])}–{esc((row.get('required_vwap_atr_band') or ['—','—'])[1])} ATR · nearest edge {esc(row.get('distance_to_nearest_band_edge_atr'))} ATR</p></article>"
        for row in proximity
    ) or "<div class='empty'><p>Proximity data will appear after the next scan.</p></div>"
    missing = perf.get("missing_fill_roundtrips", 0)
    pnl = float(perf.get("gross_realized_dollars_from_fills") or 0)
    results_note = f"{missing} closed roundtrip(s) excluded: missing fills." if missing else "Recorded broker fills only · before fees · all tracked history"
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="30"><title>Intraday · Operating overview</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#0a1018;color:#eaf0f8;font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}}main{{max-width:1180px;margin:auto;padding:36px 28px}}a{{color:inherit;text-decoration:none}}a:hover{{text-decoration:underline}}a:focus-visible{{outline:3px solid #65e0bf;outline-offset:5px}}header,.heading,.order,footer{{display:flex;justify-content:space-between;align-items:center;gap:20px}}eyebrow,.eyebrow{{font-size:11px;letter-spacing:.15em;text-transform:uppercase;color:#a3b6cb;font-weight:700}}h1{{font-size:28px;letter-spacing:-1px;margin:5px 0}}h2{{font-size:19px;margin:0}}h3{{font-size:17px;margin:8px 0}}p{{color:#a3b6cb;margin:7px 0}}.button{{border:1px solid #3c536d;border-radius:10px;padding:11px 17px;white-space:nowrap;font-weight:600;background:#152235}}.lock{{color:#a7b8ca;font-size:11px;letter-spacing:.06em}}.paper{{color:#7ce5c4}}.hero{{margin:28px 0 20px;border:1px solid #285346;border-radius:20px;padding:30px;background:radial-gradient(ellipse at top right,#193d36 0,transparent 70%),#111e25}}.hero.warn{{border-color:#806534;background:radial-gradient(ellipse at top right,#3d3021 0,transparent 70%),#191d24}}.hero h2{{font-size:clamp(25px,3vw,36px);letter-spacing:-1px;max-width:800px;margin:10px 0}}.hero p{{max-width:780px}}.status-dot{{display:inline-block;width:8px;height:8px;border-radius:50%;background:#7ce5c4;margin-right:9px}}.warn .status-dot{{background:#f1c47c}}.cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}}.card,.panel{{border:1px solid #253446;background:#101a27;border-radius:16px;padding:23px}}.value{{font-size:30px;letter-spacing:-1px;font-weight:650;margin-top:9px}}.caption{{font-size:12px;color:#a3b6cb;margin-top:8px}}.workspace{{display:grid;grid-template-columns:1.6fr 1fr;gap:16px;margin-top:20px}}.tag{{border-radius:7px;background:#203247;padding:5px 10px;font-size:12px;color:#c6d9eb;overflow-wrap:anywhere}}.empty{{padding:27px 0;text-align:center}}.empty-icon{{font-size:30px;color:#66819a}}.empty p{{font-size:13px}}.order{{padding:16px 0;border-bottom:1px solid #253446;flex-wrap:wrap}}.order p{{font-size:12px;overflow-wrap:anywhere}}.data-chip{{display:inline-block;font-size:12px;margin:10px 6px 0 0;color:#a3b6cb}}.data-chip b{{color:#d5e2ef;margin-right:5px}}.setup{{margin:17px 0;padding-bottom:13px;border-bottom:1px solid #253446}}.setup:last-child{{border:0;margin-bottom:0;padding-bottom:0}}.setup p,.proximity p{{font-size:13px}}.proximity{{padding:17px 0;border-bottom:1px solid #253446}}.proximity:last-child{{border:0}}.gate-grid{{display:flex;gap:7px;flex-wrap:wrap;margin-top:13px}}.gate-grid span{{padding:5px 9px;border-radius:7px;font-size:11px;background:#2a1f27;color:#f2a8b8}}.gate-grid .pass{{background:#17352e;color:#7ce5c4}}footer{{margin-top:23px;font-size:12px;color:#a3b6cb;flex-wrap:wrap}}.muted-link{{color:#bdd4eb}}@media(max-width:760px){{main{{padding:22px 16px}}.cards,.workspace{{grid-template-columns:1fr}}header{{align-items:flex-start;flex-wrap:wrap}}.hero{{padding:23px}}.card{{padding:18px}}.value{{font-size:27px}}}}
</style></head><body><main><header><div><div class="eyebrow">Trading workspace / Paper session</div><h1>Intraday overview</h1><div class="lock"><span class="paper">PAPER ONLY</span> &nbsp; / &nbsp; LIVE INTRADAY CLOSED</div></div><a class="button" href="/dashboard/intraday?view=detailed">Detailed view ↗</a></header>
<section class="hero {tone}" aria-label="Operating status"><div class="eyebrow"><span class="status-dot"></span>Current operating status</div><h2>{esc(title)}</h2><p>{esc(detail)}</p></section>
<div class="cards"><section class="card"><div class="eyebrow">Recorded paper P/L</div><div class="value">${pnl:,.2f}</div><div class="caption">{esc(results_note)}</div></section><section class="card"><div class="eyebrow">Broker roundtrips</div><div class="value">{esc(perf.get('verified_fill_roundtrips', 0))}<span class="caption"> with entry + exit fills</span></div><div class="caption">{esc(perf.get('recorded_order_count', 0))} recorded orders · Not shadow results</div></section><section class="card"><div class="eyebrow">Market regime</div><div class="value">{esc(market)}</div><div class="caption">{esc(direction)} · Regime is not an entry signal</div></section></div>
<section class="panel" style="margin-top:20px"><div class="heading"><h2>Setup proximity</h2><span class="caption">Rules, not a probability</span></div>{proximity_html}</section><div class="workspace"><section class="panel"><div class="heading"><h2>Needs your attention</h2><span class="tag">{len(active) + len(candidates)} active</span></div>{activity_html}<a class="muted-link" href="/dashboard/intraday?view=detailed">View order history &amp; diagnostics →</a></section><section class="panel"><h2>Session essentials</h2><div class="setup"><div class="eyebrow">Enabled strategies</div><p>{esc(strategies)}</p><p>{esc(sleeves)}</p></div><div class="setup"><div class="eyebrow">Paper controls</div><p>New entries: {esc('ready' if readiness.get('paper_ready') else 'paused')} · Auto-submit: {esc('on' if scan.get('paper_auto_submit_enabled') else 'off / not reported')}</p><p>Live trading remains disabled.</p></div><div class="setup"><div class="eyebrow">Data at last scan</div>{''.join(tiles) or '<p>No data reported</p>'}</div></section></div>
<footer><span>Last scan: {esc(last_scan)} · Refreshes every 30 seconds</span><a href="/health">System health ↗</a></footer></main></body></html>"""
