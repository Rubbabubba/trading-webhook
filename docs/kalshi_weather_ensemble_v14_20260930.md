# Kalshi Weather Ensemble V14 — prospective shadow sleeve

Date: September 30, 2026

## Decision

Run a new weather ensemble sleeve in shadow mode across every currently active
Kalshi daily-temperature ladder with a registered series-to-station mapping.
The sleeve never places an order. It retains forecast vintages, market
distributions, hypothetical high-edge candidates, settlement values, and
market-relative scores so the hypothesis can be accepted or rejected on fresh
evidence.

The honest prior is negative. A small public bot reports +$3.89 on 20 settled
paper trades, but its public repository does not provide an independently
auditable trade ledger. A larger public WeatherBot experiment tested GFS,
ECMWF IFS, ECMWF AIFS, and WeatherNext 2 ensembles over 578 station-days and 19
stations; every locked ensemble variant lost to Kalshi on Brier and ranked
probability score. Its separately preregistered model-update latency test also
found that Kalshi moved about 16 minutes before the hobby-scale collector first
saw a model run. V14 therefore tests a narrower two-model agreement subset and
must beat the market directly.

The frozen registry covers the 48 current daily-temperature series: high and
low ladders for 24 settlement cities/stations. Exact series names and live rule
text must both match; similarly named non-weather contracts fail closed.

## Frozen hypothesis and parameters

V14 combines NOAA GEFS and ECMWF IFS ensemble member distributions with equal
model weight. It admits an event only when:

- its Kalshi series has an explicit settlement-station mapping;
- the rule text still identifies that location;
- every listed contract has parseable threshold or range bounds; a complete,
  non-overlapping ladder is required only for multinomial normalization;
- the target is one to seven days away;
- at least 20 GFS and 40 ECMWF members are present;
- the two ensemble centers differ by no more than 3°F;
- the two contract probabilities differ by no more than 15 percentage points;
- both model families prefer the same side;
- the executable ask is 10–90 cents with at least one displayed contract; and
- raw edge is at least 12 cents, leaving at least 10 cents after a fixed
  two-cent fee-and-model stress.

One best hypothetical candidate is retained per parent event per 30-minute
decision bucket. A displayed quote is never counted as a fill. Forecast
availability uses first observed receipt time rather than provider run labels.

The first live smoke cycle exposed future events with only one or several
independently resolvable binary contracts listed. The registration was amended
before any settlement evidence existed: those contracts use binary Brier/proper
scores, while complete ladders retain normalized multinomial Brier/RPS scoring.
The nine pre-amendment incomplete-ladder observations remain in the database.

## Evidence gate

The prospective gate requires 300 resolved city-days, at least six cities with
50 days each, and two seasons. Cost-stressed settlement P&L and the
city-day-clustered 95% lower bound must be positive. The ensemble distribution
must also beat the normalized Kalshi midpoint distribution on both Brier score
and ranked probability score. Passing all conditions permits only a separate
one-contract Demo trial. After 500 city-days, nonpositive stressed P&L or
failure to beat the market rejects the sleeve.

## Sources

- [Open-Meteo Ensemble API](https://open-meteo.com/en/docs/ensemble-api)
- [National Weather Service API](https://www.weather.gov/documentation/services-web-api)
- [AwpDemon weather bot](https://github.com/AwpDemon/kalshi-weather-bot)
- [WeatherBot ensemble benchmark preregistration](https://github.com/wallyworley/weatherbot/blob/main/docs/research/EXP_2026_013_ENSEMBLE_MARKET_BENCHMARK.md)
- [WeatherBot ensemble benchmark results](https://github.com/wallyworley/weatherbot/blob/main/docs/research/EXP_2026_013_RESULTS.md)
- [WeatherBot market-reaction latency results](https://github.com/wallyworley/weatherbot/blob/main/docs/research/EXP_2026_011_RESULTS.md)

The machine-readable frozen registration is in
`configs/kalshi_weather_ensemble_v14_20260930/registration.json`.
