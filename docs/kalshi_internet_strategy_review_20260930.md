# Kalshi Internet Strategy Review — September 30, 2026

## Decision

Public bot repositories provide useful exchange plumbing, but none reviewed supplies independently verified, transferable profitability. The strongest directly applicable evidence is transaction-level research on favorite-longshot bias and executable arbitrage. The system will test those effects as bounded shadow sleeves instead of importing another project's unverified signals.

## Evidence reviewed

| Source | Evidence | Application |
|---|---|---|
| Bürgi, Deng, and Whelan, *Makers and Takers: The Economics of the Kalshi Prediction Market* | More than 300,000 Kalshi contracts; low-price contracts underperform break-even and high-price contracts have small positive returns; maker/taker behavior differs. | Test high-probability contracts from the passive side and retain maker/taker separation. |
| Cardozo and Rivero-Wildemauwe, *The Favorite-Longshot Bias in Prediction Markets* | 588 million Polymarket trades; the two-sided effect is robust in Crypto and Politics and absent in Sports; parent-event grouping materially changes results. | Restrict the new favorite sleeve to Crypto and Politics and cluster every gate by parent event. |
| Gebele, Mutzel, and Matthes, *Executable Arbitrage and Market Efficiency in Prediction Markets* | Payoff-bound violations are not necessarily executable; depth, supported transformations, and settlement paths determine realizable profit. | Keep structural arbitrage separate and require synchronous executable depth plus unfinished-leg stress. |
| Cont, Kukanov, and Stoikov, *The Price Impact of Order Book Events* / queue-imbalance literature | Top-of-book imbalance can predict short-horizon price movement in liquid books. | Preserve V12 as the all-market microprice benchmark; require prospective 5/30/300-second markouts. |
| Public Kalshi bot repositories reviewed | Reusable API, paper-trading, OMS, weather, momentum, and mean-reversion components; no audited live profitability found. | Reuse ideas only after preregistration; do not treat repository claims or synthetic backtests as proof. |

Primary references:

- https://www.ifo.de/en/cesifo/publications/2026/working-paper/makers-and-takers-economics-kalshi-prediction-market
- https://arxiv.org/abs/2609.12878
- https://arxiv.org/abs/2608.00666
- https://arxiv.org/abs/1512.03492
- https://github.com/ksubramanian709/Kalshi-Market-Making
- https://github.com/hamad-khawaja/kalshi-trading-bot

## V13 prospective test

`kalshi_favorite_maker_v13_shadow` tests one passive favorite per parent event per hour. It admits Crypto and Politics only, requires a displayed 90–98 cent bid, a 1–5 cent spread, at least one displayed contract at the bid, and expiration between one hour and fourteen days. It assumes no fill and subtracts two cents for fees and execution stress at settlement.

The promotion gate requires 100 resolved parent events, both registered families, positive stressed net overall and in each family, and a positive parent-event-clustered 95% lower confidence bound. Passing this gate authorizes only a separate one-contract Demo trial. It does not authorize production.

## Other models retained

- V12 remains the frozen all-market microprice/imbalance benchmark.
- Structural V1 remains the executable nested-threshold arbitrage sleeve.
- Favorite-longshot V1 remains the broad symmetric calibration benchmark.
- Sports challenger evidence remains separate and cannot be pooled with other families.

## Next specialized sleeves

Weather and macroeconomic contracts can support independent fair-value models using official forecast distributions and nowcasts. They require exact settlement-source mapping, vintage-data retention, and a prospective scoring period. Cross-exchange arbitrage requires exact contract-resolution equivalence and synchronized executable depth on both venues. These are separate research projects and must not be mixed into the V13 result.
