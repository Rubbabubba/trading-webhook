# Daily sports paper-performance email

Authorized September 10, 2026. Target one email each morning around 8:00 a.m. America/Chicago to the user's connected Gmail account, matthewcdumas@gmail.com. The first scheduled report is September 11 for September 10. Gmail profile and sending tools are available. The daily send is explicitly authorized; do not require a new approval for each email. The app permits one heartbeat per task, so the existing 30-minute operational/improvement follow-up also sends the digest on its first run at or after 8:00 a.m. This normally gives an 8:00–8:30 delivery window while the app/computer are available. Check delivery eligibility before choosing to stay quiet on a routine poll; do not send before 8:00 a.m. or before September 11.

## Reporting period

Use the previous America/Chicago calendar day, midnight inclusive to the next midnight exclusive, converted to UTC with zoneinfo. Do not use the last 24 hours at send time or the UTC calendar date. Attribute each realized sale/settlement to its actual ledger timestamp. An overnight game can span reports; identify carryover positions and distinguish activity during the day from lifetime game totals. An unresolved market has no invented settlement.

## Required email content

Subject: Paper sports performance — YYYY-MM-DD (Central)

Lead with the prior day's actual results and material coverage limitations. Include MLB, NFL, college football, EPL, MLS, men's tennis and women's tennis. For an inactive sport, say no scheduled/recorded activity rather than treating missing data as a successful zero-return test. Novig is historical research only until a live paper integration is actually implemented.

For each active sport, show separate rows for each paper build and holding variant: games observed, entries, exits/settlements, realized net P&L after modeled costs, available fees/slippage, and open/unresolved positions at the reporting cutoff. Include the main winners/losers or instructive examples when useful. Keep five- and fifteen-minute hypothetical accounts separate; do not add them into a single bankroll or treat the same game traded twice as two independent games. Distinguish baseline versus challenger results on paired games, and explain when their game coverage differs.

Show model/data problems, missed games, zero-trade cases, unavailable exits and affected results explicitly. Drawdown must be labeled accurately: per-game lifetime drawdown is not daily portfolio drawdown. Derive a daily figure only from adequate equity observations. Do not invent exact fees if an older build saved modeled aggregate costs only; state the limitation. An empty or unavailable ledger is not evidence of zero trades/P&L.

Include a short build-changes section: version, whether it was actually deployed, high-level change, reason, tests, and which future games evaluate it. Read the change journal and verify source/launcher/ledger evidence. Distinguish proposed, tested, launched and rolled-back changes. If no paper build changed during the reporting day, say so explicitly. Retain unsuccessful changes and their results. Mention operational fixes separately from strategy changes.

End with next steps and material limitations, without claiming proven profitability. Use an email-readable table or plain-text list. Put the actual information in the body; local filesystem links alone are unusable on another device. Avoid sending raw ledgers, account identifiers or unrelated personal information.

## Authoritative sources and accounting

- New paper baseline: sports_paper/research_20260910/paper.sqlite3. games contains frozen mappings; samples supplies UTC timestamps and observations; actions joins samples through sample_id and separates horizon. Sum buy counts and sell/settle counts only in the report window. Use action.profit_cents for closed positions. Costs of an entry from a previous day are already included in its eventual realized profit; do not deduct them twice. Report daily charged fees/slippage separately from trade P&L. Reconstruct open positions from the last account snapshot before the day-end cutoff, not the current mutable account state. Use a consistent read-only SQLite transaction.
- Original NFL/MLB baseline: configs/sports_week_20260908/manifest.json and sports_paper/week_20260908/<slug>/paper.sqlite3. Each samples row contains at, actions, observation and account. Per-game report.json can corroborate totals but is cumulative, so cannot replace date-filtered accounting. Include scheduled games without ledgers as missed/unavailable, such as the September 9 NFL opener.
- Original college test, if its dates are relevant: ncaaf_paper/smu_fsu_20260907, with its own version/cost assumptions.
- Capture only: sports_paper/capture_20260910 and sports_paper/expansion_20260910. These have observations, not trading returns; report them as capture-only when not matched to a paper experiment.
- Improvements: sports_paper/rolling_review_20260910/decisions.json, docs/sports_rolling_improvement_20260910.md, and candidate paths recorded there. Include all actually launched candidate versions in the digest and date-filter them too. Future deployment records should include deployed_at in UTC, version, status, summary, rationale, tests, ledger/output path and evaluation scope.

## Delivery and deduplication

Save the exact report body and source/cutoff notes under sports_paper/daily_email/YYYY-MM-DD before sending. Use the connected Gmail send_email tool with this verified recipient and a text/plain or text/html MIME body. The user already authorized these daily emails. Do not send a test/setup email outside the morning schedule unless asked.

Maintain sports_paper/daily_email/delivery.json keyed by report date, recipient and subject, recording successful Gmail message ID and send timestamp. Before sending, check both this journal and Sent mail for the exact subject and recipient to avoid duplicates if a prior run succeeded before saving its journal. If the send response is uncertain, search Sent and resolve status before retrying. A generated report or draft does not count as sent. If tools or data are unavailable, report the delivery/data issue here rather than falsely claiming success. Send an honest no-activity summary on quiet days because the user explicitly requested daily delivery. Do not silently stop the email schedule when a research slate ends; make clear when no active tests remain.

The local task requires Codex and the computer to be available at the scheduled time. If execution is delayed, report the actual delivery time and the intended prior-day window; do not backdate delivery. Daily report generation does not authorize live orders, purchases or changes to risk limits.
