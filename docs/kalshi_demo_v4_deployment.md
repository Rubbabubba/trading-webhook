# Kalshi V4 demo execution deployment

`kalshi-demo-v4` is a separate Render background worker for the Kalshi demo
environment. It does not replace `kalshi-paper-portfolio`, which remains the
production-book profitability experiment.

## Safety boundary

- Demo API base URL and demo-scoped journal are hard-coded through `DemoClient`
  and `BinaryJournal`; production credentials cannot authenticate to this host.
- One contract maximum and one open position maximum.
- One filled entry per event, with no side flip or reentry.
- Capital limit 160 cents, order limit 110 cents, and daily loss stop 100 cents.
- Every mutation is durably journaled before the HTTP request.
- Unknown submissions are reconciled and never blindly resent.
- Inventory and resting orders reconcile before each mutation.
- Production execution remains disabled in status and configuration.

The worker uses V4's frozen liquidity-confirmed momentum signal on demo order
books. Its execution results validate API behavior and operational recovery; demo
liquidity is not profitability evidence. The separate local V4 paper worker keeps
testing the same hypothesis on production public books.

## Render configuration

Blueprint: `deploy/kalshi-demo-v4.render.yaml`.

The service requires:

- `KALSHI_DEMO_API_KEY_ID` as a secret environment variable;
- the matching private key as a Render secret file named
  `kalshi-demo-private.pem`;
- `KALSHI_DEMO_PRIVATE_KEY_PATH=/etc/secrets/kalshi-demo-private.pem`;
- a persistent disk mounted at `/var/data`.

Never paste the private key into logs, source, environment-variable values, or
conversation output. Render exposes uploaded secret files at `/etc/secrets`.

## Acceptance

Before calling the deployment operational, verify that it authenticates to demo,
reports the expected approximately $500 demo balance, has zero unknown mutation,
uses the persistent journal after restart, and remains limited to one contract.
An actual entry is signal-dependent and is not required merely to prove startup.
