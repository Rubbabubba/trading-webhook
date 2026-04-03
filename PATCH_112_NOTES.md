# Patch 112 – IEX-safe bar fallback + candidate payload plumbing fix

- routes 1-minute bar fallback through REST bar helper using configured feed (IEX-safe)
- avoids SDK bar path that can trigger recent SIP entitlement errors
- passes scan timestamp and candidate close/price metadata into entry execution so candidate fallback can activate
- updates patch version marker
