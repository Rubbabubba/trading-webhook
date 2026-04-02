Patch 103 - quote snapshot return fix

Root cause fixed:
- get_latest_quote_snapshot() built quote/trade fields but never returned a snapshot dict.
- execute_entry_signal() then did snapshot.get("price") on None, causing:
  'NoneType' object has no attribute 'get'

What changed:
- restored a concrete return object from get_latest_quote_snapshot()
- preserved quote_debug, freshness, spread, mid, bid/ask, and trade price fields
- bumped patch marker to patch-103-quote-snapshot-return-fix
