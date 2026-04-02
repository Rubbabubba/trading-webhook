Patch 100: execution liquidity override
- adds an IEX-only high-liquidity spread override to avoid false wide-spread rejects
- preserves the primary spread gate for non-IEX feeds and lower-liquidity symbols
- records spread override diagnostics on the quote snapshot
