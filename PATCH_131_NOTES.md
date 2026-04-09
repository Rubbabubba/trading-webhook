# Patch 131 Notes

- Fixes scan exception introduced by Patch 130 by restoring missing `math` import.
- Adds fallback recent-scan evaluation so a single scan exception does not immediately collapse live eligibility when a recent completed scan is still available.
- Preserves Patch 130 extension-discipline behavior.
