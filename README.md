# 🚀 ANTIGRAVITY — Crypto Funding Rate Arbitrage Scanner

Automated scanner that identifies cross-exchange funding rate arbitrage opportunities.

## How it works

- Scans ~1,900 perpetual futures pairs across 17 exchanges
- Filters opportunities with spread > 0.4% and volume > $5,000/min on both sides
- Logs the **top 3 opportunities** per scan window to [`scan_history.md`](scan_history.md)

## Schedule (Bogotá UTC-5)

| Scan Time | Trading Window |
|-----------|---------------|
| 06:40     | 07:00         |
| 10:40     | 11:00         |
| 14:40     | 15:00         |
| 18:40     | 19:00         |
| 22:40     | 23:00         |

## Files

| File | Description |
|------|-------------|
| `advanced_scan.py` | Main scanner — fetches funding rates from all exchanges |
| `log_top3.py` | Reads CSV output and logs top 3 to `scan_history.md` |
| `arbitrage_scanner.py` | Core library (ccxt-based exchange connectors) |
| `scan_history.md` | 📊 **Daily log** — top 3 opportunities per window |
| `advanced_opportunities.csv` | Latest raw scan output |

## Setup

```bash
pip install -r requirements.txt
python advanced_scan.py   # Run scan
python log_top3.py        # Log top 3 to scan_history.md
```

## Automated via GitHub Actions

The workflow (`.github/workflows/scan.yml`) runs automatically 5× per day.
Results are committed back to this repo automatically.
