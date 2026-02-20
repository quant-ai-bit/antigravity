"""
log_top3.py
-----------
Reads the latest advanced_opportunities.csv and appends the top 3 opportunities
(by spread, with Vol LONG >= 5000 and Vol SHORT >= 5000) to scan_history.md.

Called automatically after advanced_scan.py by run_scan.bat.
"""

import pandas as pd
from datetime import datetime
import pytz
import os

# --- Configuration ---
CSV_FILE = "advanced_opportunities.csv"
HISTORY_FILE = "scan_history.md"
MIN_VOL = 5000          # Minimum 1-min volume in USD for both sides
TOP_N = 3               # Number of top opportunities to log

# Map scan run time -> target hour label
# The scan runs 20 min before the target hour; we map by nearest target.
TARGET_HOURS = [7, 11, 15, 19, 23]


def get_target_label():
    """Return the target hour for the current run based on current Bogotá time."""
    bogota_tz = pytz.timezone('America/Bogota')
    now = datetime.now(bogota_tz)
    hour = now.hour

    # Find the nearest upcoming target hour
    for t in TARGET_HOURS:
        if t - 1 <= hour <= t:
            return f"{t:02d}:00"

    # Fallback: return closest target
    closest = min(TARGET_HOURS, key=lambda t: abs(t - hour))
    return f"{closest:02d}:00"


def format_pct(val):
    try:
        return f"{float(val)*100:.4f}%"
    except:
        return str(val)


def format_vol(val):
    try:
        v = float(val)
        if v >= 1_000_000:
            return f"${v/1_000_000:.1f}M"
        elif v >= 1_000:
            return f"${v/1_000:.1f}k"
        else:
            return f"${v:.0f}"
    except:
        return str(val)


def main():
    bogota_tz = pytz.timezone('America/Bogota')
    now = datetime.now(bogota_tz)
    date_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%H:%M')
    target_label = get_target_label()

    # --- Load CSV ---
    if not os.path.exists(CSV_FILE):
        print(f"[log_top3] ERROR: {CSV_FILE} not found.")
        return

    df = pd.read_csv(CSV_FILE)

    if df.empty:
        print("[log_top3] CSV is empty. Nothing to log.")
        return

    # --- Filter ---
    df = df[
        (df['LONG_VOL_1M'] >= MIN_VOL) &
        (df['SHORT_VOL_1M'] >= MIN_VOL)
    ].copy()

    if df.empty:
        print(f"[log_top3] No opportunities meet the volume filter (>= ${MIN_VOL:,}).")
        # Still write a "no results" entry
        top3 = pd.DataFrame()
    else:
        # Sort by SPREAD descending
        df = df.sort_values('SPREAD', ascending=False)
        # Deduplicate: keep best spread per unique (PAR, LONG_EXCH, SHORT_EXCH) combo
        df = df.drop_duplicates(subset=['PAR', 'LONG_EXCH', 'SHORT_EXCH'], keep='first')
        top3 = df.head(TOP_N)

    # --- Build markdown block ---
    lines = []

    # Check if history file exists and if today's date header already exists
    existing_content = ""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            existing_content = f.read()

    needs_date_header = f"## 📅 {date_str}" not in existing_content

    if needs_date_header:
        lines.append(f"\n## 📅 {date_str}\n")
        lines.append("| Hora | # | Par | Spread | Long Exchange | Rate Long | Short Exchange | Rate Short | Vol L | Vol S | Asim |")
        lines.append("|------|---|-----|--------|---------------|-----------|----------------|------------|-------|-------|------|")

    if top3.empty:
        lines.append(f"| {target_label} | — | *Sin oportunidades con Vol ≥ ${MIN_VOL:,}* | — | — | — | — | — | — | — | — |")
    else:
        for rank, (_, row) in enumerate(top3.iterrows(), 1):
            asim = "✅" if str(row.get('ASYMMETRIC', '')).lower() in ['yes', 'sí', 'si'] else "❌"
            lines.append(
                f"| {target_label} | {rank} | **{row['PAR'].replace('/USDT:USDT','/USDT')}** "
                f"| {format_pct(row['SPREAD'])} "
                f"| {row['LONG_EXCH']} "
                f"| {format_pct(row['LONG_RATE'])} "
                f"| {row['SHORT_EXCH']} "
                f"| {format_pct(row['SHORT_RATE'])} "
                f"| {format_vol(row['LONG_VOL_1M'])} "
                f"| {format_vol(row['SHORT_VOL_1M'])} "
                f"| {asim} |"
            )

    block = "\n".join(lines) + "\n"

    # --- Add date header to top of file if new day OR append rows ---
    if needs_date_header:
        # Read existing, prepend date header after the main title (or at end)
        # We append at the end for simplicity
        with open(HISTORY_FILE, 'a', encoding='utf-8') as f:
            f.write(block)
    else:
        # Date section exists — append rows after last line of today's table
        # Find insertion point: after last row that starts with "| " for today
        with open(HISTORY_FILE, 'a', encoding='utf-8') as f:
            f.write("\n".join(lines[2:]) + "\n")  # Skip header lines, write only rows

    print(f"[log_top3] ✅ Logged {min(len(top3), TOP_N)} opportunities for {target_label} on {date_str}")
    if not top3.empty:
        for rank, (_, row) in enumerate(top3.iterrows(), 1):
            print(f"  #{rank}: {row['PAR']} | Spread: {format_pct(row['SPREAD'])} | {row['LONG_EXCH']} → {row['SHORT_EXCH']}")


if __name__ == "__main__":
    main()
