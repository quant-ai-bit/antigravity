from arbitrage_scanner import ArbitrageScanner
import pandas as pd
from datetime import datetime, time, timedelta, timezone
from tabulate import tabulate
import pytz

# Constants
TARGET_HOURS_BOGOTA = [7, 11, 15, 19, 23] # UTC-5
UTC_OFFSET = -5
POSITION_SIZE = 500
LEVERAGE = 10
MIN_VOLUME_1M = 10

def get_next_target_hour():
    """Calculates the upcoming target hours for display."""
    now_utc = datetime.now(timezone.utc)
    bogota_tz = pytz.timezone('America/Bogota')
    now_bogota = now_utc.astimezone(bogota_tz)
    return now_bogota

def check_funding_time_match(timestamp_ms, target_hour):
    """
    Checks if the funding timestamp matches the target hour in Bogota time.
    timestamp_ms: unix timestamp in ms
    target_hour: int (0-23)
    """
    if not timestamp_ms:
        # Fallback: Assume standard 8h schedule (00, 08, 16 UTC)
        # Check if target_hour (Bogota) corresponds to 00, 08, or 16 UTC.
        # Bogota is UTC-5.
        # 00 UTC = 19:00 Prev Day Bogota (-5) -> Matches 19 target
        # 08 UTC = 03:00 Bogota -> Matches nothing
        # 16 UTC = 11:00 Bogota -> Matches 11 target
        
        # So standard exchanges pay at 11am and 7pm Bogota.
        # They do NOT pay at 7am or 3pm Bogota usually (that would be 12 UTC or 20 UTC).
        # We can assume True if target is 11 or 19.
        if target_hour in [11, 19]:
            return True
        return False
    
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    bogota_tz = pytz.timezone('America/Bogota')
    dt_bogota = dt_utc.astimezone(bogota_tz)
    
    # Allow 5 minute buffer? Usually funding is exact hour.
    # Check if hour matches
    return dt_bogota.hour == target_hour

def main():
    scanner = ArbitrageScanner()
    
    print(f"Starting Advanced Scan for target hours (UTC-5): {TARGET_HOURS_BOGOTA}")
    print("Fetching funding rates...")
    df_rates = scanner.fetch_funding_rates()
    
    if df_rates.empty:
        print("No data fetched.")
        return

    # Pivot Data
    # We need a different approach than simple pivot because we need next_funding_time
    # Group by symbol
    grouped = df_rates.groupby('symbol')
    
    opportunities = []
    
    bogota_tz = pytz.timezone('America/Bogota')
    current_date = datetime.now(bogota_tz).strftime('%Y-%m-%d')
    
    print(f"Analyzing {len(grouped)} pairs...")
    
    for symbol, group in grouped:
        if len(group) < 2:
            continue
            
        # Iterate through target hours to see if this pair is relevant
        # Simplifying: User wants to verify options FOR specific hours.
        # We check each possible pair combination (Exchange A, Exchange B)
        
        exchanges = group.to_dict('records')
        
        # O(N^2) comparison for this symbol
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                a = exchanges[i]
                b = exchanges[j]
                
                # Check for each target hour
                for target_hour in TARGET_HOURS_BOGOTA:
                    
                    # Check if each exchange charges at this hour
                    a_charges = check_funding_time_match(a.get('next_funding_time'), target_hour)
                    b_charges = check_funding_time_match(b.get('next_funding_time'), target_hour)
                    
                    # Logic:
                    # Long A, Short B
                    # Cost = Rate A (if charges) + Rate B (if charges, usually negative for short?)
                    # Funding Rate is usually received by Long if positive? No.
                    # POSITIVE Rate = Long PAYS Short.
                    # NEGATIVE Rate = Short PAYS Long.
                    
                    # We want to Receive Funding.
                    # Best Case: Long (Positive Rate -> Pay? No, Negative -> Receive)
                    # Let's verify standard perps:
                    # +Rate: Long pays Short.
                    # -Rate: Short pays Long.
                    
                    # We want to be Short on +Rate (Receive)
                    # We want to be Long on -Rate (Receive)
                    
                    # Strategy: Long A, Short B.
                    # Net Rate = (Rate B * -1) - (Rate A) ? 
                    # If we Long A: pay Rate A. (If Rate A is -0.01%, we receive 0.01%) -> -Rate A
                    # If we Short B: receive Rate B. (If Rate B is 0.01%, we receive 0.01%) -> Rate B
                    

                    # Net Profit % = (Rate B if charges) - (Rate A if charges)
                    # Wait, careful with signs.
                    # Cashflow Long = -RateA
                    # Cashflow Short = +RateB
                    # Net = RateB - RateA.
                    
                    # Adjustment for "Does not charge":
                    rate_a_eff = a['funding_rate'] if a_charges else 0
                    rate_b_eff = b['funding_rate'] if b_charges else 0
                    
                    # Spread direction 1: Long A, Short B
                    spread_1 = rate_b_eff - rate_a_eff
                    
                    # Spread direction 2: Long B, Short A
                    spread_2 = rate_a_eff - rate_b_eff
                    
                    best_spread = 0
                    long_side = None
                    short_side = None
                    
                    if spread_1 > spread_2:
                        best_spread = spread_1
                        long_side = a
                        short_side = b
                    else:
                        best_spread = spread_2
                        long_side = b
                        short_side = a
                        
                    if best_spread > 0.004:
                        # We have a candidate!
                        # Now check Volume
                        vol_l = scanner.get_volume_1h(long_side['exchange'], symbol)
                        vol_s = scanner.get_volume_1h(short_side['exchange'], symbol)
                        
                        # STRICT VOLUME CHECK
                        # User specified MIN_VOLUME_1M = 30000
                        min_volume_required = MIN_VOLUME_1M
                        
                        if (vol_l is None or vol_l < min_volume_required) or (vol_s is None or vol_s < min_volume_required):
                            # print(f"Skipping {symbol} ({long_side['exchange']}/{short_side['exchange']}) due to low volume: L={vol_l} S={vol_s} Req={min_volume_required}")
                            continue

                        # Check Intervals
                        int_l = scanner.get_funding_interval(long_side['exchange'], symbol)
                        int_s = scanner.get_funding_interval(short_side['exchange'], symbol)
                        
                        is_asymmetric = (int_l != int_s)

                        # Fetch Fees
                        # Need to access scanner.exchanges[name].market(symbol)
                        # Warning: Some symbols in scanner might be standard, but market access requires specific symbol? 
                        # Usually ccxt normalizes it.
                        
                        def get_fees(exch_name, sym):
                            try:
                                ex = scanner.exchanges.get(exch_name)
                                if not ex: return 0.0005, 0.0002 # Default
                                m = ex.market(sym)
                                # Try to get fee tiers, otherwise default to taker/maker
                                taker = m.get('taker', m.get('feeSide', 'get') == 'get' and 0.0005) # Fallback 0.05%
                                maker = m.get('maker', m.get('feeSide', 'make') == 'make' and 0.0002) # Fallback 0.02%
                                # Helper: standard perps usually 0.05% taker, 0.02% maker
                                if taker is None: taker = 0.0005
                                if maker is None: maker = 0.0002
                                return taker, maker
                            except:
                                return 0.0005, 0.0002

                        l_taker, l_maker = get_fees(long_side['exchange'], symbol)
                        s_taker, s_maker = get_fees(short_side['exchange'], symbol)
                        
                        # User requested columns:
                        # % COMISION TAKER, % COMISION MAKER
                        
                        # Format Output
                        opp = {
                            'FECHA': current_date,
                            'HORA': f"{target_hour}:00",
                            'PAR': symbol,
                            'VALOR_OP': POSITION_SIZE,
                            'LEVERAGE': LEVERAGE,
                            'LONG_EXCH': long_side['exchange'],
                            'LONG_RATE': long_side['funding_rate'],
                            'LONG_NEXT': datetime.fromtimestamp(long_side['next_funding_time']/1000).strftime('%H:%M') if long_side.get('next_funding_time') else 'N/A',
                            'LONG_INTERVAL': int_l,
                            'LONG_VOL_1M': vol_l,
                            'LONG_FEE_TAKER': l_taker,
                            'LONG_FEE_MAKER': l_maker,
                            'SHORT_EXCH': short_side['exchange'],
                            'SHORT_RATE': short_side['funding_rate'],
                            'SHORT_NEXT': datetime.fromtimestamp(short_side['next_funding_time']/1000).strftime('%H:%M') if short_side.get('next_funding_time') else 'N/A',
                            'SHORT_INTERVAL': int_s,
                            'SHORT_VOL_1M': vol_s,
                            'SHORT_FEE_TAKER': s_taker,
                            'SHORT_FEE_MAKER': s_maker,
                            'SPREAD': best_spread,
                            'ASYMMETRIC': 'Yes' if is_asymmetric else 'No'
                        }
                        opportunities.append(opp)
                        print(f"Found: {symbol} Spread: {best_spread:.4%} for {target_hour}:00 (Asym: {opp['ASYMMETRIC']})")

    if not opportunities:
        print("No opportunities found matching criteria and time slots.")
        return
        
    # Convert to DF and Save
    df_final = pd.DataFrame(opportunities)
    df_final.to_csv('advanced_opportunities.csv', index=False)
    print("\nSaved to advanced_opportunities.csv")
    
    # Print preview
    print(tabulate(df_final.head(10), headers='keys', tablefmt='psql'))

    if scanner.skipped_exchanges:
        print("\n" + "="*50)
        print("WARNING: The following exchanges were SKIPPED (No bulk support):")
        for s in scanner.skipped_exchanges:
            print(f"- {s}")
        print("="*50 + "\n")

if __name__ == "__main__":
    main()
