import ccxt
import pandas as pd
from datetime import datetime

class ArbitrageScanner:
    def __init__(self):
        self.skipped_exchanges = []
        self.exchanges = {
            'binance': ccxt.binance({'enableRateLimit': True}),
            'bybit': ccxt.bybit({'enableRateLimit': True}),
            'okx': ccxt.okx({'enableRateLimit': True}),
            'kucoin': ccxt.kucoin({'enableRateLimit': True}),
            'xt': ccxt.xt({'enableRateLimit': True}),
            'gateio': ccxt.gateio({
                'enableRateLimit': True,
                'options': {
                    'adjustForTimeDifference': True
                }
            }),
            'coinex': ccxt.coinex({'enableRateLimit': True}),
            'bitget': ccxt.bitget({'enableRateLimit': True}),
            'mexc': ccxt.mexc({'enableRateLimit': True}),
            'htx': ccxt.htx({'enableRateLimit': True}), # Huobi
            'kraken': ccxt.kraken({'enableRateLimit': True}),
            'deribit': ccxt.deribit({'enableRateLimit': True}),
            'bitmex': ccxt.bitmex({'enableRateLimit': True}),
            'bingx': ccxt.bingx({'enableRateLimit': True}),
            'bitmart': ccxt.bitmart({'enableRateLimit': True}),
            'lbank': ccxt.lbank({'enableRateLimit': True}),
            'deepcoin': ccxt.deepcoin({'enableRateLimit': True}),
            'toobit': ccxt.toobit({'enableRateLimit': True}),
        }

    def fetch_funding_rates(self, symbols=None):
        """
        Fetches funding rates from all configured exchanges in parallel.
        Returns a DataFrame with the data.
        """
        all_rates = []
        import concurrent.futures

        print("Fetching funding rates in parallel...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.exchanges)) as executor:
            future_to_exchange = {
                executor.submit(self._fetch_exchange_rates, name, exchange, symbols): name 
                for name, exchange in self.exchanges.items()
            }
            
            for future in concurrent.futures.as_completed(future_to_exchange):
                name = future_to_exchange[future]
                try:
                    rates = future.result()
                    if rates:
                        all_rates.extend(rates)
                except Exception as e:
                    print(f"Error fetching from {name}: {e}")

        return pd.DataFrame(all_rates)

    def _fetch_exchange_rates(self, name, exchange, symbols):
        """Helper method to fetch rates from a single exchange safe for threading."""
        exchange_rates = []
        try:
            # Load markets if needed (check if loaded first to avoid redundant calls if possible,
            # but load_markets is usually necessary for safe multi-threaded use in some ccxt versions 
            # or if not loaded yet)
            # Safe to call repeatedly, it caches
            exchange.timeout = 15000  # 15 seconds timeout
            # Load markets if needed
            exchange.load_markets()
            
            # Force singular fetch for specific exchanges known to have bulk issues with ALL symbols
            force_singular = [] # Toobit bulk check passed in debug
            
            use_bulk = exchange.has['fetchFundingRates'] and name not in force_singular
            
            if use_bulk:
                # Set a timeout for the request if possible via params, or rely on global timeout
                # CCXT objects usually have a .timeout property
                # exchange.timeout = 10000 # 10 seconds
                
                rates = exchange.fetch_funding_rates(symbols if symbols else None)
                
                for symbol, data in rates.items():
                    funding_rate = data.get('fundingRate')
                    next_funding = data.get('nextFundingTime') or data.get('nextFundingRateTimestamp')
                    
                    if funding_rate is not None:
                        exchange_rates.append({
                            'exchange': name,
                            'symbol': symbol,
                            'funding_rate': funding_rate,
                            'next_funding_time': next_funding,
                            'timestamp': datetime.now()
                        })
            else:
                # Fallback to singular fetch (loop)
                # If symbols is None, we need to populate it with ALL swap symbols
                target_symbols = symbols
                if target_symbols is None:
                    # Get all Linear Swap symbols
                    # This depends on exchange metadata structure
                    target_symbols = [
                        s for s in exchange.markets 
                        if exchange.markets[s].get('swap') and exchange.markets[s].get('linear')
                    ]
                    # Skip if too many symbols for singular fetch to avoid stalls
                    if len(target_symbols) > 20: 
                        print(f"Skipping {name} (No bulk fetch support, >20 symbols: {len(target_symbols)})")
                        self.skipped_exchanges.append(f"{name} ({len(target_symbols)} symbols)")
                        return []

                    # Print warning if we are scanning hundreds of symbols one by one
                    print(f"Warning: Singular fetching {len(target_symbols)} symbols for {name}")

                if exchange.has['fetchFundingRate']:
                    for symbol in target_symbols:
                        try:
                            if symbol in exchange.markets:
                                data = exchange.fetch_funding_rate(symbol)
                                funding_rate = data.get('fundingRate')
                                next_funding = data.get('nextFundingTime') or data.get('nextFundingRateTimestamp')
                                
                                if funding_rate is not None:
                                    exchange_rates.append({
                                        'exchange': name,
                                        'symbol': symbol,
                                        'funding_rate': funding_rate,
                                        'next_funding_time': next_funding,
                                        'timestamp': datetime.now()
                                    })
                        except Exception as e:
                            # Log specific symbol error but don't fail the whole exchange
                            # print(f"Error fetching {symbol} from {name}: {e}") 
                            pass
                else:
                     pass # Exchange supports neither?

        except Exception as e:
            print(f"Error processing {name}: {e}")
            
        return exchange_rates

    def calculate_arbitrage(self, df):
        """
        Calculates arbitrage opportunities.
        """
        if df.empty:
            return pd.DataFrame()

        # Pivot the table to have exchanges as columns and symbols as rows
        pivot_df = df.pivot_table(
            index='symbol', 
            columns='exchange', 
            values='funding_rate'
        )
        
        opportunities = []
        
        for symbol, row in pivot_df.iterrows():
            # Get valid rates for this symbol
            rates = row.dropna()
            if len(rates) < 2:
                continue
                
            min_rate = rates.min()
            max_rate = rates.max()
            min_exchange = rates.idxmin()
            max_exchange = rates.idxmax()
            
            spread = max_rate - min_rate
            
            # Filter for meaningful spreads (User requested > 0.5% originally, lowered to 0.1%)
            if spread > 0.001: 
                opportunities.append({
                    'symbol': symbol,
                    'long_exchange': min_exchange,
                    'long_rate': min_rate,
                    'short_exchange': max_exchange,
                    'short_rate': max_rate,
                    'spread': spread,
                    'annualized_spread': spread * 3 * 365 # Approx 3 times a day * 365 days
                })
                
        if not opportunities:
            return pd.DataFrame()

        return pd.DataFrame(opportunities).sort_values('spread', ascending=False)
        
    
    def get_funding_interval(self, exchange_name, symbol):
        """
        Returns the funding interval in hours (e.g. 1, 4, 8).
        Only called for candidates to minimize API calls.
        """
        try:
            exchange = self.exchanges.get(exchange_name)
            if not exchange:
                return 8 # Default
            
            # Ensure markets loaded
            if not exchange.markets:
                exchange.load_markets()
                
            if symbol not in exchange.markets:
                return 8
                
            market = exchange.market(symbol)
            info = market.get('info', {})
            
            # 1. Check direct 'fundingInterval' (Bybit, etc.)
            # Bybit often returns minutes as string '240' or '480' or hours
            if 'fundingInterval' in info:
                try:
                    val = int(info['fundingInterval'])
                    if val > 24: # It's minutes
                        return val / 60
                    return val # It's hours
                except:
                    pass
            
            # 2. Check 'fundingInterval' in root market dict (ccxt standardized?)
            if 'fundingInterval' in market and market['fundingInterval']:
                 try:
                    # ccxt typically standardizes to milliseconds or string?
                    # verification needed, but fallback is safe.
                    pass
                 except:
                    pass

            # 3. History Inference (OKX, etc.)
            if exchange.has['fetchFundingRateHistory']:
                try:
                    # Fetch last 3 to be safe
                    hist = exchange.fetch_funding_rate_history(symbol, limit=3)
                    if len(hist) >= 2:
                        # Sort just in case
                        hist = sorted(hist, key=lambda x: x['timestamp'])
                        t_last = hist[-1]['timestamp']
                        t_prev = hist[-2]['timestamp']
                        diff_ms = t_last - t_prev
                        if diff_ms > 0:
                            return diff_ms / 3600000 # Convert ms to hours
                except Exception as e:
                    # print(f"Interval inference failed for {exchange_name}: {e}")
                    pass
                    
            return 8 # Default standard
        except Exception as e:
            # print(f"Error getting interval for {exchange_name}: {e}")
            return 8

    def get_volume_1h(self, exchange_name, symbol):
        """
        Fetches the average 1-minute volume for the last 60 minutes.
        Returns volume in USD or None if failed.
        """
        try:
            exchange = self.exchanges.get(exchange_name)
            if not exchange:
                return None
                
            if not exchange.has['fetchOHLCV']:
                return None
                
            # Fetch last 60 1-minute candles
            # limit=60 usually works
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1m', limit=60)
            
            if not ohlcv:
                return None
            
            # Sum of (Volume * ClosePrice) / Count? Or just sum of volume?
            # User asked: "VOLUMEN PROMEDIO POR MINUTO DE LOS ULTIMOS 60 MINUTOS"
            # So we calculate volume * price (quote volume) for each minute, then average it.
            
            volumes_usd = []
            for candle in ohlcv:
                # [timestamp, open, high, low, close, volume]
                close = candle[4]
                vol = candle[5]
                volumes_usd.append(close * vol)
                
            if not volumes_usd:
                return 0
                
            avg_vol = sum(volumes_usd) / len(volumes_usd)
            return avg_vol
            
        except Exception as e:
            # print(f"Error fetching volume for {symbol} on {exchange_name}: {e}")
            return None
