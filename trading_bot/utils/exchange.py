"""
Bitget Exchange Wrapper v8.0 PRO HARDENED
════════════════════════════════════════

✔ Backward compatible
✔ Symbol validation (CRITICAL)
✔ Liquidity filter
✔ Safe normalization
✔ Precision hardened
✔ Sniper ready
"""

import ccxt
import time
from loguru import logger
from trading_bot.config import settings


# ==========================================================
# SAFE FLOAT
# ==========================================================

def safe_float(x, default=0.0):
    try:
        return float(x) if x is not None else default
    except:
        return default


class BitgetExchange:

    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2

    def __init__(self):

        common_config = {
            "apiKey": settings.BITGET_API_KEY,
            "secret": settings.BITGET_API_SECRET,
            "password": settings.BITGET_API_PASSPHRASE,
            "enableRateLimit": True,
            "rateLimit": 200,
        }

        self.spot = ccxt.bitget({
            **common_config,
            "options": {
                "defaultType": "spot",
                "createMarketBuyOrderRequiresPrice": False,
            },
        })

        self.futures = ccxt.bitget({
            **common_config,
            "options": {
                "defaultType": "swap",
                "defaultMarginMode": settings.MARGIN_MODE,
            },
        })

        self._spot_markets = {}
        self._futures_markets = {}

        # 🔥 CACHE UTILI
        self._valid_symbols = set()
        self._liquid_symbols = set()


# ==========================================================
# INITIALIZATION
# ==========================================================

    def initialize(self):

        logger.info("[EXCHANGE] Initializing Bitget...")

        if "spot" in settings.MARKET_TYPES:
            self._spot_markets = self._retry(self.spot.load_markets)

        if "futures" in settings.MARKET_TYPES:
            self._futures_markets = self._retry(self.futures.load_markets)

            logger.info(f"[EXCHANGE] Futures markets: {len(self._futures_markets)}")

            self._build_symbol_cache()


# ==========================================================
# SYMBOL CACHE (CRITICAL)
# ==========================================================

    def _build_symbol_cache(self):

        self._valid_symbols = set(self._futures_markets.keys())

        # 🔥 liquidi (sniper)
        for s, info in self._futures_markets.items():

            try:
                if info.get("quote") != "USDT":
                    continue

                if not info.get("active"):
                    continue

                # opzionale: filtra solo perpetual
                if info.get("type") != "swap":
                    continue

                self._liquid_symbols.add(s)

            except:
                continue

        logger.info(f"[EXCHANGE] Valid symbols: {len(self._valid_symbols)}")
        logger.info(f"[EXCHANGE] Liquid symbols: {len(self._liquid_symbols)}")


# ==========================================================
# SYMBOL NORMALIZATION (ROBUST)
# ==========================================================

    def _normalize_symbol(self, symbol, market):

        if market == "futures":

            if ":" in symbol:
                return symbol

            if "/USDT" in symbol:
                return f"{symbol}:USDT"

            if symbol.endswith("USDT"):
                base = symbol.replace("USDT", "")
                return f"{base}/USDT:USDT"

        return symbol


# ==========================================================
# VALIDATION (NEW)
# ==========================================================

    def is_symbol_supported(self, symbol, market="futures"):

        symbol = self._normalize_symbol(symbol, market)

        if market == "futures":
            return symbol in self._valid_symbols

        return True


    def is_symbol_liquid(self, symbol, market="futures"):

        symbol = self._normalize_symbol(symbol, market)

        return symbol in self._liquid_symbols


# ==========================================================
# MARKET DATA
# ==========================================================

    def fetch_ohlcv(self, symbol, timeframe, limit=300, market="futures"):

        try:

            client = self.spot if market == "spot" else self.futures
            symbol = self._normalize_symbol(symbol, market)

            if not self.is_symbol_supported(symbol, market):
                return []

            raw = self._retry(
                client.fetch_ohlcv,
                symbol,
                timeframe,
                limit=limit
            )

            if not raw:
                return []

            return [
                {
                    "ts": r[0],
                    "open": r[1],
                    "high": r[2],
                    "low": r[3],
                    "close": r[4],
                    "volume": r[5],
                }
                for r in raw
            ]

        except Exception as e:

            logger.debug(f"[OHLCV] {symbol} {e}")
            return []


    def fetch_ticker(self, symbol, market="spot"):

        try:

            client = self.spot if market == "spot" else self.futures
            symbol = self._normalize_symbol(symbol, market)

            if not self.is_symbol_supported(symbol, market):
                return None

            t = self._retry(client.fetch_ticker, symbol)

            if not t:
                return None

            return {
                "last": safe_float(t.get("last") or t.get("close")),
                "bid": safe_float(t.get("bid")),
                "ask": safe_float(t.get("ask")),
                "volume": safe_float(t.get("quoteVolume")),
            }

        except Exception as e:

            logger.debug(f"[TICKER] {symbol} {e}")
            return None


# ==========================================================
# SNIPER ASSET SELECTION (NEW)
# ==========================================================

    def get_top_liquid_symbols(self, limit=20):

        symbols = list(self._liquid_symbols)

        # fallback semplice (puoi migliorare con volumi)
        return symbols[:limit]


# ==========================================================
# BALANCE
# ==========================================================

    def fetch_balance(self, market="spot"):

        client = self.spot if market == "spot" else self.futures

        try:
            return self._retry(client.fetch_balance)
        except Exception as e:
            logger.error(f"[BALANCE] {e}")
            return {}


    def get_usdt_balance(self, market="spot"):

        balance = self.fetch_balance(market)

        if not balance:
            return 0

        try:
            return safe_float(balance["USDT"]["free"])
        except:
            return 0


# ==========================================================
# ORDERS
# ==========================================================

    def create_market_order(self, symbol, side, amount, market="futures", params=None):

        if not settings.IS_LIVE:

            logger.info(f"[PAPER] {market} {side} {symbol} {amount}")

            return {
                "id": f"paper_{int(time.time())}",
                "filled": safe_float(amount),
                "status": "closed"
            }

        try:

            client = self.futures if market == "futures" else self.spot
            markets = self._futures_markets if market == "futures" else self._spot_markets

            symbol = self._normalize_symbol(symbol, market)

            if symbol not in markets:
                logger.error(f"[ORDER] unknown symbol {symbol}")
                return None

            amount = safe_float(amount)

            if amount <= 0:
                return None

            # precision
            amount = float(client.amount_to_precision(symbol, amount))

            # min size
            min_size = safe_float(markets[symbol]["limits"]["amount"]["min"])

            if amount < min_size:
                logger.warning(f"[ORDER] too small {symbol}")
                return None

            order = self._retry(
                client.create_market_order,
                symbol,
                side,
                amount,
                params=params or {}
            )

            if not order:
                return None

            return {
                "id": order.get("id"),
                "filled": safe_float(order.get("filled") or amount),
                "status": order.get("status", "unknown")
            }

        except Exception as e:

            logger.error(f"[ORDER] {symbol} {e}")
            return None


# ==========================================================
# POSITIONS
# ==========================================================

    def fetch_positions(self):

        try:

            raw = self._retry(self.futures.fetch_positions)

            return [
                p for p in raw
                if safe_float(p.get("contracts")) != 0
            ]

        except Exception as e:

            logger.error(f"[POSITIONS] {e}")
            return []


# ==========================================================
# RETRY ENGINE
# ==========================================================

    def _retry(self, fn, *args, **kwargs):

        last = None

        for attempt in range(1, self.RETRY_ATTEMPTS + 1):

            try:
                return fn(*args, **kwargs)

            except ccxt.RateLimitExceeded:

                wait = self.RETRY_DELAY * attempt * 2
                logger.warning(f"[RETRY] rate limit {wait}s")
                time.sleep(wait)

            except ccxt.NetworkError as e:

                last = e
                logger.warning(f"[RETRY] network {attempt}")
                time.sleep(self.RETRY_DELAY * attempt)

            except ccxt.ExchangeError as e:

                logger.error(f"[EXCHANGE ERROR] {e}")
                raise

        if last:
            raise last

        raise Exception("retry failed")