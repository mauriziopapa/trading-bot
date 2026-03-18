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

        # OHLCV cache: {(symbol, timeframe, limit, market): (timestamp, data)}
        self._ohlcv_cache = {}
        self._ohlcv_ttl = 25  # seconds — shorter than 30s scan interval


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

            # Force one-way mode (no hedge)
            self._set_one_way_mode()

            self._build_symbol_cache()


# ==========================================================
# ONE-WAY MODE
# ==========================================================

    def _set_one_way_mode(self):
        """Disable hedge mode — one-way position only."""
        try:
            self.futures.set_position_mode(False)
            logger.info("[EXCHANGE] Position mode set to one-way (no hedge)")
        except Exception as e:
            # Some accounts may already be in one-way mode
            logger.warning(f"[EXCHANGE] set_position_mode: {e}")


# ==========================================================
# BOOTSTRAP — cancel orders + close all positions
# ==========================================================

    def bootstrap_clean(self):
        """Cancel all open orders and close all positions for a clean start."""
        try:
            # Cancel all open orders
            try:
                open_orders = self._retry(self.futures.fetch_open_orders)
                for o in (open_orders or []):
                    try:
                        self._retry(
                            self.futures.cancel_order,
                            o["id"], o.get("symbol", "")
                        )
                        logger.info(f"[BOOTSTRAP] cancelled order {o['id']} {o.get('symbol')}")
                    except Exception as oe:
                        logger.warning(f"[BOOTSTRAP] cancel order failed: {oe}")
            except Exception as e:
                logger.warning(f"[BOOTSTRAP] fetch_open_orders: {e}")

            # Close all positions
            positions = self.fetch_positions()
            for p in positions:
                size = safe_float(p.get("contracts"))
                if abs(size) <= 0:
                    continue
                symbol = p["symbol"]
                pos_side = p.get("side", "")
                close_side = "sell" if pos_side == "long" else "buy"
                try:
                    self._retry(
                        self.futures.create_market_order,
                        symbol, close_side, abs(size),
                        params={"reduceOnly": True}
                    )
                    logger.info(f"[BOOTSTRAP] closed {symbol} {pos_side} size={size}")
                except Exception as pe:
                    logger.error(f"[BOOTSTRAP] close {symbol} failed: {pe}")

            logger.warning("[BOOTSTRAP] exchange fully cleaned")
        except Exception as e:
            logger.error(f"[BOOTSTRAP] error: {e}")


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

            # Check cache
            cache_key = (symbol, timeframe, limit, market)
            now = time.time()
            cached = self._ohlcv_cache.get(cache_key)
            if cached and (now - cached[0]) < self._ohlcv_ttl:
                return cached[1]

            raw = self._retry(
                client.fetch_ohlcv,
                symbol,
                timeframe,
                limit=limit
            )

            if not raw:
                return []

            result = [
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

            # Store in cache
            self._ohlcv_cache[cache_key] = (now, result)

            # Evict stale entries periodically
            if len(self._ohlcv_cache) > 50:
                stale = [k for k, v in self._ohlcv_cache.items() if now - v[0] > self._ohlcv_ttl * 2]
                for k in stale:
                    del self._ohlcv_cache[k]

            return result

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


    def fetch_tickers_batch(self, symbols, market="futures"):
        """Batch-fetch tickers for multiple symbols in one API call."""

        try:

            client = self.spot if market == "spot" else self.futures
            normalized = [self._normalize_symbol(s, market) for s in symbols]
            valid = [s for s in normalized if self.is_symbol_supported(s, market)]

            if not valid:
                return {}

            raw = self._retry(client.fetch_tickers, valid)

            if not raw:
                return {}

            result = {}
            for sym, t in raw.items():
                result[sym] = {
                    "last": safe_float(t.get("last") or t.get("close")),
                    "bid": safe_float(t.get("bid")),
                    "ask": safe_float(t.get("ask")),
                    "volume": safe_float(t.get("quoteVolume")),
                }

            return result

        except Exception as e:

            logger.debug(f"[TICKERS BATCH] {e}")
            return {}


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

        logger.info(f"[ORDER REQUEST] {market} {side} {symbol} amount={amount}")

        # ==========================================================
        # PAPER MODE
        # ==========================================================
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

            # ==========================================================
            # NORMALIZE SYMBOL
            # ==========================================================
            symbol = self._normalize_symbol(symbol, market)

            if symbol not in markets:
                logger.error(f"[ORDER] unknown symbol {symbol}")
                return None

            amount = safe_float(amount)

            if amount <= 0:
                logger.error(f"[ORDER] invalid amount {amount}")
                return None

            # ==========================================================
            # PRECISION HANDLING
            # ==========================================================
            try:
                amount = float(client.amount_to_precision(symbol, amount))
            except Exception as e:
                logger.error(f"[ORDER] precision error {symbol} {e}")
                return None

            # ==========================================================
            # MIN SIZE CHECK
            # ==========================================================
            min_size = safe_float(markets[symbol]["limits"]["amount"]["min"])

            if amount < min_size:
                logger.warning(f"[ORDER] too small {symbol} amount={amount} min={min_size}")
                return None

            # ==========================================================
            # MIN NOTIONAL CHECK (importantissimo su futures)
            # ==========================================================
            ticker = client.fetch_ticker(symbol)
            price = safe_float(ticker.get("last"))

            if price <= 0:
                logger.error(f"[ORDER] invalid price {symbol}")
                return None

            notional = amount * price

            if notional < 5:  # soglia sicurezza (Bitget spesso > 5 USDT)
                logger.warning(f"[ORDER] notional too small {symbol} value={notional}")
                return None

            # ==========================================================
            # FUTURES PARAMS
            # One-way mode: no holdSide needed.
            # For closing: caller passes reduceOnly=True
            # For opening: just marginMode
            # ==========================================================
            params = params or {}

            if market == "futures":
                if "marginMode" not in params:
                    params["marginMode"] = "cross"

            # ==========================================================
            # EXECUTION (RETRY SAFE)
            # ==========================================================
            order = None

            try:

                order = self._retry(
                    client.create_market_order,
                    symbol,
                    side,
                    amount,
                    params=params
                )

            except Exception as e:
                logger.error(
                    f"[ORDER ERROR] {symbol} | side={side} | amount={amount} | params={params} | err={e}"
                )
                return None

            if not order:
                logger.error(f"[ORDER FAILED] {symbol} no response")
                return None

            # ==========================================================
            # RESPONSE NORMALIZATION
            # ==========================================================
            filled = safe_float(order.get("filled") or amount)

            logger.info(
                f"[ORDER SUCCESS] {symbol} | side={side} | filled={filled}"
            )

            return {
                "id": order.get("id"),
                "filled": filled,
                "status": order.get("status", "unknown")
            }

        except Exception as e:

            logger.error(
                f"[ORDER FATAL] {symbol} | side={side} | amount={amount} | err={e}"
            )

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