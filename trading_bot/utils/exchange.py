"""
Bitget Exchange Wrapper v4
════════════════════════════════════════

Fix:
✓ reduceOnly close protection
✓ futures precision handling
✓ min order size protection
✓ improved logging
"""

import ccxt
import time
from loguru import logger
from trading_bot.config import settings


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

        self._last_leverage_setup = 0


# ==========================================================
# INITIALIZATION
# ==========================================================

    def initialize(self):

        logger.info("Inizializzazione exchange Bitget...")

        if "spot" in settings.MARKET_TYPES:
            self._spot_markets = self._retry(self.spot.load_markets)
            logger.info(f"Spot: {len(self._spot_markets)} mercati")
            logger.info(f"Example markets: {list(self._spot_markets.keys())[:10]}")

        if "futures" in settings.MARKET_TYPES:
            self._futures_markets = self._retry(self.futures.load_markets)
            logger.info(f"Futures: {len(self._futures_markets)} mercati")

            self._setup_leverage()


    def _setup_leverage(self):

        lev = settings.DEFAULT_LEVERAGE

        if lev == self._last_leverage_setup:
            return

        count = 0

        for symbol in settings.FUTURES_SYMBOLS:

            if symbol not in self._futures_markets:
                continue

            try:

                self.futures.set_leverage(
                    lev,
                    symbol,
                    params={"marginMode": settings.MARGIN_MODE}
                )

                count += 1

            except Exception as e:

                if "not modified" not in str(e).lower():
                    logger.debug(f"leverage {symbol}: {e}")

        self._last_leverage_setup = lev

        logger.info(f"[EXCHANGE] Leva {lev}x su {count} futures")

    def is_valid_symbol(self, symbol, market="spot"):

        markets = self._spot_markets if market == "spot" else self._futures_markets

        return symbol in markets

# ==========================================================
# MARKET DATA
# ==========================================================

    def fetch_ohlcv(self, symbol, timeframe, limit=300, market="spot"):

        client = self.spot if market == "spot" else self.futures
        markets = self._spot_markets if market == "spot" else self._futures_markets

        # 🔧 FIX: controllo simbolo valido
        if symbol not in markets:
            logger.debug(f"[OHLCV] symbol not supported {symbol}")
            return None

        raw = self._retry(client.fetch_ohlcv, symbol, timeframe, limit=limit)

        if not raw:
            return None

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

    def fetch_ticker(self, symbol, market="spot"):

        client = self.spot if market == "spot" else self.futures
        markets = self._spot_markets if market == "spot" else self._futures_markets

        if symbol not in markets:
            logger.debug(f"[TICKER] symbol not supported {symbol}")
            return None

        t = self._retry(client.fetch_ticker, symbol)

        if not t:
            return None

        if "last" not in t or not t["last"]:
            t["last"] = t.get("close")

        return t


# ==========================================================
# BALANCE
# ==========================================================

    def fetch_balance(self, market="spot"):

        client = self.spot if market == "spot" else self.futures

        return self._retry(client.fetch_balance)


    def get_usdt_balance(self, market="spot"):

        balance = self.fetch_balance(market)

        if not balance:
            return 0

        try:
            return float(balance["free"].get("USDT", 0))
        except Exception:
            return 0


# ==========================================================
# ORDERS
# ==========================================================

    def create_market_order(self, symbol, side, amount, market="spot", params=None):

        if not settings.IS_LIVE:

            logger.info(f"[PAPER] {market} {side} {symbol} {amount}")

            return {
                "id": f"paper_{int(time.time())}",
                "status": "closed"
            }

        client = self.spot if market == "spot" else self.futures

        markets = self._spot_markets if market == "spot" else self._futures_markets

        info = markets.get(symbol)

        if not info:
            logger.error(f"[ORDER] symbol unknown {symbol}")
            return None

        try:

            amount = float(client.amount_to_precision(symbol, amount))

            limits = info.get("limits", {})
            amount_limits = limits.get("amount", {})

            min_size = float(amount_limits.get("min") or 0)

            if float(amount) < min_size:

                logger.warning(
                    f"[ORDER] size too small {symbol} {amount} < {min_size}"
                )

                return None

            logger.info(
                f"[ORDER] {market} {side} {symbol} size={amount}"
            )

            return self._retry(
                client.create_market_order,
                symbol,
                side,
                amount,
                params=params or {}
            )

        except Exception as e:

            msg = str(e)

            if "No position to close" in msg:

                logger.warning(f"[ORDER] reduceOnly no position {symbol}")

                return {
                    "id": f"phantom_close_{int(time.time())}",
                    "status": "closed"
                }

            logger.error(f"[ORDER] {symbol} {e}")

            return None


# ==========================================================
# POSITIONS
# ==========================================================

    def fetch_positions(self):

        raw = self._retry(self.futures.fetch_positions)

        return [p for p in raw if float(p.get("contracts", 0)) != 0]


# ==========================================================
# RETRY
# ==========================================================

    def _retry(self, fn, *args, **kwargs):

        last = None

        for attempt in range(1, self.RETRY_ATTEMPTS + 1):

            try:
                return fn(*args, **kwargs)

            except ccxt.RateLimitExceeded:

                wait = self.RETRY_DELAY * attempt * 3

                logger.warning(f"Rate limit {wait}s")

                time.sleep(wait)

            except ccxt.NetworkError as e:

                last = e

                logger.warning(f"network retry {attempt}")

                time.sleep(self.RETRY_DELAY * attempt)

            except ccxt.ExchangeError as e:

                logger.error(f"Exchange error: {e}")

                raise

       if last:
            raise last

        raise Exception("retry failed")