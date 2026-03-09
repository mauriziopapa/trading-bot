"""
Bitget Exchange Wrapper
Gestisce connessioni separate per Spot e Futures (Swap),
retry automatico, rate limiting e normalizzazione simboli.
"""

import ccxt
import time
from loguru import logger
from trading_bot.config import settings


class BitgetExchange:
    """
    Singleton wrapper attorno a ccxt.bitget.
    Espone metodi unificati per spot e futures.
    """

    RETRY_ATTEMPTS = 3
    RETRY_DELAY    = 2   # secondi tra retry

    def __init__(self):
        common_config = {
            "apiKey":    settings.BITGET_API_KEY,
            "secret":    settings.BITGET_API_SECRET,
            "password":  settings.BITGET_API_PASSPHRASE,   # obbligatorio su Bitget
            "enableRateLimit": True,
            "rateLimit": 200,                           # ms tra richieste
        }

        # Client Spot
        self.spot = ccxt.bitget({
            **common_config,
            "options": {"defaultType": "spot"},
        })

        # Client Futures Perpetui (swap)
        self.futures = ccxt.bitget({
            **common_config,
            "options": {
                "defaultType": "swap",
                "defaultMarginMode": settings.MARGIN_MODE,
            },
        })

        self._spot_markets    = {}
        self._futures_markets = {}

    # ─── Init & Market Loading ────────────────────────────────────────────────

    def initialize(self):
        """Carica i mercati disponibili e configura la leva."""
        logger.info("Inizializzazione exchange Bitget...")

        if "spot" in settings.MARKET_TYPES:
            self._spot_markets = self._retry(self.spot.load_markets)
            logger.info(f"Spot: {len(self._spot_markets)} mercati caricati")
            logger.info(f"Spot: {self._spot_markets} mercati caricati")
        if "futures" in settings.MARKET_TYPES:
            self._futures_markets = self._retry(self.futures.load_markets)
            logger.info(f"Futures: {len(self._futures_markets)} mercati caricati")
            self._setup_leverage()

    def _setup_leverage(self):
        """Imposta leva e margin mode per tutti i simboli futures configurati."""
        for symbol in settings.FUTURES_SYMBOLS:
            try:
                self.futures.set_leverage(
                    settings.DEFAULT_LEVERAGE,
                    symbol,
                    params={"marginMode": settings.MARGIN_MODE}
                )
                logger.debug(f"Leva {settings.DEFAULT_LEVERAGE}x impostata per {symbol}")
            except Exception as e:
                logger.warning(f"Setup leva {symbol}: {e}")

    # ─── OHLCV ───────────────────────────────────────────────────────────────

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 300, market: str = "spot"):
        """Ritorna candele OHLCV come lista di dict."""
        client = self.spot if market == "spot" else self.futures
        raw = self._retry(client.fetch_ohlcv, symbol, timeframe, limit=limit)
        return [
            {"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
            for r in raw
        ]

    # ─── Ticker & Orderbook ──────────────────────────────────────────────────

    def fetch_ticker(self, symbol: str, market: str = "spot") -> dict:
        client = self.spot if market == "spot" else self.futures
        return self._retry(client.fetch_ticker, symbol)

    def fetch_order_book(self, symbol: str, limit: int = 20, market: str = "spot") -> dict:
        client = self.spot if market == "spot" else self.futures
        return self._retry(client.fetch_order_book, symbol, limit)

    # ─── Account & Balance ───────────────────────────────────────────────────

    def fetch_balance(self, market: str = "spot") -> dict:
        client = self.spot if market == "spot" else self.futures
        return self._retry(client.fetch_balance)

    def get_usdt_balance(self, market: str = "spot") -> float:
        balance = self.fetch_balance(market)
        return float(balance.get("USDT", {}).get("free", 0))

    def fetch_positions(self) -> list:
        """Ritorna tutte le posizioni futures aperte."""
        raw = self._retry(self.futures.fetch_positions)
        return [p for p in raw if float(p.get("contracts", 0)) != 0]

    # ─── Orders ──────────────────────────────────────────────────────────────

    def create_market_order(self, symbol: str, side: str, amount: float,
                            market: str = "spot", params: dict = None) -> dict | None:
        if not settings.IS_LIVE:
            logger.info(f"[PAPER] {market.upper()} {side.upper()} {amount:.6f} {symbol}")
            return {"id": f"paper_{int(time.time())}", "status": "closed",
                    "symbol": symbol, "side": side, "amount": amount}

        client = self.spot if market == "spot" else self.futures
        return self._retry(client.create_market_order, symbol, side, amount, params=params or {})

    def create_limit_order(self, symbol: str, side: str, amount: float, price: float,
                           market: str = "spot", params: dict = None) -> dict | None:
        if not settings.IS_LIVE:
            logger.info(f"[PAPER] LIMIT {market.upper()} {side.upper()} {amount:.6f} {symbol} @ {price}")
            return {"id": f"paper_{int(time.time())}", "status": "open",
                    "symbol": symbol, "side": side, "amount": amount, "price": price}

        client = self.spot if market == "spot" else self.futures
        return self._retry(client.create_limit_order, symbol, side, amount, price, params=params or {})

    def cancel_order(self, order_id: str, symbol: str, market: str = "spot"):
        client = self.spot if market == "spot" else self.futures
        return self._retry(client.cancel_order, order_id, symbol)

    def fetch_open_orders(self, symbol: str, market: str = "spot") -> list:
        client = self.spot if market == "spot" else self.futures
        return self._retry(client.fetch_open_orders, symbol)

    # ─── Utility ─────────────────────────────────────────────────────────────

    def get_min_order_size(self, symbol: str, market: str = "spot") -> float:
        markets = self._spot_markets if market == "spot" else self._futures_markets
        info = markets.get(symbol, {})
        return float(info.get("limits", {}).get("amount", {}).get("min", 0.001))

    def price_precision(self, symbol: str, market: str = "spot") -> int:
        markets = self._spot_markets if market == "spot" else self._futures_markets
        info = markets.get(symbol, {})
        return int(info.get("precision", {}).get("price", 2))

    # ─── Retry Logic ─────────────────────────────────────────────────────────

    def _retry(self, fn, *args, **kwargs):
        last_err = None
        for attempt in range(1, self.RETRY_ATTEMPTS + 1):
            try:
                return fn(*args, **kwargs)
            except ccxt.RateLimitExceeded:
                wait = self.RETRY_DELAY * attempt * 3
                logger.warning(f"Rate limit — attendo {wait}s")
                time.sleep(wait)
            except ccxt.NetworkError as e:
                logger.warning(f"Network error (tentativo {attempt}): {e}")
                time.sleep(self.RETRY_DELAY * attempt)
                last_err = e
            except ccxt.ExchangeError as e:
                logger.error(f"Exchange error: {e}")
                raise
        raise last_err or Exception("Max retry raggiunti")
