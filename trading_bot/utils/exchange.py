"""
Bitget Exchange Wrapper v3
═══════════════════════════════════════════════════════════════
FIX CRITICI:
  ✓ Market BUY spot: Bitget richiede il prezzo per calcolare il costo.
    Aggiunta opzione createMarketBuyOrderRequiresPrice=False +
    fallback: converte amount in cost (amount * price) automaticamente.
  ✓ Leverage re-sync quando cambia dalla dashboard.
  ✓ Logging dettagliato sugli errori ordine per debugging.
"""

import ccxt
import time
from loguru import logger
from trading_bot.config import settings


class BitgetExchange:
    RETRY_ATTEMPTS = 3
    RETRY_DELAY    = 2

    def __init__(self):
        common_config = {
            "apiKey":    settings.BITGET_API_KEY,
            "secret":    settings.BITGET_API_SECRET,
            "password":  settings.BITGET_API_PASSPHRASE,
            "enableRateLimit": True,
            "rateLimit": 200,
        }

        # ── Client Spot ──────────────────────────────────────────────────
        self.spot = ccxt.bitget({
            **common_config,
            "options": {
                "defaultType": "spot",
                # FIX CRITICO: Bitget spot market BUY richiede il prezzo
                # per calcolare il costo totale. Con questa opzione a False,
                # ccxt accetta l'amount direttamente come quantità dell'asset
                # e lo converte internamente.
                "createMarketBuyOrderRequiresPrice": False,
            },
        })

        # ── Client Futures ───────────────────────────────────────────────
        self.futures = ccxt.bitget({
            **common_config,
            "options": {
                "defaultType": "swap",
                "defaultMarginMode": settings.MARGIN_MODE,
            },
        })

        self._spot_markets    = {}
        self._futures_markets = {}
        self._last_leverage_setup: int = 0  # traccia ultima leva impostata

    # ─── Init & Market Loading ────────────────────────────────────────────────

    def initialize(self):
        logger.info("Inizializzazione exchange Bitget...")

        if "spot" in settings.MARKET_TYPES:
            self._spot_markets = self._retry(self.spot.load_markets)
            logger.info(f"Spot: {len(self._spot_markets)} mercati caricati")

        if "futures" in settings.MARKET_TYPES:
            self._futures_markets = self._retry(self.futures.load_markets)
            logger.info(f"Futures: {len(self._futures_markets)} mercati caricati")
            self._setup_leverage()

    def _setup_leverage(self):
        """
        Imposta leva e margin mode per tutti i simboli futures.
        Con AUTO mode ci possono essere 100+ simboli — set leverage
        solo su quelli effettivamente presenti nei mercati caricati.
        """
        current_lev = settings.DEFAULT_LEVERAGE
        if current_lev == self._last_leverage_setup:
            return
        symbols = settings.FUTURES_SYMBOLS
        done = 0
        for symbol in symbols:
            if symbol not in self._futures_markets:
                continue
            try:
                self.futures.set_leverage(
                    current_lev, symbol,
                    params={"marginMode": settings.MARGIN_MODE}
                )
                done += 1
            except Exception as e:
                if "leverage not modified" not in str(e).lower() and "not need" not in str(e).lower():
                    logger.debug(f"Leva {symbol}: {e}")
        self._last_leverage_setup = current_lev
        logger.info(f"[EXCHANGE] Leva {current_lev}x su {done}/{len(symbols)} futures")

    # ─── OHLCV ───────────────────────────────────────────────────────────────

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 300, market: str = "spot"):
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
        raw = self._retry(self.futures.fetch_positions)
        return [p for p in raw if float(p.get("contracts", 0)) != 0]

    def transfer_spot_to_futures(self, amount: float) -> bool:
        """
        Trasferisce USDT da Spot a Futures (USDT-M).
        Bitget API: transfer tra account interni.
        Ritorna True se il trasferimento è riuscito.
        """
        if amount < 1:
            return False
        if not settings.IS_LIVE:
            logger.info(f"[PAPER] Transfer {amount:.2f} USDT spot→futures")
            return True
        try:
            result = self.spot.transfer("USDT", amount, "spot", "swap")
            logger.info(f"[TRANSFER] ✅ {amount:.2f} USDT spot → futures | ID={result.get('id','?')}")
            return True
        except Exception as e:
            # Fallback: prova con params Bitget specifici
            try:
                result = self.spot.private_post_spot_wallet_transfer({
                    "coin": "USDT",
                    "fromType": "spot",
                    "toType": "usdt_mix",  # Bitget v2: usdt_mix = futures USDT-M
                    "amount": str(amount),
                })
                logger.info(f"[TRANSFER] ✅ {amount:.2f} USDT spot → futures (v2)")
                return True
            except Exception as e2:
                logger.warning(f"[TRANSFER] ❌ {amount:.2f} USDT: {e} / {e2}")
                return False

    def auto_rebalance(self, keep_spot_usdt: float = 5.0) -> dict:
        """
        Trasferisce automaticamente USDT liberi dallo spot ai futures.
        Mantiene 'keep_spot_usdt' sul wallet spot per eventuali fee.
        
        Chiamato periodicamente dallo scheduler (ogni 10 min).
        Ritorna dict con dettagli del trasferimento.
        """
        result = {"transferred": 0, "spot_before": 0, "spot_after": 0, "futures_after": 0}
        try:
            spot_free = self.get_usdt_balance("spot")
            result["spot_before"] = round(spot_free, 2)

            available = spot_free - keep_spot_usdt
            if available < 2:  # meno di 2 USDT liberi → non trasferire
                return result

            # Trasferisci
            amount = round(available, 2)
            ok = self.transfer_spot_to_futures(amount)

            if ok:
                result["transferred"] = amount
                result["spot_after"] = round(spot_free - amount, 2)
                result["futures_after"] = round(self.get_usdt_balance("futures"), 2)
                logger.info(
                    f"[REBALANCE] {amount:.2f} USDT spot→futures | "
                    f"Spot: {spot_free:.2f}→{result['spot_after']:.2f} | "
                    f"Futures: {result['futures_after']:.2f}"
                )
        except Exception as e:
            logger.warning(f"[REBALANCE] Errore: {e}")

        return result

    # ─── Orders ──────────────────────────────────────────────────────────────

    def create_market_order(self, symbol: str, side: str, amount: float,
                            market: str = "spot", params: dict = None) -> dict | None:
        """
        FIX CRITICO: Bitget spot market BUY richiede il costo totale in USDT,
        non la quantità dell'asset. Con createMarketBuyOrderRequiresPrice=False,
        ccxt converte automaticamente. Ma come fallback aggiuntivo:
        - Se side=buy e market=spot, passiamo anche il prezzo nel params
          per sicurezza su tutte le versioni di ccxt.
        """
        if not settings.IS_LIVE:
            logger.info(f"[PAPER] {market.upper()} {side.upper()} {amount:.6f} {symbol}")
            return {"id": f"paper_{int(time.time())}", "status": "closed",
                    "symbol": symbol, "side": side, "amount": amount}

        client = self.spot if market == "spot" else self.futures
        order_params = dict(params or {})

        # ── FIX: Spot market BUY — passa il costo in USDT ────────────────
        # Bitget spot market BUY vuole sapere QUANTO USDT spendere,
        # non quanti token comprare. Recuperiamo il prezzo corrente
        # e convertiamo amount → cost.
        if market == "spot" and side == "buy":
            try:
                ticker = self.fetch_ticker(symbol, market)
                current_price = float(ticker.get("last", 0) or ticker.get("close", 0))
                if current_price > 0:
                    # cost = quantità_token × prezzo
                    cost = round(amount * current_price, 4)
                    logger.info(
                        f"[ORDER] Spot BUY {symbol}: amount={amount:.6f} × "
                        f"price={current_price:.4f} = cost={cost:.4f} USDT"
                    )
                    # Con createMarketBuyOrderRequiresPrice=False,
                    # passiamo il cost come amount
                    try:
                        order = self._retry(
                            client.create_market_buy_order,
                            symbol, cost, order_params
                        )
                        return order
                    except (AttributeError, TypeError):
                        # Fallback: usa create_order con price
                        order = self._retry(
                            client.create_order,
                            symbol, "market", side, amount,
                            current_price, order_params
                        )
                        return order
                else:
                    logger.error(f"[ORDER] Prezzo non disponibile per {symbol}")
                    return None
            except Exception as e:
                logger.error(f"[ORDER] Spot BUY {symbol} fallito: {e}")
                return None

        # ── FIX: Spot SELL — verifica balance reale del token ────────────
        # Bitget detrae le fee dal token comprato (0.1%).
        # Il bot traccia size=2.907869 ma il balance reale è ~2.904.
        # Tentare di vendere più di quanto si possiede → loop infinito.
        if market == "spot" and side == "sell":
            try:
                base_asset = symbol.split("/")[0]
                balance = self.fetch_balance("spot")
                available = float(balance.get(base_asset, {}).get("free", 0))
                if available <= 0:
                    logger.warning(f"[ORDER] Spot SELL {symbol}: 0 {base_asset} disponibili — rimuovo posizione fantasma")
                    return {"id": f"phantom_close_{int(time.time())}", "status": "closed",
                            "symbol": symbol, "side": side, "amount": 0, "info": "phantom_removed"}
                if available < amount * 0.95:
                    logger.warning(
                        f"[ORDER] Spot SELL {symbol}: richiesti {amount:.6f} ma disponibili {available:.6f} "
                        f"({base_asset}) — uso balance reale (fee detratte)"
                    )
                    amount = available
            except Exception as e:
                logger.warning(f"[ORDER] Balance check {symbol}: {e}")

        # ── Tutti gli altri ordini (spot SELL, futures BUY/SELL) ──────────
        try:
            return self._retry(
                client.create_market_order,
                symbol, side, amount, params=order_params
            )
        except Exception as e:
            logger.error(f"[ORDER] {market} {side} {symbol} amount={amount:.6f}: {e}")
            return None

    def is_valid_symbol(self, symbol: str, market: str = "spot") -> bool:
        """Verifica se un simbolo esiste nei mercati caricati da Bitget."""
        markets = self._spot_markets if market == "spot" else self._futures_markets
        return symbol in markets

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

    def get_min_notional(self, symbol: str, market: str = "spot") -> float:
        markets = self._spot_markets if market == "spot" else self._futures_markets
        info = markets.get(symbol, {})
        cost_min = info.get("limits", {}).get("cost", {}).get("min", 0)
        if cost_min and float(cost_min) > 0:
            return float(cost_min)
        price_min = float(info.get("limits", {}).get("price", {}).get("min", 0) or 0)
        amount_min = float(info.get("limits", {}).get("amount", {}).get("min", 0.001) or 0.001)
        if price_min > 0:
            return price_min * amount_min
        return 5.0

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
