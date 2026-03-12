"""
Emerging Coins Scanner
Identifica criptovalute emergenti con:
- Volume surge nelle ultime 24h (>300% della media settimanale)
- Price momentum positivo (>5% in 24h)
- Listing recente su Bitget (< 30 giorni)
- Capitalizzazione in crescita

Usa l'API pubblica di CoinGecko (no auth) + dati Bitget.
"""

import time
import requests
from typing import Optional
from loguru import logger
from trading_bot.config import settings


COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Cache per evitare troppe chiamate
_cache: dict = {}
_cache_ts: float = 0
CACHE_TTL = 300   # 5 minuti


class EmergingScanner:
    """
    Scansiona il mercato alla ricerca di criptovalute emergenti
    da aggiungere dinamicamente alla watchlist del bot.
    """

    def __init__(self,
                 min_volume_usd: float = 5_000_000,    # volume 24h minimo
                 min_price_change_24h: float = 5.0,    # % cambio prezzo minimo
                 min_volume_surge: float = 2.5,        # volume 24h / volume 7d medio
                 max_results: int = 10):
        self.min_volume_usd       = min_volume_usd
        self.min_price_change_24h = min_price_change_24h
        self.min_volume_surge     = min_volume_surge
        self.max_results          = max_results

        # Cache dei risultati
        self._last_scan: list[dict] = []
        self._last_scan_ts: float   = 0
        self._scan_ttl: float       = 600   # 10 minuti tra scan

    # ─── Public API ──────────────────────────────────────────────────────────

    def scan(self, force: bool = False) -> list[dict]:
        """
        Ritorna lista di coin emergenti.
        Usa cache se disponibile (TTL 10 min).
        """
        now = time.time()
        if not force and (now - self._last_scan_ts) < self._scan_ttl:
            return self._last_scan

        logger.info("[EMERGING] Avvio scan criptovalute emergenti...")
        results = []

        # 1. Trending da CoinGecko
        trending = self._fetch_trending()
        results.extend(trending)

        # 2. Top gainers 24h su Bitget
        gainers = self._fetch_bitget_gainers()
        results.extend(gainers)

        # 3. Deduplica e filtra per criteri minimi
        seen = set()
        filtered = []
        for coin in results:
            key = coin.get("symbol", "").upper()
            if key and key not in seen:
                seen.add(key)
                if self._passes_filters(coin):
                    filtered.append(coin)

        # Ordina per score complessivo
        filtered.sort(key=lambda x: x.get("score", 0), reverse=True)
        self._last_scan     = filtered[:self.max_results]
        self._last_scan_ts  = now

        logger.info(f"[EMERGING] Trovate {len(self._last_scan)} coin emergenti")
        for c in self._last_scan:
            logger.info(
                f"  {c['symbol']:12} | vol24h=${c.get('volume_24h_usd', 0)/1e6:.1f}M"
                f" | chg={c.get('price_change_24h', 0):+.1f}%"
                f" | score={c.get('score', 0):.0f}"
            )

        return self._last_scan

    def get_spot_symbols(self) -> list[str]:
        """Ritorna i simboli emergenti in formato Bitget spot (es. BTC/USDT)."""
        coins = self.scan()
        symbols = []
        for c in coins:
            sym = c.get("symbol", "").upper()
            if sym:
                bitget_sym = f"{sym}/USDT"
                symbols.append(bitget_sym)
        return symbols

    def get_futures_symbols(self) -> list[str]:
        """Ritorna i simboli emergenti in formato Bitget futures."""
        coins = self.scan()
        symbols = []
        for c in coins:
            sym = c.get("symbol", "").upper()
            if sym:
                bitget_sym = f"{sym}/USDT:USDT"
                symbols.append(bitget_sym)
        return symbols

    # ─── Data Sources ────────────────────────────────────────────────────────

    def _fetch_trending(self) -> list[dict]:
        """Fetch trending coins da CoinGecko (top 7 trending)."""
        try:
            r = requests.get(
                f"{COINGECKO_BASE}/search/trending",
                timeout=10,
                headers={"Accept": "application/json"}
            )
            if r.status_code != 200:
                return []

            data = r.json()
            coins = data.get("coins", [])
            results = []

            # Fetch dettagli per ogni trending coin
            ids = [c["item"]["id"] for c in coins[:7]]
            if not ids:
                return []

            detail_r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "ids": ",".join(ids),
                    "order": "volume_desc",
                    "sparkline": False,
                    "price_change_percentage": "24h",
                },
                timeout=10
            )
            if detail_r.status_code != 200:
                return []

            for coin in detail_r.json():
                results.append(self._normalize_coingecko(coin, source="trending"))

            return results

        except Exception as e:
            logger.debug(f"[EMERGING] CoinGecko trending error: {e}")
            return []

    def _fetch_bitget_gainers(self) -> list[dict]:
        """Fetch top gainers 24h da Bitget public API."""
        try:
            r = requests.get(
                "https://api.bitget.com/api/v2/spot/market/tickers",
                timeout=10,
                headers={"Accept": "application/json"}
            )
            if r.status_code != 200:
                return []

            data = r.json()
            tickers = data.get("data", [])

            # Filtra solo coppie USDT con volume significativo
            usdt_tickers = [
                t for t in tickers
                if t.get("symbol", "").endswith("USDT")
                and float(t.get("usdtVol", 0)) >= self.min_volume_usd
            ]

            # Ordina per cambio percentuale 24h
            usdt_tickers.sort(
                key=lambda x: float(x.get("changeUtc24h", 0) or 0),
                reverse=True
            )

            results = []
            for t in usdt_tickers[:20]:
                change_24h = float(t.get("changeUtc24h", 0) or 0) * 100
                volume_24h = float(t.get("usdtVol", 0) or 0)
                symbol_raw = t.get("symbol", "")
                symbol     = symbol_raw.replace("USDT", "").strip()

                if change_24h >= self.min_price_change_24h:
                    results.append({
                        "symbol":           symbol,
                        "price":            float(t.get("lastPr", 0) or 0),
                        "price_change_24h": round(change_24h, 2),
                        "volume_24h_usd":   volume_24h,
                        "volume_surge":     self._estimate_surge(volume_24h),
                        "source":           "bitget_gainers",
                        "score":            self._compute_score(change_24h, volume_24h, None),
                    })

            return results

        except Exception as e:
            logger.debug(f"[EMERGING] Bitget gainers error: {e}")
            return []

    def _fetch_new_listings(self) -> list[dict]:
        """
        Fetch nuove listing CoinGecko (ultime 2 settimane).
        Usato per identificare coin recentemente listate.
        """
        try:
            r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "id_asc",
                    "per_page": 50,
                    "page": 1,
                    "sparkline": False,
                    "price_change_percentage": "24h",
                },
                timeout=10
            )
            if r.status_code != 200:
                return []

            results = []
            for coin in r.json():
                results.append(self._normalize_coingecko(coin, source="new_listing"))
            return results

        except Exception as e:
            logger.debug(f"[EMERGING] New listings error: {e}")
            return []

    # ─── Helpers ─────────────────────────────────────────────────────────────

    def _normalize_coingecko(self, coin: dict, source: str) -> dict:
        """Normalizza dati CoinGecko al formato interno."""
        change_24h = coin.get("price_change_percentage_24h") or 0
        volume_24h = coin.get("total_volume") or 0
        market_cap = coin.get("market_cap") or 0

        return {
            "symbol":           (coin.get("symbol") or "").upper(),
            "name":             coin.get("name", ""),
            "price":            coin.get("current_price") or 0,
            "price_change_24h": round(float(change_24h), 2),
            "volume_24h_usd":   float(volume_24h),
            "market_cap_usd":   float(market_cap),
            "volume_surge":     self._estimate_surge(float(volume_24h)),
            "source":           source,
            "score":            self._compute_score(float(change_24h), float(volume_24h), float(market_cap)),
        }

    def _passes_filters(self, coin: dict) -> bool:
        """Controlla se un coin passa tutti i filtri minimi."""
        # Escludi le major già nella watchlist statica
        static = {s.split("/")[0] for s in settings.SPOT_SYMBOLS + settings.FUTURES_SYMBOLS}
        if coin.get("symbol", "").upper() in static:
            return False

        # Escludi stablecoin
        stable = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FRAX", "LUSD"}
        if coin.get("symbol", "").upper() in stable:
            return False

        # Filtri quantitativi
        if coin.get("volume_24h_usd", 0) < self.min_volume_usd:
            return False
        if coin.get("price_change_24h", 0) < self.min_price_change_24h:
            return False

        return True

    def _estimate_surge(self, volume_24h: float) -> float:
        """
        Stima il volume surge.
        In assenza di dati storici, usa una euristica sul volume assoluto.
        """
        if volume_24h > 500_000_000:  return 5.0
        if volume_24h > 100_000_000:  return 3.5
        if volume_24h > 50_000_000:   return 2.5
        if volume_24h > 10_000_000:   return 1.8
        return 1.0

    def _compute_score(self, change_24h: float, volume_24h: float,
                       market_cap: Optional[float]) -> float:
        """Score composito 0-100 per ranking."""
        score = 0.0

        # Peso cambio prezzo (max 40 punti)
        score += min(40, max(0, change_24h) * 2)

        # Peso volume (max 40 punti)
        if volume_24h > 500_000_000:  score += 40
        elif volume_24h > 100_000_000: score += 30
        elif volume_24h > 50_000_000:  score += 20
        elif volume_24h > 10_000_000:  score += 10

        # Bonus small cap (più potenziale di crescita) (max 20 punti)
        if market_cap and 0 < market_cap < 500_000_000:
            score += 20
        elif market_cap and market_cap < 2_000_000_000:
            score += 10

        return round(score, 1)
