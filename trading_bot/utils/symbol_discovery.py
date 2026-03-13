"""
Symbol Discovery — Auto-scopre tutte le coppie tradabili su Bitget
═══════════════════════════════════════════════════════════════
Quando SPOT_SYMBOLS=AUTO o FUTURES_SYMBOLS=AUTO:
  1. Carica TUTTI i mercati da Bitget
  2. Filtra per volume 24h > MIN_VOL (esclude dead coins)
  3. Esclude stablecoin, leveraged token, illiquide
  4. Aggiorna ogni ora automaticamente
  5. Ritorna la lista di simboli come se fossero in env

Risultato tipico: ~100-150 spot, ~80-120 futures.
"""

import time
import requests
from loguru import logger

# ── Esclusioni ────────────────────────────────────────────────────────────────
_STABLECOINS = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FRAX", "LUSD",
    "USDD", "GUSD", "SUSD", "FDUSD", "PYUSD", "USDE", "USDS", "EURS",
}
_EXCLUDED_SUFFIXES = {"3L", "3S", "5L", "5S", "2L", "2S", "UP", "DOWN", "BULL", "BEAR"}
_HEADERS = {"User-Agent": "TradingBot/4.0", "Accept": "application/json"}


class SymbolDiscovery:
    """
    Scopre automaticamente le coppie tradabili su Bitget.
    Cache 1 ora. Thread-safe (chiamato da main loop).
    """

    def __init__(self, min_volume_usd: float = 500_000):
        self.min_volume_usd = min_volume_usd
        self._spot_cache: list[str] = []
        self._futures_cache: list[str] = []
        self._spot_ts: float = 0
        self._futures_ts: float = 0
        self._ttl: float = 3600  # 1 ora

    def get_spot_symbols(self) -> list[str]:
        """Ritorna lista di simboli spot tipo ['BTC/USDT', 'ETH/USDT', ...]"""
        now = time.time()
        if self._spot_cache and (now - self._spot_ts) < self._ttl:
            return self._spot_cache

        try:
            symbols = self._discover_spot()
            if symbols:
                self._spot_cache = symbols
                self._spot_ts = now
                logger.info(f"[DISCOVERY] Spot: {len(symbols)} coppie attive (vol>=${self.min_volume_usd/1e6:.1f}M)")
            return self._spot_cache
        except Exception as e:
            logger.error(f"[DISCOVERY] Spot fallito: {e}")
            return self._spot_cache  # ritorna cache vecchia

    def get_futures_symbols(self) -> list[str]:
        """Ritorna lista di simboli futures tipo ['BTC/USDT:USDT', 'ETH/USDT:USDT', ...]"""
        now = time.time()
        if self._futures_cache and (now - self._futures_ts) < self._ttl:
            return self._futures_cache

        try:
            symbols = self._discover_futures()
            if symbols:
                self._futures_cache = symbols
                self._futures_ts = now
                logger.info(f"[DISCOVERY] Futures: {len(symbols)} coppie attive (vol>=${self.min_volume_usd/1e6:.1f}M)")
            return self._futures_cache
        except Exception as e:
            logger.error(f"[DISCOVERY] Futures fallito: {e}")
            return self._futures_cache

    def _discover_spot(self) -> list[str]:
        """Chiama Bitget API per ottenere tutti i ticker spot con volume."""
        r = requests.get(
            "https://api.bitget.com/api/v2/spot/market/tickers",
            headers=_HEADERS, timeout=15
        )
        if r.status_code != 200:
            logger.warning(f"[DISCOVERY] Spot API status {r.status_code}")
            return []

        tickers = r.json().get("data", [])
        symbols = []

        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue

            vol = float(t.get("usdtVol", 0) or 0)
            if vol < self.min_volume_usd:
                continue

            base = sym.replace("USDT", "").strip().upper()

            # Filtri esclusione
            if base in _STABLECOINS:
                continue
            if any(base.endswith(s) for s in _EXCLUDED_SUFFIXES):
                continue
            if len(base) < 2:
                continue

            symbols.append(f"{base}/USDT")

        # Ordina per volume (le più liquide prima)
        # Recupera volume per sorting
        vol_map = {}
        for t in tickers:
            sym = t.get("symbol", "")
            base = sym.replace("USDT", "").strip().upper()
            vol_map[f"{base}/USDT"] = float(t.get("usdtVol", 0) or 0)

        symbols.sort(key=lambda s: vol_map.get(s, 0), reverse=True)

        logger.info(
            f"[DISCOVERY] Spot top 10: "
            f"{', '.join(s.replace('/USDT','') for s in symbols[:10])}"
        )
        return symbols

    def _discover_futures(self) -> list[str]:
        """Chiama Bitget API per ottenere tutti i ticker futures USDT-M."""
        r = requests.get(
            "https://api.bitget.com/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
            headers=_HEADERS, timeout=15
        )
        if r.status_code != 200:
            logger.warning(f"[DISCOVERY] Futures API status {r.status_code}")
            return []

        tickers = r.json().get("data", [])
        symbols = []
        vol_map = {}

        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue

            vol = float(t.get("usdtVol", 0) or t.get("quoteVol", 0) or 0)
            if vol < self.min_volume_usd:
                continue

            base = sym.replace("USDT", "").strip().upper()

            if base in _STABLECOINS:
                continue
            if any(base.endswith(s) for s in _EXCLUDED_SUFFIXES):
                continue
            if len(base) < 2:
                continue

            ccxt_sym = f"{base}/USDT:USDT"
            symbols.append(ccxt_sym)
            vol_map[ccxt_sym] = vol

        symbols.sort(key=lambda s: vol_map.get(s, 0), reverse=True)

        logger.info(
            f"[DISCOVERY] Futures top 10: "
            f"{', '.join(s.split('/')[0] for s in symbols[:10])}"
        )
        return symbols

    def get_top_by_volume(self, market: str = "spot", n: int = 4) -> list[str]:
        """Ritorna le top N coppie per volume — usato per SCALPING_SYMBOLS."""
        syms = self.get_spot_symbols() if market == "spot" else self.get_futures_symbols()
        # Le prime N sono già ordinate per volume
        result = syms[:n]
        # Per scalping serve formato base: BTC/USDT (non BTC/USDT:USDT)
        if market == "futures":
            result = [s.split(":")[0] for s in result]
        return result


# ── Singleton ─────────────────────────────────────────────────────────────────
_discovery = None

def get_discovery() -> SymbolDiscovery:
    global _discovery
    if _discovery is None:
        _discovery = SymbolDiscovery(min_volume_usd=500_000)
    return _discovery
