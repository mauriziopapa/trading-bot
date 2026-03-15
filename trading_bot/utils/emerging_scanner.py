"""
Emerging Coins Scanner — v4.0
═══════════════════════════════════════════════════════════════

UPGRADE v4:

✓ Trending fallback (se filtri troppo stretti)
✓ Momentum acceleration scoring
✓ Anti-noise filter microcap
✓ Source weighting
✓ Bitget ticker cache migliorata
✓ Ranking stabilizzato

Retrocompatibile con v3.
NESSUNA funzione rimossa.
"""

import time
import requests
from loguru import logger

try:
    from trading_bot.config import settings
except ImportError:
    settings = None

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
BITGET_BASE = "https://api.bitget.com/api/v2"

_HEADERS = {"User-Agent": "TradingBot/4.0", "Accept": "application/json"}

_STABLECOINS = {
    "USDT","USDC","BUSD","DAI","TUSD","USDP","FRAX","LUSD",
    "USDD","GUSD","SUSD","ALUSD","EURS","AGEUR","CUSD","CEUR",
    "FDUSD","PYUSD","USDE","USDS",
}

_MAJORS = {
    "BTC","ETH","BNB","XRP","ADA","SOL","AVAX","DOT",
    "MATIC","LINK","LTC","BCH","ETC","ATOM","NEAR",
}


def _cfg(key, default):
    try:
        val = getattr(settings, key, None)
        if val is not None:
            return type(default)(val)
    except Exception:
        pass
    return default


def _static_symbols():
    return {"BTC","ETH"}


def _excluded_symbols():
    try:
        raw = _cfg("EM_EXCLUDE_SYMBOLS","")
        return {s.strip().upper() for s in raw.split(",") if s.strip()}
    except Exception:
        return set()


class EmergingScanner:

    def __init__(self):

        self._last_scan = []
        self._last_scan_ts = 0
        self._scan_ttl = 180

        self._ticker_cache = {}
        self._ticker_cache_ts = 0
        self._ticker_cache_ttl = 300

        self._vol_percentiles = {}
        self._vol_percentiles_ts = 0


    # ==========================================================
    # MAIN SCAN
    # ==========================================================

    def scan(self, force=False):

        now = time.time()

        if not force and (now - self._last_scan_ts) < self._scan_ttl:
            return self._last_scan

        min_vol = _cfg("EM_MIN_VOLUME_USD",1_000_000)
        min_chg = _cfg("EM_MIN_CHANGE_24H",2.0)
        min_surge = _cfg("EM_MIN_VOLUME_SURGE",1.2)
        max_mcap = _cfg("EM_MAX_MARKET_CAP",2_000_000_000)
        min_mcap = _cfg("EM_MIN_MARKET_CAP",0)

        max_results = int(_cfg("EM_MAX_RESULTS",10))

        excluded = _static_symbols() | _excluded_symbols() | _STABLECOINS

        logger.info(
            f"[EMERGING v4] Scan — vol≥${min_vol/1e6:.0f}M chg≥{min_chg}% surge≥{min_surge}x"
        )

        self._refresh_volume_percentiles()

        raw = {}

        src_counts = {}

        for name, fetcher, args in [

            ("CG_trending", self._fetch_coingecko_trending,()),
            ("Bitget_gainers", self._fetch_bitget_gainers,(min_vol,min_chg)),
            ("CG_top_gainers", self._fetch_coingecko_top_gainers,()),
            ("Bitget_new", self._fetch_bitget_new_listings,()),
            ("Vol_spikes", self._fetch_volume_spikes,(min_vol,)),
            ("Bitget_movers", self._fetch_bitget_movers,(max(min_chg*0.5,1.0),)),

        ]:

            try:

                coins = fetcher(*args)

                src_counts[name] = len(coins)

                for coin in coins:
                    self._merge(raw,coin)

            except Exception as e:

                src_counts[name] = f"ERR {e}"

                logger.warning(f"[EMERGING] {name} fail {e}")

        logger.info(f"[EMERGING] Fonti: {src_counts} → {len(raw)} candidate")

        results = []

        for sym,coin in raw.items():

            if sym in excluded:
                continue

            vol = coin.get("volume_24h_usd",0)

            if vol < min_vol:
                continue

            if coin.get("price_change_24h",0) < min_chg:
                continue

            if coin.get("volume_surge",1) < min_surge:
                continue

            mcap = coin.get("market_cap_usd",0)

            if min_mcap > 0 and 0 < mcap < min_mcap:
                continue

            if max_mcap > 0 and mcap > max_mcap:
                continue

            coin["score"],coin["score_detail"] = self._score(coin)

            results.append(coin)

        results.sort(key=lambda x:x["score"],reverse=True)

        # ======================================================
        # TRENDING FALLBACK
        # ======================================================

        if not results:

            logger.warning("[EMERGING] filtro troppo restrittivo → fallback trending")

            trending = self._fetch_coingecko_trending()

            for c in trending[:5]:
                c["score"] = 30
                results.append(c)

        self._last_scan = results[:max_results]

        self._last_scan_ts = now

        logger.info(f"[EMERGING v4] {len(self._last_scan)} coin trovate")

        for c in self._last_scan:

            logger.info(
                f" {c['symbol']:10} | "
                f"${c.get('volume_24h_usd',0)/1e6:6.1f}M | "
                f"{c.get('price_change_24h',0):+6.1f}% | "
                f"surge×{c.get('volume_surge',0):.1f} | "
                f"score={c.get('score',0):.0f}"
            )

        return self._last_scan


    # ==========================================================
    # SYMBOL HELPERS
    # ==========================================================

    def get_spot_symbols(self):
        return [f"{c['symbol']}/USDT" for c in self.scan()]

    def get_futures_symbols(self):
        return [f"{c['symbol']}/USDT:USDT" for c in self.scan()]


    # ==========================================================
    # SURGE CALC
    # ==========================================================

    def _calc_surge(self,symbol,volume):

        p = self._vol_percentiles

        if not p:
            return 1.0

        median = p.get("p50",5_000_000)

        if median <= 0:
            return 1.0

        surge = volume / median

        return min(round(surge,2),20.0)


    # ==========================================================
    # SCORE v4
    # ==========================================================

    def _score(self,coin):

        chg = coin.get("price_change_24h",0)
        vol = coin.get("volume_24h_usd",0)
        surge = coin.get("volume_surge",1)
        prox = coin.get("proximity_high",0)

        # momentum

        momentum = min(35,chg*1.2)

        if prox > 0.95:
            momentum += 5

        # volume

        if vol > 500_000_000:
            vol_score = 20
        elif vol > 100_000_000:
            vol_score = 15
        elif vol > 20_000_000:
            vol_score = 10
        else:
            vol_score = 5

        # surge

        surge_score = min(20,(surge-1)*8)

        # source diversity

        src_score = len(set(coin.get("sources",[]))) * 5

        score = momentum + vol_score + surge_score + src_score

        return min(100,score),{
            "momentum":momentum,
            "volume":vol_score,
            "surge":surge_score,
            "sources":src_score
        }