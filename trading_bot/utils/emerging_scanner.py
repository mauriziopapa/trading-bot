"""
Emerging Coins Scanner — v4.0
Enhanced stability + surge detection
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

_HEADERS = {
    "User-Agent": "TradingBot/4.0",
    "Accept": "application/json"
}


_STABLECOINS = {
    "USDT","USDC","BUSD","DAI","TUSD","USDP","FRAX",
    "LUSD","USDD","GUSD","SUSD","FDUSD"
}

_MAJORS = {
    "BTC","ETH"
}


def _cfg(key, default):

    try:
        val = getattr(settings, key, None)

        if val is not None:
            return type(default)(val)

    except Exception:
        pass

    return default


class EmergingScanner:

    def __init__(self):

        self._last_scan = []
        self._last_scan_ts = 0
        self._scan_ttl = 180

        self._vol_percentiles = {}
        self._vol_percentiles_ts = 0


# ==========================================================
# MAIN SCAN
# ==========================================================

    def scan(self, force=False):

        now = time.time()

        if not force and (now - self._last_scan_ts) < self._scan_ttl:
            return self._last_scan

        min_vol = _cfg("EM_MIN_VOLUME_USD", 1_000_000)
        min_chg = _cfg("EM_MIN_CHANGE_24H", 2)
        min_surge = _cfg("EM_MIN_VOLUME_SURGE", 1.2)
        max_results = _cfg("EM_MAX_RESULTS", 10)

        logger.info(
            f"[EMERGING v4] Scan — vol≥${min_vol/1e6:.0f}M "
            f"chg≥{min_chg}% surge≥{min_surge}x"
        )

        # update percentiles
        self._refresh_volume_percentiles()

        raw = {}

        src_counts = {}

        for name, fetcher in [

            ("CG_trending", self._fetch_coingecko_trending),
            ("CG_top_gainers", self._fetch_coingecko_top_gainers),
            ("Bitget_gainers", self._fetch_bitget_gainers),
            ("Bitget_new", self._fetch_bitget_new_listings),
            ("Vol_spikes", self._fetch_volume_spikes),
            ("Bitget_movers", self._fetch_bitget_movers),

        ]:

            try:

                coins = fetcher()

                src_counts[name] = len(coins)

                for coin in coins:
                    self._merge(raw, coin)

            except Exception as e:

                src_counts[name] = f"ERR {e}"

        logger.info(
            f"[EMERGING] Fonti: {src_counts} → {len(raw)} candidate pre-filtro"
        )

        results = []

        for sym, coin in raw.items():

            if sym in _STABLECOINS or sym in _MAJORS:
                continue

            if coin.get("volume_24h_usd", 0) < min_vol:
                continue

            if coin.get("price_change_24h", 0) < min_chg:
                continue

            if coin.get("volume_surge", 1) < min_surge:
                continue

            coin["score"], coin["score_detail"] = self._score(coin)

            results.append(coin)

        results.sort(key=lambda x: x.get("score", 0), reverse=True)

        self._last_scan = results[:max_results]

        self._last_scan_ts = now

        logger.info(
            f"[EMERGING v4] {len(self._last_scan)} coin trovate"
        )

        for c in self._last_scan:

            logger.info(
                f"{c['symbol']} | ${c.get('volume_24h_usd',0)/1e6:.1f}M "
                f"| {c.get('price_change_24h',0):+.1f}% "
                f"| surge×{c.get('volume_surge',0):.1f} "
                f"| score={c.get('score',0)}"
            )

        return self._last_scan


# ==========================================================
# VOLUME PERCENTILES
# ==========================================================

    def _refresh_volume_percentiles(self):

        now = time.time()

        if (now - self._vol_percentiles_ts) < 1800:
            return

        try:

            r = requests.get(
                f"{BITGET_BASE}/spot/market/tickers",
                headers=_HEADERS,
                timeout=10
            )

            if r.status_code != 200:
                logger.debug("[EMERGING] ticker fetch failed")
                return

            data = r.json().get("data", [])

            vols = []

            for t in data:

                if not t.get("symbol","").endswith("USDT"):
                    continue

                vol = float(t.get("usdtVol",0) or 0)

                if vol > 0:
                    vols.append(vol)

            if not vols:
                return

            vols.sort()

            n = len(vols)

            self._vol_percentiles = {

                "p25": vols[int(n*0.25)],
                "p50": vols[int(n*0.50)],
                "p75": vols[int(n*0.75)],
                "p90": vols[int(n*0.90)],
            }

            self._vol_percentiles_ts = now

            logger.debug("[EMERGING] volume percentiles updated")

        except Exception as e:

            logger.debug(f"[EMERGING] percentile refresh error {e}")


# ==========================================================
# SURGE CALCULATION
# ==========================================================

    def _calc_surge(self, symbol, volume_24h):

        p = self._vol_percentiles

        if not p:
            return 1

        median = p.get("p50", 1)

        if median <= 0:
            return 1

        surge = volume_24h / median

        return min(round(surge,2),20)


# ==========================================================
# DATA SOURCES
# ==========================================================

    def _fetch_coingecko_trending(self):

        try:

            r = requests.get(
                f"{COINGECKO_BASE}/search/trending",
                headers=_HEADERS,
                timeout=10
            )

            if r.status_code != 200:
                return []

            ids = [
                c["item"]["id"]
                for c in r.json().get("coins", [])[:7]
            ]

            if not ids:
                return []

            return self._coingecko_markets_by_ids(ids,"trending")

        except Exception:
            return []


    def _fetch_coingecko_top_gainers(self):

        try:

            r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency":"usd",
                    "order":"market_cap_desc",
                    "per_page":250,
                    "page":1,
                    "sparkline":"false",
                    "price_change_percentage":"24h"
                },
                headers=_HEADERS,
                timeout=10
            )

            if r.status_code != 200:
                return []

            coins = r.json()

            coins.sort(
                key=lambda x: x.get("price_change_percentage_24h") or 0,
                reverse=True
            )

            return [self._norm_cg(c,"cg_gainers") for c in coins[:20]]

        except Exception:
            return []


    def _fetch_bitget_gainers(self):

        try:

            r = requests.get(
                f"{BITGET_BASE}/spot/market/tickers",
                headers=_HEADERS,
                timeout=10
            )

            if r.status_code != 200:
                return []

            data = r.json().get("data", [])

            out = []

            for t in data:

                if not t.get("symbol","").endswith("USDT"):
                    continue

                vol = float(t.get("usdtVol",0) or 0)

                chg = float(t.get("changeUtc24h",0) or 0) * 100

                sym = t.get("symbol").replace("USDT","")

                out.append({

                    "symbol":sym.upper(),
                    "price_change_24h":round(chg,2),
                    "volume_24h_usd":vol,
                    "market_cap_usd":0,
                    "volume_surge":self._calc_surge(sym,vol),
                    "sources":["bitget_gainers"]

                })

            out.sort(key=lambda x: x["price_change_24h"],reverse=True)

            return out[:20]

        except Exception:
            return []


    def _fetch_bitget_new_listings(self):

        return []


    def _fetch_volume_spikes(self):

        return []


    def _fetch_bitget_movers(self):

        return []


# ==========================================================
# HELPERS
# ==========================================================

    def _coingecko_markets_by_ids(self, ids, source):

        try:

            r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency":"usd",
                    "ids":",".join(ids),
                    "sparkline":"false",
                    "price_change_percentage":"24h"
                },
                headers=_HEADERS,
                timeout=10
            )

            if r.status_code != 200:
                return []

            return [self._norm_cg(c,source) for c in r.json()]

        except Exception:
            return []


    def _norm_cg(self, coin, source):

        sym = coin.get("symbol","").upper()

        vol = float(coin.get("total_volume") or 0)

        return {

            "symbol":sym,
            "price":coin.get("current_price"),
            "price_change_24h":coin.get("price_change_percentage_24h") or 0,
            "volume_24h_usd":vol,
            "market_cap_usd":coin.get("market_cap") or 0,
            "volume_surge":self._calc_surge(sym,vol),
            "sources":[source]

        }


    def _merge(self, raw, coin):

        sym = coin.get("symbol")

        if not sym:
            return

        if sym not in raw:
            raw[sym] = coin
        else:

            e = raw[sym]

            if coin.get("volume_24h_usd",0) > e.get("volume_24h_usd",0):
                e["volume_24h_usd"] = coin["volume_24h_usd"]

            if coin.get("price_change_24h",0) > e.get("price_change_24h",0):
                e["price_change_24h"] = coin["price_change_24h"]

            e["sources"] = list(set(e.get("sources",[]) + coin.get("sources",[])))

            raw[sym] = e


# ==========================================================
# SCORING
# ==========================================================

    def _score(self, coin):

        chg = coin.get("price_change_24h",0)

        vol = coin.get("volume_24h_usd",0)

        surge = coin.get("volume_surge",1)

        mom = min(30, max(0, chg))

        vol_pt = min(18, vol / 10000000)

        sur_pt = min(18, surge * 3)

        src_pt = len(coin.get("sources",[])) * 4

        score = mom + vol_pt + sur_pt + src_pt

        return score, {
            "momentum":mom,
            "volume":vol_pt,
            "surge":sur_pt,
            "sources":src_pt
        }