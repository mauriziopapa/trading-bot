"""
Emerging Coins Scanner — v5.0
Adaptive Alpha Engine (production-ready)
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
    "User-Agent": "TradingBot/5.0",
    "Accept": "application/json"
}

_STABLECOINS = {
    "USDT","USDC","BUSD","DAI","TUSD","USDP","FRAX",
    "LUSD","USDD","GUSD","SUSD","FDUSD"
}

_MAJORS = {"BTC","ETH"}


# ==========================================
# CONFIG HELPERS
# ==========================================

def _cfg(key, default):
    try:
        val = getattr(settings, key, None)
        if val is not None:
            return type(default)(val)
    except Exception:
        pass
    return default


def _adaptive_params(regime=None):

    base = {
        "min_vol": _cfg("EM_MIN_VOLUME_USD", 500_000),
        "min_chg": _cfg("EM_MIN_CHANGE_24H", 1.5),
        "min_surge": _cfg("EM_MIN_VOLUME_SURGE", 1.03),
    }

    if regime == "AGGRO":
        base["min_chg"] *= 0.7
        base["min_surge"] *= 0.9

    elif regime == "DEFENSIVE":
        base["min_chg"] *= 1.3
        base["min_vol"] *= 1.5

    return base


# ==========================================
# MAIN CLASS
# ==========================================

class EmergingScanner:

    def __init__(self):

        self._bitget_markets = None
        self._bitget_markets_ts = 0

        self._last_scan = []
        self._last_scan_ts = 0
        self._scan_ttl = 180

        self._vol_percentiles = {}
        self._vol_percentiles_ts = 0


# ==========================================
# MARKETS
# ==========================================

    def _get_bitget_markets(self):

        now = time.time()

        if self._bitget_markets and (now - self._bitget_markets_ts) < 3600:
            return self._bitget_markets

        try:
            import ccxt

            exchange = ccxt.bitget()
            markets = exchange.load_markets()

            self._bitget_markets = markets
            self._bitget_markets_ts = now

            logger.info(f"[EMERGING] cached {len(markets)} bitget markets")

            return markets

        except Exception as e:
            logger.warning(f"[EMERGING] markets load error {e}")
            return {}


# ==========================================
# MAIN SCAN
# ==========================================

    def scan(self, force=False, regime=None):

        now = time.time()

        if not force and (now - self._last_scan_ts) < self._scan_ttl:
            return self._last_scan

        cfg = _adaptive_params(regime)

        logger.info(
            f"[EMERGING v5.0] regime={regime} | "
            f"vol≥${cfg['min_vol']/1e6:.2f}M "
            f"chg≥{cfg['min_chg']:.2f}% "
            f"surge≥{cfg['min_surge']:.2f}x"
        )

        self._refresh_volume_percentiles()

        raw = {}
        src_counts = {}

        sources = [
            ("CG_trending", self._fetch_coingecko_trending),
            ("CG_top_gainers", self._fetch_coingecko_top_gainers),
            ("Bitget_gainers", self._fetch_bitget_gainers),
        ]

        for name, fetcher in sources:
            try:
                coins = fetcher()
                src_counts[name] = len(coins)

                for coin in coins:
                    self._merge(raw, coin)

            except Exception as e:
                src_counts[name] = f"ERR {e}"

        logger.info(
            f"[EMERGING] Fonti: {src_counts} → {len(raw)} candidate"
        )

        results = []

        for sym, coin in raw.items():

            if sym in _STABLECOINS or sym in _MAJORS:
                continue

            vol = coin.get("volume_24h_usd", 0)
            chg = coin.get("price_change_24h", 0)
            surge = coin.get("volume_surge", 1)

            logger.debug(
                f"[EM_FILTER] {sym} | vol={vol:.0f} | chg={chg:.2f} | surge={surge:.2f}"
            )

            # ==========================================
            # SOFT FILTER (2/3 RULE)
            # ==========================================
            score_filter = 0

            if vol >= cfg["min_vol"]:
                score_filter += 1

            if abs(chg) >= cfg["min_chg"]:
                score_filter += 1

            if surge >= cfg["min_surge"]:
                score_filter += 1

            if score_filter < 2:
                continue

            coin["score"], coin["score_detail"] = self._score(coin)
            coin["confidence"] = min(1.0, coin["score"] / 40)

            results.append(coin)

        results.sort(key=lambda x: x.get("score", 0), reverse=True)

        results = self._filter_tradable(results)

        # ==========================================
        # FALLBACK
        # ==========================================
        if not results:
            logger.warning("[EMERGING] fallback activated")

            for sym, coin in raw.items():

                if sym in _STABLECOINS or sym in _MAJORS:
                    continue

                coin["score"], coin["score_detail"] = self._score(coin)
                coin["confidence"] = 0.3

                results.append(coin)

            results.sort(key=lambda x: x.get("score", 0), reverse=True)

        self._last_scan = results[:20]
        self._last_scan_ts = now

        logger.info(f"[EMERGING v5.0] {len(self._last_scan)} coin trovate")

        for c in self._last_scan:
            logger.info(
                f"{c['symbol']} | "
                f"${c.get('volume_24h_usd',0)/1e6:.1f}M | "
                f"{c.get('price_change_24h',0):+.1f}% | "
                f"surge×{c.get('volume_surge',0):.2f} | "
                f"score={c.get('score',0):.2f} | "
                f"conf={c.get('confidence',0):.2f}"
            )

        return self._last_scan


# ==========================================
# MARKET FILTER
# ==========================================

    def _filter_tradable(self, results):

        try:

            markets = self._get_bitget_markets()
            tradable = []

            for c in results:

                sym = c["symbol"].upper()
                pair = f"{sym}/USDT"

                if pair in markets:
                    tradable.append(c)
                    continue

                for m in markets.keys():
                    if m.split("/")[0] == sym:
                        tradable.append(c)
                        break

            return tradable

        except Exception as e:
            logger.warning(f"Market filter error: {e}")
            return results


# ==========================================
# VOLUME PERCENTILES
# ==========================================

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
                return

            data = r.json().get("data", [])

            vols = []

            for t in data:

                if not t.get("symbol","").endswith("USDT"):
                    continue

                vol = float(t.get("usdtVol",0) or 0)

                if vol > 0:
                    vols.append(vol)

            if len(vols) < 50:
                return

            vols.sort()
            n = len(vols)

            self._vol_percentiles = {
                "p50": vols[int(n*0.50)],
                "p75": vols[int(n*0.75)],
                "p90": vols[int(n*0.90)],
            }

            self._vol_percentiles_ts = now

        except Exception as e:
            logger.debug(f"[EMERGING] percentile refresh error {e}")


# ==========================================
# SURGE
# ==========================================

    def _calc_surge(self, symbol, volume_24h):

        p = self._vol_percentiles

        if not p:
            return 1.0

        median = p.get("p50", 1)

        if median <= 0:
            return 1.0

        surge = volume_24h / median

        return min(round(surge,2),20)


# ==========================================
# DATA SOURCES
# ==========================================

    def _fetch_coingecko_trending(self):

        try:

            r = requests.get(
                f"{COINGECKO_BASE}/search/trending",
                headers=_HEADERS,
                timeout=10
            )

            if r.status_code != 200:
                return []

            coins = r.json().get("coins", [])

            ids = [
                c["item"]["id"]
                for c in coins[:7]
                if "item" in c
            ]

            if not ids:
                return []

            return self._coingecko_markets_by_ids(ids,"cg_trending")

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
                    "sources":["bitget"]
                })

            out.sort(key=lambda x: x["price_change_24h"],reverse=True)

            return out[:20]

        except Exception:
            return []


# ==========================================
# HELPERS
# ==========================================

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

        sym = coin.get("symbol","").upper().replace("-","")
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

            e["volume_24h_usd"] = max(
                e.get("volume_24h_usd",0),
                coin.get("volume_24h_usd",0)
            )

            if abs(coin.get("price_change_24h",0)) > abs(e.get("price_change_24h",0)):
                e["price_change_24h"] = coin.get("price_change_24h")

            e["sources"] = list(
                set(e.get("sources",[]) + coin.get("sources",[]))
            )


# ==========================================
# SCORE
# ==========================================

    def _score(self, coin):

        chg = abs(coin.get("price_change_24h", 0))
        vol = coin.get("volume_24h_usd", 0)
        surge = coin.get("volume_surge", 1)

        mom = min(30, chg * 1.5)
        vol_pt = min(25, vol / 2_000_000)
        sur_pt = min(25, surge * 5)
        src_pt = len(coin.get("sources", [])) * 5

        score = mom + vol_pt + sur_pt + src_pt

        return score, {
            "momentum": mom,
            "volume": vol_pt,
            "surge": sur_pt,
            "sources": src_pt
        }