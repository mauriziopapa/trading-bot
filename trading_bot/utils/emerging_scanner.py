"""
Emerging Scanner v6.0 — SNIPER MODE
Futures-first | Anti-fake | Momentum early detection
"""

import time
import requests
from loguru import logger

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
BITGET_BASE = "https://api.bitget.com/api/v2"

_HEADERS = {
    "User-Agent": "TradingBot/6.0",
    "Accept": "application/json"
}

_STABLES = {"USDT","USDC","BUSD","DAI"}
_BLACKLIST = {"BTC","ETH"}


class EmergingScanner:

    def __init__(self):

        self._last_scan = []
        self._last_ts = 0
        self._ttl = 120


# ==========================================================
# MAIN
# ==========================================================

    def scan(self, force=False, regime=None):

        now = time.time()

        if not force and (now - self._last_ts) < self._ttl:
            return self._last_scan

        logger.info("[SNIPER SCAN] start")

        raw = []

        raw += self._bitget_gainers()
        raw += self._coingecko_gainers()

        merged = self._merge(raw)

        # 🔥 CORE FILTER
        results = []

        for c in merged:

            sym = c["symbol"]

            if sym in _STABLES or sym in _BLACKLIST:
                continue

            vol = c["volume"]
            chg = c["change"]
            price = c["price"]

            # 🔥 HARD FILTERS (profit-oriented)
            if vol < 2_000_000:
                continue

            if abs(chg) < 2:
                continue

            # 🔥 avoid late pump
            if abs(chg) > 25:
                continue

            # 🔥 avoid micro price junk
            if price < 0.0005:
                continue

            score = self._score(c)

            c["score"] = score
            results.append(c)

        results.sort(key=lambda x: x["score"], reverse=True)

        # 🔥 TOP QUALITY ONLY
        self._last_scan = results[:10]
        self._last_ts = now

        logger.info(f"[SNIPER] selected {len(self._last_scan)} coins")

        for c in self._last_scan:
            logger.info(f"{c['symbol']} | {c['change']:.1f}% | vol={c['volume']/1e6:.1f}M | score={c['score']:.1f}")

        return self._last_scan


# ==========================================================
# SOURCES
# ==========================================================

    def _bitget_gainers(self):

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

                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue

                vol = float(t.get("usdtVol", 0) or 0)
                chg = float(t.get("changeUtc24h", 0) or 0) * 100
                price = float(t.get("last", 0) or 0)

                out.append({
                    "symbol": sym.replace("USDT",""),
                    "volume": vol,
                    "change": chg,
                    "price": price,
                    "source": "bitget"
                })

            return sorted(out, key=lambda x: x["change"], reverse=True)[:30]

        except Exception as e:
            logger.error(f"[SNIPER] bitget error {e}")
            return []


    def _coingecko_gainers(self):

        try:

            r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency":"usd",
                    "order":"volume_desc",
                    "per_page":100,
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

            out = []

            for c in coins:

                out.append({
                    "symbol": c.get("symbol","").upper(),
                    "volume": float(c.get("total_volume") or 0),
                    "change": float(c.get("price_change_percentage_24h") or 0),
                    "price": float(c.get("current_price") or 0),
                    "source": "cg"
                })

            return sorted(out, key=lambda x: x["volume"], reverse=True)[:50]

        except Exception as e:
            logger.error(f"[SNIPER] cg error {e}")
            return []


# ==========================================================
# MERGE
# ==========================================================

    def _merge(self, raw):

        merged = {}

        for c in raw:

            sym = c["symbol"]

            if sym not in merged:
                merged[sym] = c
            else:
                e = merged[sym]
                e["volume"] = max(e["volume"], c["volume"])
                e["change"] = c["change"] if abs(c["change"]) > abs(e["change"]) else e["change"]

        return list(merged.values())


# ==========================================================
# SCORE
# ==========================================================

    def _score(self, c):

        vol = c["volume"]
        chg = abs(c["change"])

        score = 0

        # volume weight
        score += min(30, vol / 1_000_000)

        # momentum
        score += chg * 2

        # sweet spot (5%–15%)
        if 5 < chg < 15:
            score += 20

        # anti overextended
        if chg > 20:
            score -= 15

        return score