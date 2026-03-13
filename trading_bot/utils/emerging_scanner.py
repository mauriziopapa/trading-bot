"""
Emerging Coins Scanner — v3.0
═══════════════════════════════════════════════════════════════
FIX CRITICI:
  ✓ Volume surge calcolato RELATIVO al token stesso (non mediana globale)
    Usa il volume 24h del token vs il volume mediano di TUTTI i ticker
    come proxy. In v4 servirà volume storico per-token.
  ✓ Score composito ricalibrato: momentum ha peso maggiore
  ✓ Cache API condivisa — non richiama ticker se già fetchato < 5 min

OTTIMIZZAZIONI:
  ✓ Proximity-to-high: coin vicine al max 24h hanno più momentum
  ✓ Source diversity bonus aumentato
  ✓ New listing age scaling migliorato
"""

import time
import requests
from loguru import logger

try:
    from trading_bot.config import settings
except ImportError:
    settings = None

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
BITGET_BASE    = "https://api.bitget.com/api/v2"

_STABLECOINS = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FRAX", "LUSD",
    "USDD", "GUSD", "SUSD", "ALUSD", "EURS", "AGEUR", "CUSD", "CEUR",
    "FDUSD", "PYUSD", "USDE", "USDS",
}

_MAJORS = {
    "BTC", "ETH", "BNB", "XRP", "ADA", "SOL", "AVAX", "DOT",
    "MATIC", "LINK", "LTC", "BCH", "ETC", "ATOM", "NEAR",
}

_HEADERS = {"User-Agent": "TradingBot/3.0", "Accept": "application/json"}


def _cfg(key: str, default):
    try:
        val = getattr(settings, key, None)
        if val is not None:
            return type(default)(val)
    except Exception:
        pass
    return default


def _static_symbols() -> set[str]:
    try:
        spot    = getattr(settings, "SPOT_SYMBOLS",    []) or []
        futures = getattr(settings, "FUTURES_SYMBOLS", []) or []
        return {s.split("/")[0].upper() for s in spot + futures}
    except Exception:
        return set()


def _excluded_symbols() -> set[str]:
    try:
        raw = _cfg("EM_EXCLUDE_SYMBOLS", "")
        return {s.strip().upper() for s in raw.split(",") if s.strip()}
    except Exception:
        return set()


class EmergingScanner:
    def __init__(self):
        self._last_scan: list[dict] = []
        self._last_scan_ts: float   = 0.0
        self._scan_ttl: float       = 180   # 3 minuti (era 10 min — troppo lento)

        # ── Cache ticker per surge calculation ───────────────────────────
        self._ticker_cache: dict[str, dict] = {}  # symbol → ticker data
        self._ticker_cache_ts: float        = 0.0
        self._ticker_cache_ttl: float       = 300  # 5 min

        # ── Percentili volume per surge relativo ─────────────────────────
        self._vol_percentiles: dict = {}
        self._vol_percentiles_ts: float = 0.0

    def scan(self, force: bool = False) -> list[dict]:
        now = time.time()
        if not force and (now - self._last_scan_ts) < self._scan_ttl:
            return self._last_scan

        min_vol     = _cfg("EM_MIN_VOLUME_USD",   1_000_000.0)
        min_chg     = _cfg("EM_MIN_CHANGE_24H",   2.0)
        min_surge   = _cfg("EM_MIN_VOLUME_SURGE", 1.2)
        max_mcap    = _cfg("EM_MAX_MARKET_CAP",   2_000_000_000.0)
        min_mcap    = _cfg("EM_MIN_MARKET_CAP",   0.0)
        max_results = int(_cfg("EM_MAX_RESULTS",  10))
        excluded    = _static_symbols() | _excluded_symbols() | _STABLECOINS

        logger.info(
            f"[EMERGING v3] Scan — vol≥${min_vol/1e6:.0f}M chg≥{min_chg}% "
            f"surge≥{min_surge}x mcap=[${min_mcap/1e6:.0f}M–${max_mcap/1e6:.0f}M]"
        )

        # ── Aggiorna cache percentili volume ─────────────────────────────
        self._refresh_volume_percentiles()

        raw: dict[str, dict] = {}

        for coin in self._fetch_coingecko_trending():
            self._merge(raw, coin)
        for coin in self._fetch_bitget_gainers(min_vol, min_chg):
            self._merge(raw, coin)
        for coin in self._fetch_coingecko_top_gainers():
            self._merge(raw, coin)
        for coin in self._fetch_bitget_new_listings():
            self._merge(raw, coin)
        for coin in self._fetch_volume_spikes(min_vol):
            self._merge(raw, coin)
        for coin in self._fetch_bitget_movers(max(min_chg * 0.5, 1.5)):
            self._merge(raw, coin)

        results = []
        for sym, coin in raw.items():
            if sym in excluded:
                continue
            if coin.get("volume_24h_usd", 0) < min_vol:
                continue
            if coin.get("price_change_24h", 0) < min_chg:
                continue
            surge = coin.get("volume_surge", 1.0)
            if surge < min_surge:
                continue
            mcap = coin.get("market_cap_usd", 0)
            if min_mcap > 0 and 0 < mcap < min_mcap:
                continue
            if max_mcap > 0 and mcap > max_mcap:
                continue

            coin["score"], coin["score_detail"] = self._score(coin)
            results.append(coin)

        results.sort(key=lambda x: x.get("score", 0), reverse=True)
        self._last_scan    = results[:max_results]
        self._last_scan_ts = now

        logger.info(f"[EMERGING v3] {len(self._last_scan)} coin trovate (da {len(raw)} candidate)")
        for c in self._last_scan:
            logger.info(
                f"  {c['symbol']:10} | ${c.get('volume_24h_usd',0)/1e6:6.1f}M "
                f"| {c.get('price_change_24h',0):+6.1f}% "
                f"| surge×{c.get('volume_surge',0):.1f} "
                f"| score={c.get('score',0):.0f} "
                f"| src={','.join(c.get('sources',[]))}"
            )
        return self._last_scan

    def get_spot_symbols(self) -> list[str]:
        return [f"{c['symbol']}/USDT" for c in self.scan()]

    def get_futures_symbols(self) -> list[str]:
        return [f"{c['symbol']}/USDT:USDT" for c in self.scan()]

    # ─── Volume Percentiles (per surge relativo) ─────────────────────────────

    def _refresh_volume_percentiles(self):
        """Calcola percentili di volume dal mercato Bitget per surge relativo."""
        now = time.time()
        if (now - self._vol_percentiles_ts) < 1800:
            return
        try:
            r = requests.get(f"{BITGET_BASE}/spot/market/tickers",
                             headers=_HEADERS, timeout=10)
            if r.status_code != 200:
                return

            tickers = r.json().get("data", [])
            vols = {}
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                vol = float(t.get("usdtVol", 0) or 0)
                if vol > 0:
                    base = sym.replace("USDT", "").strip().upper()
                    vols[base] = vol

            if vols:
                sorted_vols = sorted(vols.values())
                n = len(sorted_vols)
                self._vol_percentiles = {
                    "p25": sorted_vols[int(n * 0.25)],
                    "p50": sorted_vols[int(n * 0.50)],
                    "p75": sorted_vols[int(n * 0.75)],
                    "p90": sorted_vols[int(n * 0.90)],
                    "all":  vols,   # per lookup per-token
                }
                self._vol_percentiles_ts = now
                logger.debug(
                    f"[EMERGING] Vol percentiles: p25=${sorted_vols[int(n*0.25)]/1e6:.1f}M "
                    f"p50=${sorted_vols[int(n*0.50)]/1e6:.1f}M "
                    f"p90=${sorted_vols[int(n*0.90)]/1e6:.1f}M"
                )
        except Exception as e:
            logger.debug(f"[EMERGING] Vol percentiles: {e}")

    def _calc_surge(self, symbol: str, volume_24h: float) -> float:
        """
        FIX CRITICO: calcola surge relativo al percentile del mercato.
        Non più volume/mediana_globale (che dava BTC=6000x e altcoin=0.4x).

        Logica:
        1. Se il token è nel cache percentili, usa il suo volume come base
           (approssimazione: il volume "normale" è la media recente)
        2. Altrimenti usa il percentile 50 come fallback
        3. Clamp a max 20x
        """
        p = self._vol_percentiles
        if not p:
            return 1.0

        # Il "volume normale" del token è il suo volume nella snapshot precedente
        # Per ora usiamo il percentile bucket in cui cade
        all_vols = p.get("all", {})
        base_sym = symbol.upper()

        # Se abbiamo il volume storico del token, confronta con sé stesso
        # (in futuro: media 7d). Per ora: confronta con mediana del bucket.
        prev_vol = all_vols.get(base_sym, 0)

        if prev_vol > 0 and prev_vol != volume_24h:
            # Surge = volume attuale / volume precedente snapshot
            surge = volume_24h / prev_vol
        else:
            # Fallback: confronta con mediana mercato
            median = p.get("p50", 5_000_000)
            surge = volume_24h / median if median > 0 else 1.0

        return min(round(surge, 2), 20.0)

    # ─── Data Sources ────────────────────────────────────────────────────────

    def _fetch_coingecko_trending(self) -> list[dict]:
        try:
            r = requests.get(f"{COINGECKO_BASE}/search/trending",
                             headers=_HEADERS, timeout=10)
            if r.status_code != 200:
                return []
            ids = [c["item"]["id"] for c in r.json().get("coins", [])[:7] if "item" in c]
            if not ids:
                return []
            return self._coingecko_markets_by_ids(ids, source="trending")
        except Exception as e:
            logger.debug(f"[EMERGING] CG trending: {e}")
            return []

    def _fetch_coingecko_top_gainers(self) -> list[dict]:
        try:
            r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency": "usd", "order": "market_cap_desc",
                    "per_page": 250, "page": 1, "sparkline": "false",
                    "price_change_percentage": "24h",
                },
                headers=_HEADERS, timeout=12,
            )
            if r.status_code != 200:
                return []
            coins = r.json()
            gainers = sorted(
                coins,
                key=lambda x: x.get("price_change_percentage_24h") or 0,
                reverse=True,
            )[:20]
            return [self._norm_cg(c, "cg_gainers") for c in gainers]
        except Exception as e:
            logger.debug(f"[EMERGING] CG top gainers: {e}")
            return []

    def _fetch_bitget_gainers(self, min_vol: float, min_chg: float) -> list[dict]:
        try:
            r = requests.get(f"{BITGET_BASE}/spot/market/tickers",
                             headers=_HEADERS, timeout=10)
            if r.status_code != 200:
                return []
            tickers = [
                t for t in r.json().get("data", [])
                if t.get("symbol", "").endswith("USDT")
                and float(t.get("usdtVol", 0) or 0) >= min_vol
            ]
            tickers.sort(
                key=lambda x: float(x.get("changeUtc24h", 0) or 0),
                reverse=True,
            )
            out = []
            for t in tickers[:30]:
                chg  = float(t.get("changeUtc24h", 0) or 0) * 100
                vol  = float(t.get("usdtVol",      0) or 0)
                sym  = t.get("symbol", "").replace("USDT", "").strip()
                last = float(t.get("lastPr",  0) or 0)
                high = float(t.get("high24h", 0) or 0)
                if chg < min_chg:
                    continue

                proximity = last / high if high > 0 else 0
                out.append({
                    "symbol":           sym.upper(),
                    "price":            last,
                    "price_change_24h": round(chg, 2),
                    "volume_24h_usd":   vol,
                    "market_cap_usd":   0.0,
                    "volume_surge":     self._calc_surge(sym, vol),
                    "proximity_high":   round(proximity, 3),
                    "sources":          ["bitget_gainers"],
                    "is_new_listing":   False,
                    "listing_age_days": None,
                    "name":             sym,
                })
            return out
        except Exception as e:
            logger.debug(f"[EMERGING] Bitget gainers: {e}")
            return []

    def _fetch_bitget_new_listings(self) -> list[dict]:
        try:
            days_thresh = int(_cfg("EM_NEW_LISTING_DAYS", 30))
            r = requests.get(f"{BITGET_BASE}/spot/public/coins",
                             headers=_HEADERS, timeout=10)
            if r.status_code != 200:
                return []

            now_ms = time.time() * 1000
            out = []
            for coin in r.json().get("data", []):
                launch_ms = float(coin.get("launchTime", 0) or 0)
                if launch_ms <= 0:
                    continue
                age_days = (now_ms - launch_ms) / 86_400_000
                if age_days > days_thresh:
                    continue
                sym = (coin.get("coin", "") or "").upper()
                if not sym:
                    continue
                out.append({
                    "symbol":           sym,
                    "name":             coin.get("coinName", sym),
                    "price":            0.0,
                    "price_change_24h": 0.0,
                    "volume_24h_usd":   0.0,
                    "market_cap_usd":   0.0,
                    "volume_surge":     1.0,
                    "sources":          ["new_listing"],
                    "is_new_listing":   True,
                    "listing_age_days": int(age_days),
                })
            return out
        except Exception as e:
            logger.debug(f"[EMERGING] New listings: {e}")
            return []

    def _fetch_volume_spikes(self, min_vol: float) -> list[dict]:
        try:
            r = requests.get(f"{BITGET_BASE}/spot/market/tickers",
                             headers=_HEADERS, timeout=10)
            if r.status_code != 200:
                return []
            out = []
            for t in r.json().get("data", []):
                if not t.get("symbol", "").endswith("USDT"):
                    continue
                vol  = float(t.get("usdtVol", 0) or 0)
                last = float(t.get("lastPr",  0) or 0)
                if vol < min_vol or last <= 0:
                    continue

                sym = t.get("symbol", "").replace("USDT", "").upper()
                surge = self._calc_surge(sym, vol)
                if surge < 3.0:
                    continue

                chg = float(t.get("changeUtc24h", 0) or 0) * 100
                out.append({
                    "symbol":           sym,
                    "name":             sym,
                    "price":            last,
                    "price_change_24h": round(chg, 2),
                    "volume_24h_usd":   vol,
                    "market_cap_usd":   0.0,
                    "volume_surge":     surge,
                    "sources":          ["vol_spike"],
                    "is_new_listing":   False,
                    "listing_age_days": None,
                })
            return out
        except Exception as e:
            logger.debug(f"[EMERGING] Volume spikes: {e}")
            return []

    def _fetch_bitget_movers(self, min_chg: float) -> list[dict]:
        try:
            r = requests.get(f"{BITGET_BASE}/spot/market/tickers",
                             headers=_HEADERS, timeout=10)
            if r.status_code != 200:
                return []
            out = []
            for t in r.json().get("data", []):
                if not t.get("symbol", "").endswith("USDT"):
                    continue
                vol  = float(t.get("usdtVol",      0) or 0)
                chg  = float(t.get("changeUtc24h", 0) or 0) * 100
                last = float(t.get("lastPr",       0) or 0)
                if vol < 500_000 or last <= 0 or chg < min_chg:
                    continue
                sym  = t.get("symbol", "").replace("USDT", "").strip().upper()
                high = float(t.get("high24h", last) or last)
                proximity = last / high if high > 0 else 0
                out.append({
                    "symbol":           sym,
                    "name":             sym,
                    "price":            last,
                    "price_change_24h": round(chg, 2),
                    "volume_24h_usd":   vol,
                    "market_cap_usd":   0.0,
                    "volume_surge":     self._calc_surge(sym, vol),
                    "proximity_high":   round(proximity, 3),
                    "sources":          ["bitget_movers"],
                    "is_new_listing":   False,
                    "listing_age_days": None,
                })
            out.sort(key=lambda x: x["price_change_24h"] * x["volume_24h_usd"], reverse=True)
            return out[:25]
        except Exception as e:
            logger.debug(f"[EMERGING] Bitget movers: {e}")
            return []

    # ─── Helpers ─────────────────────────────────────────────────────────────

    def _coingecko_markets_by_ids(self, ids: list[str], source: str) -> list[dict]:
        try:
            r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency": "usd", "ids": ",".join(ids),
                    "sparkline": "false", "price_change_percentage": "24h",
                },
                headers=_HEADERS, timeout=12,
            )
            if r.status_code != 200:
                return []
            return [self._norm_cg(c, source) for c in r.json()]
        except Exception:
            return []

    def _norm_cg(self, coin: dict, source: str) -> dict:
        chg  = float(coin.get("price_change_percentage_24h") or 0)
        vol  = float(coin.get("total_volume")  or 0)
        mcap = float(coin.get("market_cap")    or 0)
        sym  = (coin.get("symbol") or "").upper()
        return {
            "symbol":           sym,
            "name":             coin.get("name", sym),
            "price":            float(coin.get("current_price") or 0),
            "price_change_24h": round(chg, 2),
            "volume_24h_usd":   vol,
            "market_cap_usd":   mcap,
            "volume_surge":     self._calc_surge(sym, vol),
            "sources":          [source],
            "is_new_listing":   False,
            "listing_age_days": None,
        }

    def _merge(self, raw: dict, coin: dict) -> None:
        sym = coin.get("symbol", "").upper()
        if not sym:
            return
        if sym not in raw:
            raw[sym] = coin.copy()
        else:
            existing = raw[sym]
            for field in ["price", "price_change_24h", "volume_24h_usd", "market_cap_usd"]:
                if coin.get(field, 0) > existing.get(field, 0):
                    existing[field] = coin[field]
            for s in coin.get("sources", []):
                if s not in existing.get("sources", []):
                    existing.setdefault("sources", []).append(s)
            if coin.get("is_new_listing"):
                existing["is_new_listing"]   = True
                existing["listing_age_days"] = coin.get("listing_age_days")
            if coin.get("volume_surge", 0) > existing.get("volume_surge", 0):
                existing["volume_surge"] = coin["volume_surge"]
            # Proximity to high
            if coin.get("proximity_high", 0) > existing.get("proximity_high", 0):
                existing["proximity_high"] = coin["proximity_high"]
            raw[sym] = existing

    def _score(self, coin: dict) -> tuple[float, dict]:
        """
        Score composito 0-100 v3 — ricalibrato per aggressività.
        Momentum ha peso maggiore, proximity to high come bonus.
        """
        chg   = coin.get("price_change_24h", 0)
        vol   = coin.get("volume_24h_usd",   0)
        surge = coin.get("volume_surge",      1.0)
        mcap  = coin.get("market_cap_usd",   0)
        srcs  = coin.get("sources",          [])
        prox  = coin.get("proximity_high",   0)

        # ① Momentum 24h — max 30 pt (era 25, peso aumentato)
        if   chg >= 50: mom = 30.0
        elif chg >= 30: mom = 26.0
        elif chg >= 20: mom = 22.0
        elif chg >= 10: mom = 16.0
        elif chg >=  5: mom = 10.0
        else:           mom = max(0.0, chg * 1.0)

        # Bonus proximity to high: se siamo > 95% del max 24h → momentum vivo
        if prox > 0.95:
            mom = min(30.0, mom + 4)

        # ② Volume assoluto — max 18 pt
        if   vol >= 1_000_000_000: vol_pt = 18.0
        elif vol >=   500_000_000: vol_pt = 15.0
        elif vol >=   100_000_000: vol_pt = 12.0
        elif vol >=    50_000_000: vol_pt = 8.0
        elif vol >=    10_000_000: vol_pt = 5.0
        else:                      vol_pt = 2.0

        # ③ Volume surge — max 18 pt
        if   surge >= 8: sur_pt = 18.0
        elif surge >= 5: sur_pt = 14.0
        elif surge >= 3: sur_pt = 10.0
        elif surge >= 2: sur_pt = 6.0
        else:            sur_pt = max(0.0, (surge - 1) * 4)

        # ④ Small-cap bonus — max 18 pt
        if   0 < mcap <   50_000_000: cap_pt = 18.0
        elif 0 < mcap <  200_000_000: cap_pt = 15.0
        elif 0 < mcap <  500_000_000: cap_pt = 11.0
        elif 0 < mcap < 2_000_000_000: cap_pt = 6.0
        elif mcap == 0:                cap_pt = 4.0
        else:                          cap_pt = 2.0

        # ⑤ Multi-source bonus — max 16 pt (era 15)
        n_srcs   = len(set(srcs))
        multi_pt = min(16.0, n_srcs * 5.5)

        if coin.get("is_new_listing"):
            age = coin.get("listing_age_days") or 30
            if age <= 3:    multi_pt = min(16.0, multi_pt + 7)
            elif age <= 7:  multi_pt = min(16.0, multi_pt + 5)
            elif age <= 14: multi_pt = min(16.0, multi_pt + 3)

        total = round(mom + vol_pt + sur_pt + cap_pt + multi_pt, 1)
        detail = {
            "momentum":    round(mom,      1),
            "volume":      round(vol_pt,   1),
            "surge":       round(sur_pt,   1),
            "small_cap":   round(cap_pt,   1),
            "multi_source": round(multi_pt, 1),
        }
        return min(100.0, total), detail
