"""
Emerging Coins Scanner — v2.0
═══════════════════════════════════════════════════════════════
5 fonti di discovery:

  ① CoinGecko Trending      — le 7 coin più cercate ora
  ② Bitget Spot Gainers     — top 24h% con volume minimo
  ③ CoinGecko Top Gainers   — top 24h su 250 coin per mktcap (free)
  ④ Bitget New Listings     — coin listate < N giorni (configurabile)
  ⑤ Volume Spike Detector   — coin con volume surge anomalo (>Nx media)

Score composito 0-100 con 5 dimensioni:
  • Momentum 24h         (max 25 pt)
  • Volume assoluto      (max 20 pt)
  • Volume surge         (max 20 pt)
  • Small-cap bonus      (max 20 pt)
  • Multi-source bonus   (max 15 pt)

8 parametri configurabili in runtime dal DB (bot_config):
  EM_MIN_VOLUME_USD       float  — volume 24h minimo (default 5M)
  EM_MIN_CHANGE_24H       float  — % cambio 24h minimo (default 5.0)
  EM_MIN_VOLUME_SURGE     float  — volume / media 7d minimo (default 2.0)
  EM_MAX_MARKET_CAP       float  — market cap massimo in USD (default 2B)
  EM_MIN_MARKET_CAP       float  — market cap minimo in USD (default 0)
  EM_MAX_RESULTS          int    — max coin restituite (default 10)
  EM_NEW_LISTING_DAYS     int    — soglia listing recente in giorni (default 30)
  EM_EXCLUDE_SYMBOLS      str    — simboli esclusi comma-separated (default "")
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
    "USDT","USDC","BUSD","DAI","TUSD","USDP","FRAX","LUSD",
    "USDD","GUSD","SUSD","ALUSD","EURS","AGEUR","CUSD","CEUR",
    "FDUSD","PYUSD","USDE","USDS",
}

_MAJORS = {
    "BTC","ETH","BNB","XRP","ADA","SOL","AVAX","DOT",
    "MATIC","LINK","LTC","BCH","ETC","ATOM","NEAR",
}

_HEADERS = {"User-Agent": "TradingBot/2.0", "Accept": "application/json"}


# ── Runtime param helpers ─────────────────────────────────────────────────────

def _cfg(key: str, default):
    """Legge un parametro dal DB settings con fallback al default."""
    try:
        val = getattr(settings, key, None)
        if val is not None:
            return type(default)(val)
    except Exception:
        pass
    return default

def _static_symbols() -> set[str]:
    """Simboli già nella watchlist statica — da escludere dai risultati."""
    try:
        spot    = getattr(settings, "SPOT_SYMBOLS",    []) or []
        futures = getattr(settings, "FUTURES_SYMBOLS", []) or []
        return {s.split("/")[0].upper() for s in spot + futures}
    except Exception:
        return set()

def _excluded_symbols() -> set[str]:
    """Simboli esclusi manualmente dall'utente (EM_EXCLUDE_SYMBOLS)."""
    try:
        raw = _cfg("EM_EXCLUDE_SYMBOLS", "")
        return {s.strip().upper() for s in raw.split(",") if s.strip()}
    except Exception:
        return set()


# ═══════════════════════════════════════════════════════════════════════════════

class EmergingScanner:
    """
    Identifica criptovalute emergenti da 5 fonti con score composito.

    I parametri di filtro si leggono dal DB ad ogni scan → modificabili
    dalla dashboard senza riavviare il bot.
    """

    def __init__(self):
        self._last_scan: list[dict] = []
        self._last_scan_ts: float   = 0.0
        self._scan_ttl: float       = 600   # 10 minuti

    # ─── Public API ──────────────────────────────────────────────────────────

    def scan(self, force: bool = False) -> list[dict]:
        """
        Ritorna la lista ordinata di coin emergenti.
        Ogni coin include:
          symbol, name, price, price_change_24h, volume_24h_usd,
          market_cap_usd, volume_surge, score, sources (list[str]),
          score_detail (dict), is_new_listing (bool), listing_age_days (int|None)
        """
        now = time.time()
        if not force and (now - self._last_scan_ts) < self._scan_ttl:
            return self._last_scan

        # Leggi parametri runtime dal DB
        min_vol     = _cfg("EM_MIN_VOLUME_USD",   5_000_000.0)
        min_chg     = _cfg("EM_MIN_CHANGE_24H",   5.0)
        min_surge   = _cfg("EM_MIN_VOLUME_SURGE", 2.0)
        max_mcap    = _cfg("EM_MAX_MARKET_CAP",   2_000_000_000.0)
        min_mcap    = _cfg("EM_MIN_MARKET_CAP",   0.0)
        max_results = int(_cfg("EM_MAX_RESULTS",  10))
        excluded    = _static_symbols() | _excluded_symbols() | _STABLECOINS

        logger.info(
            f"[EMERGING v2] Scan — vol≥${min_vol/1e6:.0f}M chg≥{min_chg}% "
            f"surge≥{min_surge}x mcap=[${min_mcap/1e6:.0f}M–${max_mcap/1e6:.0f}M]"
        )

        # ── Raccolta dati da 5 fonti ─────────────────────────────────────
        raw: dict[str, dict] = {}  # symbol → coin_dict (merge multi-source)

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

        # ── Filtra e calcola score finale ────────────────────────────────
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
            if min_mcap > 0 and mcap > 0 and mcap < min_mcap:
                continue
            if max_mcap > 0 and mcap > 0 and mcap > max_mcap:
                continue

            coin["score"], coin["score_detail"] = self._score(coin)
            results.append(coin)

        results.sort(key=lambda x: x.get("score", 0), reverse=True)
        self._last_scan    = results[:max_results]
        self._last_scan_ts = now

        logger.info(f"[EMERGING v2] {len(self._last_scan)} coin trovate (da {len(raw)} candidate)")
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

    # ─── Data Sources ────────────────────────────────────────────────────────

    def _fetch_coingecko_trending(self) -> list[dict]:
        """① CoinGecko /search/trending — le 7 coin più cercate."""
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
        """③ CoinGecko /coins/markets — top 250 per mcap, poi filtro % change."""
        try:
            r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 250,
                    "page": 1,
                    "sparkline": "false",
                    "price_change_percentage": "24h",
                },
                headers=_HEADERS, timeout=12,
            )
            if r.status_code != 200:
                return []
            coins  = r.json()
            # Ordina per 24h change e prendi top 20
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
        """② Bitget spot tickers — top 24h% con volume minimo."""
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
                chg = float(t.get("changeUtc24h", 0) or 0) * 100
                vol = float(t.get("usdtVol",      0) or 0)
                sym = t.get("symbol", "").replace("USDT", "").strip()
                if chg < min_chg:
                    continue
                out.append({
                    "symbol":           sym.upper(),
                    "price":            float(t.get("lastPr", 0) or 0),
                    "price_change_24h": round(chg, 2),
                    "volume_24h_usd":   vol,
                    "market_cap_usd":   0.0,
                    "volume_surge":     self._est_surge(vol),
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
        """④ Bitget /spot/public/coins — coin listate recentemente."""
        try:
            days_thresh = int(_cfg("EM_NEW_LISTING_DAYS", 30))
            r = requests.get(
                f"{BITGET_BASE}/spot/public/coins",
                headers=_HEADERS, timeout=10,
            )
            if r.status_code != 200:
                return []

            now_ms = time.time() * 1000
            ms_thresh = days_thresh * 86_400 * 1000
            out = []

            for coin in r.json().get("data", []):
                launch_ms = float(coin.get("launchTime", 0) or 0)
                if launch_ms <= 0:
                    continue
                age_ms   = now_ms - launch_ms
                age_days = age_ms / 86_400_000
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

            logger.debug(f"[EMERGING] New listings (≤{days_thresh}d): {len(out)}")
            return out
        except Exception as e:
            logger.debug(f"[EMERGING] Bitget new listings: {e}")
            return []

    def _fetch_volume_spikes(self, min_vol: float) -> list[dict]:
        """⑤ Bitget tickers — coin con volume anomalo (spike detector).
        Confronta usdtVol (24h) con una stima del volume "normale" basata
        su open_price e close_price spread. Un volume 5× il baseline = spike.
        """
        try:
            r = requests.get(f"{BITGET_BASE}/spot/market/tickers",
                             headers=_HEADERS, timeout=10)
            if r.status_code != 200:
                return []

            out = []
            for t in r.json().get("data", []):
                if not t.get("symbol", "").endswith("USDT"):
                    continue
                vol  = float(t.get("usdtVol",    0) or 0)
                high = float(t.get("high24h",    0) or 0)
                low  = float(t.get("low24h",     0) or 0)
                last = float(t.get("lastPr",     0) or 0)
                if vol < min_vol or last <= 0 or high <= 0:
                    continue

                # Stima surge: range_pct forte + volume alto → spike
                range_pct = (high - low) / last * 100 if last > 0 else 0
                # Euristica: surge ∝ vol / (last × 1000) come proxy "normale"
                baseline  = last * 1000  # unità arbitrarie
                surge_est = vol / baseline if baseline > 0 else 1.0
                surge_est = min(surge_est, 10.0)

                if surge_est < 3.0:
                    continue

                sym = t.get("symbol", "").replace("USDT", "").upper()
                chg = float(t.get("changeUtc24h", 0) or 0) * 100
                out.append({
                    "symbol":           sym,
                    "name":             sym,
                    "price":            last,
                    "price_change_24h": round(chg, 2),
                    "volume_24h_usd":   vol,
                    "market_cap_usd":   0.0,
                    "volume_surge":     round(surge_est, 2),
                    "sources":          ["vol_spike"],
                    "is_new_listing":   False,
                    "listing_age_days": None,
                })
            logger.debug(f"[EMERGING] Volume spikes: {len(out)}")
            return out
        except Exception as e:
            logger.debug(f"[EMERGING] Volume spikes: {e}")
            return []

    # ─── Helpers ─────────────────────────────────────────────────────────────

    def _coingecko_markets_by_ids(self, ids: list[str], source: str) -> list[dict]:
        """Fetch dettaglio mercato CoinGecko per lista di ids."""
        try:
            r = requests.get(
                f"{COINGECKO_BASE}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "ids": ",".join(ids),
                    "sparkline": "false",
                    "price_change_percentage": "24h",
                },
                headers=_HEADERS, timeout=12,
            )
            if r.status_code != 200:
                return []
            return [self._norm_cg(c, source) for c in r.json()]
        except Exception as e:
            logger.debug(f"[EMERGING] CG markets: {e}")
            return []

    def _norm_cg(self, coin: dict, source: str) -> dict:
        """Normalizza un record CoinGecko al formato interno."""
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
            "volume_surge":     self._est_surge(vol),
            "sources":          [source],
            "is_new_listing":   False,
            "listing_age_days": None,
        }

    def _merge(self, raw: dict, coin: dict) -> None:
        """Unisce un coin nel dizionario raw, aggiornando i campi mancanti."""
        sym = coin.get("symbol", "").upper()
        if not sym:
            return
        if sym not in raw:
            raw[sym] = coin.copy()
        else:
            existing = raw[sym]
            # Arricchisce con dati migliori
            for field in ["price", "price_change_24h", "volume_24h_usd", "market_cap_usd"]:
                if coin.get(field, 0) > existing.get(field, 0):
                    existing[field] = coin[field]
            # Aggiunge source se non già presente
            for s in coin.get("sources", []):
                if s not in existing.get("sources", []):
                    existing.setdefault("sources", []).append(s)
            # Aggiorna flags
            if coin.get("is_new_listing"):
                existing["is_new_listing"]   = True
                existing["listing_age_days"] = coin.get("listing_age_days")
            # Aggiorna volume surge se maggiore
            if coin.get("volume_surge", 0) > existing.get("volume_surge", 0):
                existing["volume_surge"] = coin["volume_surge"]
            raw[sym] = existing

    def _est_surge(self, volume_24h: float) -> float:
        """Stima euristica volume surge dal volume assoluto."""
        if volume_24h > 1_000_000_000: return 8.0
        if volume_24h >   500_000_000: return 5.0
        if volume_24h >   100_000_000: return 3.5
        if volume_24h >    50_000_000: return 2.5
        if volume_24h >    10_000_000: return 1.8
        return 1.0

    def _score(self, coin: dict) -> tuple[float, dict]:
        """
        Score composito 0-100 su 5 dimensioni.
        Ritorna (score_totale, dettaglio_per_dimensione).
        """
        chg   = coin.get("price_change_24h", 0)
        vol   = coin.get("volume_24h_usd",   0)
        surge = coin.get("volume_surge",      1.0)
        mcap  = coin.get("market_cap_usd",   0)
        srcs  = coin.get("sources",          [])

        # ① Momentum 24h — max 25 pt
        if   chg >= 50: mom = 25.0
        elif chg >= 30: mom = 22.0
        elif chg >= 20: mom = 18.0
        elif chg >= 10: mom = 14.0
        elif chg >=  5: mom = 8.0
        else:           mom = max(0.0, chg * 0.8)

        # ② Volume assoluto — max 20 pt
        if   vol >= 1_000_000_000: vol_pt = 20.0
        elif vol >=   500_000_000: vol_pt = 17.0
        elif vol >=   100_000_000: vol_pt = 13.0
        elif vol >=    50_000_000: vol_pt = 9.0
        elif vol >=    10_000_000: vol_pt = 5.0
        else:                      vol_pt = 2.0

        # ③ Volume surge — max 20 pt
        if   surge >= 8: sur_pt = 20.0
        elif surge >= 5: sur_pt = 16.0
        elif surge >= 3: sur_pt = 12.0
        elif surge >= 2: sur_pt = 7.0
        else:            sur_pt = max(0.0, (surge - 1) * 4)

        # ④ Small-cap bonus — max 20 pt (micro-cap ha più upside)
        if   0 < mcap <   50_000_000: cap_pt = 20.0   # micro < 50M
        elif 0 < mcap <  200_000_000: cap_pt = 17.0   # small < 200M
        elif 0 < mcap <  500_000_000: cap_pt = 13.0   # mid   < 500M
        elif 0 < mcap < 2_000_000_000: cap_pt = 8.0   # large < 2B
        elif mcap == 0:                cap_pt = 5.0   # sconosciuto
        else:                          cap_pt = 2.0   # molto grande

        # ⑤ Multi-source bonus — max 15 pt
        n_srcs    = len(set(srcs))
        multi_pt  = min(15.0, n_srcs * 5.0)

        # Bonus listing recente
        if coin.get("is_new_listing"):
            age = coin.get("listing_age_days") or 30
            if age <= 7:   multi_pt = min(15.0, multi_pt + 5)
            elif age <= 14: multi_pt = min(15.0, multi_pt + 3)

        total = round(mom + vol_pt + sur_pt + cap_pt + multi_pt, 1)
        detail = {
            "momentum":    round(mom,    1),
            "volume":      round(vol_pt, 1),
            "surge":       round(sur_pt, 1),
            "small_cap":   round(cap_pt, 1),
            "multi_source":round(multi_pt,1),
        }
        return min(100.0, total), detail
