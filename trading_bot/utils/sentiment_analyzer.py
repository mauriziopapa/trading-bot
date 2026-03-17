"""
Market Sentiment Analyzer — v2.0
═══════════════════════════════════════════════════════════════
6 fonti aggregate in un unico score 0-100:

  FONTI ESISTENTI (peso totale 60%)
  ① Fear & Greed Index   — alternative.me        peso 0.30
  ② Funding Rate BTC/ETH — Bitget API            peso 0.20
  ③ Long/Short Ratio BTC — Bitget API            peso 0.10

  NUOVE FONTI GRATUITE (peso totale 40%)
  ④ CoinGecko Trending   — coingecko.com         peso 0.15
  ⑤ CryptoPanic News NLP — cryptopanic.com       peso 0.15
  ⑥ Open Interest Δ 24h  — Bitget API            peso 0.10

Ogni fonte produce:
  - sub_score  : float 0-100
  - status     : "ok" | "warn" | "danger"
  - raw        : str  — valore grezzo leggibile
  - signal     : str  — testo interpretato

Il result finale include una lista "sources" con tutto il dettaglio
per la visualizzazione dashboard.

Cache 15 minuti — aggiornato automaticamente ogni ciclo.
"""

import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from trading_bot.config import settings


# ── Pesi per fonte — devono sommare a 1.0 ──────────────────────────────────
_WEIGHTS = {
    "fear_greed": 0.30,
    "funding":    0.20,
    "ls_ratio":   0.10,
    "trending":   0.15,
    "news":       0.15,
    "oi_delta":   0.10,
}

# ── Coin "major" per bonus trending ────────────────────────────────────────
_MAJORS = {"BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "DOT", "MATIC", "LINK"}


class SentimentAnalyzer:
    """
    Analizzatore sentiment multi-fonte v2.

    Parametri configurabili nel DB (bot_config):
      SENTIMENT_BYPASS      bool  — salta tutto il filtro
      FEAR_GREED_LONG_MIN   float — min F&G per aprire LONG
      FEAR_GREED_LONG_MAX   float — max F&G per aprire LONG
      FEAR_GREED_SHORT_MIN  float — min F&G per aprire SHORT
      FEAR_GREED_SHORT_MAX  float — max F&G per aprire SHORT
      CRYPTOPANIC_TOKEN     str   — token API gratuito (opzionale)
    """

    _HDR = {"User-Agent": "TradingBot/2.0", "Accept": "application/json"}

    def __init__(self):
        self._cache: dict     = {}
        self._cache_ts: float = 0.0
        self._cache_ttl       = 900          # 15 minuti
        # snapshot OI precedente per calcolo delta (fallback)
        self._oi_snap: dict[str, float] = {}
        self._oi_snap_ts: float         = 0.0

    # ─── Public API ──────────────────────────────────────────────────────────

    def get_sentiment(self, force: bool = False) -> dict:
        """
        Ritorna il sentiment aggregato.

        Struttura ritornata (retrocompatibile con v1 + nuovi campi):
        {
          "score":           float,       # 0-100 ponderato
          "label":           str,         # Extreme Fear … Extreme Greed
          "bias":            str,         # bullish | bearish | neutral
          "signals":         list[str],   # testi leggibili
          "sources":         list[dict],  # dettaglio per fonte (NUOVO)
          # raw backward-compat
          "fear_greed":      int,
          "fear_greed_label":str,
          "funding_btc":     float,
          "funding_eth":     float,
          "ls_ratio_btc":    float,
          # nuovi raw
          "trending_score":  float,
          "trending_coins":  list[str],
          "news_score":      float,
          "news_bullish":    int,
          "news_bearish":    int,
          "news_total":      int,
          "news_headlines":  list[str],
          "oi_btc_pct":      float,
          "oi_eth_pct":      float,
          "oi_change_pct":   float,       # media BTC+ETH
        }
        """
        now = time.time()
        if not force and (now - self._cache_ts) < self._cache_ttl:
            return self._cache
        logger.info("[SENTIMENT v2] Aggiornamento 6 fonti...")
        result         = self._compute_sentiment()
        self._cache    = result
        self._cache_ts = now
        return result

    def should_trade_long(self, symbol: str = "BTC") -> tuple[bool, str]:
        try:
            if getattr(settings, "SENTIMENT_BYPASS", False):
                return True, "Sentiment bypass attivo"
        except Exception:
            pass
        s  = self.get_sentiment()
        fg = s.get("fear_greed", s["score"])
        try:
            fg_min = float(getattr(settings, "FEAR_GREED_LONG_MIN",  0))
            fg_max = float(getattr(settings, "FEAR_GREED_LONG_MAX",  80))
        except Exception:
            fg_min, fg_max = 0, 80
        if fg < fg_min:
            return False, f"F&G {fg} < {fg_min} — troppo fearful per LONG"
        if fg > fg_max:
            return False, f"F&G {fg} > {fg_max} — Extreme Greed, evita LONG"
        return True, f"Sentiment OK ({s['score']}) — F&G={fg} in [{fg_min},{fg_max}]"

    def should_trade_short(self, symbol: str = "BTC") -> tuple[bool, str]:
        try:
            if getattr(settings, "SENTIMENT_BYPASS", False):
                return True, "Sentiment bypass attivo"
        except Exception:
            pass
        s  = self.get_sentiment()
        fg = s.get("fear_greed", s["score"])
        try:
            fg_min = float(getattr(settings, "FEAR_GREED_SHORT_MIN", 20))
            fg_max = float(getattr(settings, "FEAR_GREED_SHORT_MAX", 100))
        except Exception:
            fg_min, fg_max = 20, 100
        if fg < fg_min:
            return False, f"F&G {fg} < {fg_min} — troppo fearful per SHORT"
        if fg > fg_max:
            return False, f"F&G {fg} > {fg_max} — fuori range SHORT"
        return True, f"Sentiment OK ({s['score']}) — F&G={fg} in [{fg_min},{fg_max}]"

    def confidence_modifier(self, signal_side: str) -> float:
        score = self.get_sentiment()["score"]
        if signal_side == "buy":
            if score < 25: return 1.3
            if score > 75: return 0.75
        elif signal_side == "sell":
            if score > 75: return 1.3
            if score < 25: return 0.75
        return 1.0

    # ─── Core ────────────────────────────────────────────────────────────────

    def _compute_sentiment(self) -> dict:
        signals: list[str]  = []
        sources: list[dict] = []
        sub: dict[str, float] = {}

        # ── Parallel fetch all 6 sources ─────────────────────────────────
        with ThreadPoolExecutor(max_workers=6) as pool:
            fut_fg      = pool.submit(self._fetch_fear_greed)
            fut_funding = pool.submit(self._fetch_funding_rates)
            fut_ls      = pool.submit(self._fetch_ls_ratio)
            fut_trend   = pool.submit(self._fetch_coingecko_trending)
            fut_news    = pool.submit(self._fetch_cryptopanic_news)
            fut_oi      = pool.submit(self._fetch_oi_delta)

        fg          = fut_fg.result()
        funding     = fut_funding.result()
        ls          = fut_ls.result()
        trending    = fut_trend.result()
        news        = fut_news.result()
        oi          = fut_oi.result()

        # ── ① Fear & Greed ────────────────────────────────────────────────
        fg_score    = fg.get("value", 50)
        fg_label    = fg.get("value_classification", "Neutral")
        sub["fear_greed"] = float(fg_score)

        if fg_score <= 15:   fg_st = "danger"
        elif fg_score <= 25: fg_st = "warn"
        elif fg_score >= 85: fg_st = "danger"
        elif fg_score >= 75: fg_st = "warn"
        else:                fg_st = "ok"

        fg_sig = (
            f"😱 Extreme Fear ({fg_score}) → contrarian LONG" if fg_score < 25
            else f"🤑 Extreme Greed ({fg_score}) → contrarian SHORT" if fg_score > 75
            else f"😐 {fg_label} ({fg_score})"
        )
        signals.append(f"Fear & Greed: {fg_sig}")
        sources.append({
            "id": "fear_greed", "name": "Fear & Greed Index",
            "icon": "😱" if fg_score < 25 else "🤑" if fg_score > 75 else "😐",
            "score": sub["fear_greed"],
            "raw": f"{fg_score} — {fg_label}",
            "weight": _WEIGHTS["fear_greed"],
            "status": fg_st,
            "signal": fg_sig,
            "thresh": "OK: 25–75 | WARN: 15–85 | KO: <15 >85",
        })

        # ── ② Funding Rate ────────────────────────────────────────────────
        funding_btc = funding.get("BTC", 0.0)
        funding_eth = funding.get("ETH", 0.0)
        avg_fund    = (funding_btc + funding_eth) / 2

        if avg_fund > 0.10:
            sub["funding"] = 88; fund_st = "danger"
            fund_sig = f"🔥 Funding estremo BTC={funding_btc:+.3f}% — rischio squeeze long"
        elif avg_fund > 0.05:
            sub["funding"] = 72; fund_st = "warn"
            fund_sig = f"📈 Funding elevato BTC={funding_btc:+.3f}% ETH={funding_eth:+.3f}%"
        elif avg_fund < -0.05:
            sub["funding"] = 12; fund_st = "danger"
            fund_sig = f"💥 Funding negativo estremo BTC={funding_btc:+.3f}% — overshort"
        elif avg_fund < -0.02:
            sub["funding"] = 28; fund_st = "warn"
            fund_sig = f"📉 Funding negativo BTC={funding_btc:+.3f}% ETH={funding_eth:+.3f}%"
        else:
            sub["funding"] = 50; fund_st = "ok"
            fund_sig = f"⚖️ Funding neutro BTC={funding_btc:+.3f}% ETH={funding_eth:+.3f}%"

        signals.append(f"Funding: {fund_sig}")
        sources.append({
            "id": "funding", "name": "Funding Rate",
            "icon": "💰",
            "score": sub["funding"],
            "raw": f"BTC {funding_btc:+.4f}% / ETH {funding_eth:+.4f}% per 8h",
            "weight": _WEIGHTS["funding"],
            "status": fund_st,
            "signal": fund_sig,
            "thresh": "OK: -0.02..+0.05 | WARN: ±0.05..±0.10 | KO: >0.10",
        })

        # ── ③ Long/Short Ratio ────────────────────────────────────────────
        ls_btc  = ls.get("BTC", 1.0)

        if ls_btc >= 2.0:
            sub["ls_ratio"] = 85; ls_st = "danger"
            ls_sig = f"🐂 L/S BTC={ls_btc:.3f} — dominanza long estrema"
        elif ls_btc >= 1.5:
            sub["ls_ratio"] = 70; ls_st = "warn"
            ls_sig = f"🐂 L/S BTC={ls_btc:.3f} — dominanza long"
        elif ls_btc <= 0.5:
            sub["ls_ratio"] = 15; ls_st = "danger"
            ls_sig = f"🐻 L/S BTC={ls_btc:.3f} — dominanza short estrema"
        elif ls_btc <= 0.7:
            sub["ls_ratio"] = 30; ls_st = "warn"
            ls_sig = f"🐻 L/S BTC={ls_btc:.3f} — dominanza short"
        else:
            sub["ls_ratio"] = 50; ls_st = "ok"
            ls_sig = f"⚖️ L/S BTC={ls_btc:.3f} — bilanciato"

        signals.append(f"L/S Ratio: {ls_sig}")
        sources.append({
            "id": "ls_ratio", "name": "Long/Short Ratio",
            "icon": "⚖️",
            "score": sub["ls_ratio"],
            "raw": f"BTC {ls_btc:.3f}",
            "weight": _WEIGHTS["ls_ratio"],
            "status": ls_st,
            "signal": ls_sig,
            "thresh": "OK: 0.7–1.5 | WARN: 1.5–2.0 | KO: >2.0 <0.5",
        })

        # ── ④ CoinGecko Trending ─── NUOVA ────────────────────────────────
        t_score         = trending.get("score", 50.0)
        t_coins         = trending.get("coins", [])
        t_changes       = trending.get("changes", [])
        sub["trending"] = t_score

        n = len(t_coins)
        avg_chg = sum(t_changes) / len(t_changes) if t_changes else 0
        if t_score >= 70:
            t_st  = "warn"
            t_sig = f"🔥 Alto momentum trending — {n} coin hot, avg Δ{avg_chg:+.1f}%"
        elif t_score <= 35:
            t_st  = "ok"    # basso trending = mercato calmo
            t_sig = f"❄️ Trending debole — basso interesse retail ({n} coin)"
        else:
            t_st  = "ok"
            t_sig = f"📊 Trending neutro — {n} coin in top7 CoinGecko"

        signals.append(f"CoinGecko Trending: {t_sig}")
        sources.append({
            "id": "trending", "name": "CoinGecko Trending",
            "icon": "🔥",
            "score": t_score,
            "raw": ", ".join(t_coins[:5]) if t_coins else "—",
            "weight": _WEIGHTS["trending"],
            "status": t_st,
            "signal": t_sig,
            "thresh": "Baseline 35 + punti per major/altcoin trending",
            "extra": {"coins": t_coins, "changes": t_changes},
        })

        # ── ⑤ CryptoPanic News NLP ─── NUOVA ──────────────────────────────
        n_score       = news.get("score", 50.0)
        n_bullish     = news.get("bullish", 0)
        n_bearish     = news.get("bearish", 0)
        n_total       = news.get("total", 0)
        n_headlines   = news.get("headlines", [])
        sub["news"]   = n_score

        if n_score >= 65:
            n_st  = "ok"
            n_sig = f"📰 News bullish — +{n_bullish} vs -{n_bearish} su {n_total} articoli"
        elif n_score <= 35:
            n_st  = "danger"
            n_sig = f"📰 News bearish — -{n_bearish} vs +{n_bullish} su {n_total} articoli"
        else:
            n_st  = "ok"
            n_sig = f"📰 News neutri — {n_bullish}↑ / {n_bearish}↓ su {n_total} articoli"

        signals.append(f"CryptoPanic News: {n_sig}")
        sources.append({
            "id": "news", "name": "CryptoPanic News",
            "icon": "📰",
            "score": n_score,
            "raw": f"+{n_bullish} bullish / -{n_bearish} bearish",
            "weight": _WEIGHTS["news"],
            "status": n_st,
            "signal": n_sig,
            "thresh": "Score = 50 + (bull-bear)/(bull+bear+1)×50",
            "extra": {
                "bullish":   n_bullish,
                "bearish":   n_bearish,
                "total":     n_total,
                "headlines": n_headlines,
            },
        })

        # ── ⑥ Open Interest Δ 24h ─── NUOVA ──────────────────────────────
        oi_btc      = oi.get("btc_pct", 0.0)
        oi_eth      = oi.get("eth_pct", 0.0)
        oi_avg      = (oi_btc + oi_eth) / 2

        if oi_avg > 20:
            sub["oi_delta"] = 85; oi_st = "danger"
            oi_sig = f"🔥 OI +{oi_avg:.1f}% — overheating, rischio long squeeze"
        elif oi_avg > 10:
            sub["oi_delta"] = 70; oi_st = "warn"
            oi_sig = f"📈 OI +{oi_avg:.1f}% 24h — nuovo denaro long in entrata"
        elif oi_avg < -20:
            sub["oi_delta"] = 15; oi_st = "danger"
            oi_sig = f"💥 OI {oi_avg:.1f}% — liquidazioni massive in corso"
        elif oi_avg < -10:
            sub["oi_delta"] = 30; oi_st = "warn"
            oi_sig = f"📉 OI {oi_avg:.1f}% 24h — deleveraging in corso"
        else:
            sub["oi_delta"] = 50; oi_st = "ok"
            oi_sig = f"⚖️ OI stabile — BTC {oi_btc:+.1f}% ETH {oi_eth:+.1f}% 24h"

        signals.append(f"Open Interest: {oi_sig}")
        sources.append({
            "id": "oi_delta", "name": "Open Interest Δ",
            "icon": "📊",
            "score": sub["oi_delta"],
            "raw": f"BTC {oi_btc:+.1f}% / ETH {oi_eth:+.1f}% in 24h",
            "weight": _WEIGHTS["oi_delta"],
            "status": oi_st,
            "signal": oi_sig,
            "thresh": "OK: ±10% | WARN: ±10..20% | KO: >±20%",
            "extra": {"btc_pct": oi_btc, "eth_pct": oi_eth},
        })

        # ── Score finale ponderato ────────────────────────────────────────
        total_w = sum(_WEIGHTS.values())
        final   = round(sum(sub[k] * _WEIGHTS[k] for k in sub) / total_w, 1)

        if final < 25:   label = "Extreme Fear"
        elif final < 45: label = "Fear"
        elif final < 55: label = "Neutral"
        elif final < 75: label = "Greed"
        else:            label = "Extreme Greed"

        bias = "bullish" if final < 40 else "bearish" if final > 60 else "neutral"

        # Aggiungi contribution % a ogni fonte per la barra dashboard
        for src in sources:
            src["contribution"] = round(sub[src["id"]] * _WEIGHTS[src["id"]] / total_w, 1)

        logger.info(
            f"[SENTIMENT v2] Score={final} | {label} | Bias={bias} | "
            f"F&G={fg_score} | Fund={avg_fund:+.3f}% | L/S={ls_btc:.2f} | "
            f"Trend={t_score:.0f} | News={n_score:.0f} | OI={oi_avg:+.1f}%"
        )

        return {
            # Core
            "score":           final,
            "label":           label,
            "bias":            bias,
            "signals":         signals,
            "sources":         sources,          # NUOVO — per dashboard detail
            # backward compat v1
            "fear_greed":      fg_score,
            "fear_greed_label":fg_label,
            "funding_btc":     funding_btc,
            "funding_eth":     funding_eth,
            "ls_ratio_btc":    ls_btc,
            # nuovi raw
            "trending_score":  t_score,
            "trending_coins":  t_coins,
            "news_score":      n_score,
            "news_bullish":    n_bullish,
            "news_bearish":    n_bearish,
            "news_total":      n_total,
            "news_headlines":  n_headlines,
            "oi_btc_pct":      oi_btc,
            "oi_eth_pct":      oi_eth,
            "oi_change_pct":   oi_avg,
        }

    # ─── Fetchers esistenti ───────────────────────────────────────────────────

    def _fetch_fear_greed(self) -> dict:
        try:
            r = requests.get("https://api.alternative.me/fng/?limit=1",
                             headers=self._HDR, timeout=8)
            if r.status_code == 200:
                d = r.json().get("data", [{}])[0]
                return {
                    "value":                int(d.get("value", 50)),
                    "value_classification": d.get("value_classification", "Neutral"),
                }
        except Exception as e:
            logger.debug(f"[SENT] Fear & Greed: {e}")
        return {"value": 50, "value_classification": "Neutral"}

    def _fetch_funding_rates(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for name, sym in {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}.items():
            try:
                r = requests.get(
                    "https://api.bitget.com/api/v2/mix/market/current-fund-rate",
                    params={"symbol": sym, "productType": "USDT-FUTURES"},
                    headers=self._HDR, timeout=8)
                if r.status_code == 200:
                    d = r.json().get("data", {})
                    if isinstance(d, list) and d:
                        d = d[0]
                    out[name] = round(float(d.get("fundingRate", 0) or 0) * 100, 4)
                else:
                    out[name] = 0.0
            except Exception as e:
                logger.debug(f"[SENT] Funding {name}: {e}")
                out[name] = 0.0
        return out

    def _fetch_ls_ratio(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for name, sym in {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}.items():
            try:
                r = requests.get(
                    "https://api.bitget.com/api/v2/mix/market/long-short-ratio",
                    params={"symbol": sym, "productType": "USDT-FUTURES",
                            "period": "1d", "limit": 1},
                    headers=self._HDR, timeout=8)
                if r.status_code == 200:
                    d = r.json().get("data", [])
                    if d:
                        out[name] = round(float(d[0].get("longShortRatio", 1.0) or 1.0), 3)
                    else:
                        out[name] = 1.0
                else:
                    out[name] = 1.0
            except Exception as e:
                logger.debug(f"[SENT] L/S {name}: {e}")
                out[name] = 1.0
        return out

    # ─── Nuovi fetchers ───────────────────────────────────────────────────────

    def _fetch_coingecko_trending(self) -> dict:
        """
        GET /search/trending — no auth, 30 req/min demo.
        Score 0-100:
          - baseline 35 (trending di per sé non è bullish)
          - +15 per ogni major (BTC/ETH/SOL…) in top7
          - +8 per ogni altcoin in top7
          - ±10 bonus se avg price_change_24h > +5% / < -5%
        """
        try:
            r = requests.get(
                "https://api.coingecko.com/api/v3/search/trending",
                headers=self._HDR, timeout=10)
            if r.status_code != 200:
                return {"score": 50.0, "coins": [], "changes": []}

            coins   = r.json().get("coins", [])
            names   = []
            changes = []
            score   = 35.0

            for c in coins[:7]:
                item = c.get("item", {})
                sym  = item.get("symbol", "").upper()
                names.append(sym)
                score += 15 if sym in _MAJORS else 8

                # price change 24h (struttura annidati in CoinGecko)
                pd   = item.get("data", {})
                chg  = pd.get("price_change_percentage_24h", {})
                val  = chg.get("usd", 0) if isinstance(chg, dict) else float(chg or 0)
                changes.append(round(float(val), 2))

            if len(changes) >= 3:
                avg = sum(changes) / len(changes)
                if avg > 5:   score += 10
                elif avg < -5: score -= 10

            score = max(0.0, min(100.0, score))
            logger.debug(f"[SENT] CoinGecko trending {names} → {score:.1f}")
            return {"score": round(score, 1), "coins": names, "changes": changes}

        except Exception as e:
            logger.debug(f"[SENT] CoinGecko trending: {e}")
            return {"score": 50.0, "coins": [], "changes": []}

    def _fetch_cryptopanic_news(self) -> dict:
        """
        GET /api/v1/posts — voti community bullish/bearish.
        Score = 50 + (bull-bear)/(bull+bear+1) * 50  → [0, 100]
        Token opzionale: CRYPTOPANIC_TOKEN in bot_config.
        Senza token usa 'public' (rate limited ma funzionante).
        """
        try:
            token = "public"
            try:
                t = getattr(settings, "CRYPTOPANIC_TOKEN", None)
                if t: token = t
            except Exception:
                pass

            r = requests.get(
                "https://cryptopanic.com/api/v1/posts/",
                params={"auth_token": token, "kind": "news",
                        "public": "true", "filter": "hot"},
                headers=self._HDR, timeout=12)

            if r.status_code == 429:
                logger.debug("[SENT] CryptoPanic rate limited")
                return {"score": 50.0, "bullish": 0, "bearish": 0,
                        "total": 0, "headlines": []}
            if r.status_code != 200:
                return {"score": 50.0, "bullish": 0, "bearish": 0,
                        "total": 0, "headlines": []}

            posts     = r.json().get("results", [])
            bullish   = 0
            bearish   = 0
            headlines = []

            for p in posts[:50]:
                v        = p.get("votes", {})
                bullish += int(v.get("positive", 0) or 0) + int(v.get("liked", 0) or 0)
                bearish += int(v.get("negative", 0) or 0) + int(v.get("disliked", 0) or 0)
                t = p.get("title", "")
                if t:
                    headlines.append(t[:80])

            ratio = (bullish - bearish) / (bullish + bearish + 1)
            score = round(max(0.0, min(100.0, 50.0 + ratio * 50.0)), 1)
            logger.debug(f"[SENT] CryptoPanic +{bullish}/-{bearish} → {score}")
            return {"score": score, "bullish": bullish, "bearish": bearish,
                    "total": len(posts), "headlines": headlines[:5]}

        except Exception as e:
            logger.debug(f"[SENT] CryptoPanic: {e}")
            return {"score": 50.0, "bullish": 0, "bearish": 0,
                    "total": 0, "headlines": []}

    def _fetch_oi_delta(self) -> dict:
        """
        Variazione Open Interest 24h da Bitget.
        Prova /open-interest-history (2 snapshot giornalieri).
        Fallback: snapshot in memoria confrontato con current OI.
        """
        now = time.time()
        out = {"btc_pct": 0.0, "eth_pct": 0.0}

        for name, sym in {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}.items():
            key = f"{name}_oi"
            try:
                # Prova history endpoint
                r = requests.get(
                    "https://api.bitget.com/api/v2/mix/market/open-interest-history",
                    params={"symbol": sym, "productType": "USDT-FUTURES",
                            "period": "1D", "limit": 2},
                    headers=self._HDR, timeout=10)

                if r.status_code == 200:
                    data = r.json().get("data", [])
                    if isinstance(data, list) and len(data) >= 2:
                        def _sz(d):
                            lst = d.get("openInterestList", [])
                            return float((lst[0].get("size", 0) if lst else d.get("size", 0)) or 0)
                        oi_now  = _sz(data[0])
                        oi_prev = _sz(data[1])
                        if oi_prev > 0:
                            out[f"{name.lower()}_pct"] = round((oi_now - oi_prev) / oi_prev * 100, 2)
                            continue

                # Fallback: current OI vs snapshot memoria
                r2 = requests.get(
                    "https://api.bitget.com/api/v2/mix/market/open-interest",
                    params={"symbol": sym, "productType": "USDT-FUTURES"},
                    headers=self._HDR, timeout=8)
                if r2.status_code == 200:
                    d    = r2.json().get("data", {})
                    if isinstance(d, list) and d: d = d[0]
                    lst  = d.get("openInterestList", [])
                    size = float((lst[0].get("size", 0) if lst else d.get("size", 0)) or 0)

                    prev = self._oi_snap.get(key, 0.0)
                    prev_ts = self._oi_snap.get(f"{key}_ts", 0.0)
                    if prev > 0 and (now - prev_ts) > 300:
                        out[f"{name.lower()}_pct"] = round((size - prev) / prev * 100, 2)
                    if now - prev_ts > 900:
                        self._oi_snap[key]          = size
                        self._oi_snap[f"{key}_ts"]  = now

            except Exception as e:
                logger.debug(f"[SENT] OI {name}: {e}")

        return out
