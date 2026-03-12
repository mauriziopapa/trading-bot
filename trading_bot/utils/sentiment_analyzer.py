"""
Market Sentiment Analyzer
Aggrega segnali di sentiment da fonti multiple:
1. Fear & Greed Index (alternative.me — gratuito)
2. Funding Rate Futures (Bitget API) — indica sentiment derivati
3. Long/Short Ratio (Bitget API) — posizionamento mercato
4. Open Interest change — denaro nuovo che entra/esce

Il SentimentScore finale (0-100) viene usato come filtro:
- Score < 25 → Fear Estremo → favorisce LONG contrarian
- Score > 75 → Greed Estremo → favorisce SHORT contrarian
- Score 40-60 → Neutro → tutte le strategie operative
"""

import requests
import time
from loguru import logger
from trading_bot.config import settings


class SentimentAnalyzer:
    """
    Analizzatore di sentiment multi-fonte.
    Usato come filtro aggiuntivo per validare i segnali delle strategie.

    Parametri configurabili dal DB (bot_config):
      SENTIMENT_BYPASS     (bool)  — se True salta il filtro sentiment (utile per test)
      FEAR_GREED_LONG_MIN  (float) — Fear & Greed minimo per aprire LONG  (default 0)
      FEAR_GREED_LONG_MAX  (float) — Fear & Greed massimo per aprire LONG  (default 80)
      FEAR_GREED_SHORT_MIN (float) — Fear & Greed minimo per aprire SHORT (default 20)
      FEAR_GREED_SHORT_MAX (float) — Fear & Greed massimo per aprire SHORT (default 100)
    """

    def __init__(self):
        self._cache: dict     = {}
        self._cache_ts: float = 0
        self._cache_ttl       = 900   # 15 minuti

    # ─── Public API ──────────────────────────────────────────────────────────

    def get_sentiment(self, force: bool = False) -> dict:
        """
        Ritorna il sentiment aggregato del mercato.
        Struttura ritornata:
        {
            "score": 0-100,          # score aggregato
            "label": str,            # Extreme Fear / Fear / Neutral / Greed / Extreme Greed
            "fear_greed": int,       # Fear & Greed Index raw
            "fear_greed_label": str,
            "funding_btc": float,    # funding rate BTC (% per 8h)
            "funding_eth": float,
            "ls_ratio_btc": float,   # Long/Short ratio BTC
            "oi_change_pct": float,  # variazione OI 24h %
            "bias": str,             # "bullish" | "bearish" | "neutral"
            "signals": list[str],    # segnali leggibili
        }
        """
        now = time.time()
        if not force and (now - self._cache_ts) < self._cache_ttl:
            return self._cache

        logger.info("[SENTIMENT] Aggiornamento dati sentiment...")
        result = self._compute_sentiment()
        self._cache    = result
        self._cache_ts = now
        return result

    def should_trade_long(self, symbol: str = "BTC") -> tuple[bool, str]:
        """
        Ritorna True se il sentiment supporta posizioni LONG.
        Legge soglie dal DB (bot_config) — configurabili in runtime dalla dashboard.
        """
        # SENTIMENT_BYPASS: se True salta completamente il filtro (utile per test)
        try:
            if getattr(settings, "SENTIMENT_BYPASS", False):
                return True, "Sentiment bypass attivo — filtro disabilitato"
        except Exception:
            pass

        s = self.get_sentiment()
        score = s["score"]
        fg    = s.get("fear_greed", score)   # usa Fear & Greed raw se disponibile

        # Leggi soglie dal DB con fallback ai default storici
        try:
            fg_min = float(getattr(settings, "FEAR_GREED_LONG_MIN",  0))
            fg_max = float(getattr(settings, "FEAR_GREED_LONG_MAX",  80))
        except Exception:
            fg_min, fg_max = 0, 80

        if fg < fg_min:
            return False, f"Fear & Greed {fg} < {fg_min} (min LONG) → troppo fearful"
        if fg > fg_max:
            return False, f"Fear & Greed {fg} > {fg_max} (max LONG) → Extreme Greed, evita LONG"
        if score < 20:
            return True, f"Extreme Fear ({score}) → contrarian LONG favorito"
        return True, f"Sentiment OK ({score}) — F&G={fg} in range [{fg_min}, {fg_max}]"

    def should_trade_short(self, symbol: str = "BTC") -> tuple[bool, str]:
        """
        Ritorna True se il sentiment supporta posizioni SHORT.
        Legge soglie dal DB (bot_config) — configurabili in runtime dalla dashboard.
        """
        # SENTIMENT_BYPASS: se True salta completamente il filtro
        try:
            if getattr(settings, "SENTIMENT_BYPASS", False):
                return True, "Sentiment bypass attivo — filtro disabilitato"
        except Exception:
            pass

        s = self.get_sentiment()
        score = s["score"]
        fg    = s.get("fear_greed", score)

        # Leggi soglie dal DB con fallback ai default storici
        try:
            fg_min = float(getattr(settings, "FEAR_GREED_SHORT_MIN", 20))
            fg_max = float(getattr(settings, "FEAR_GREED_SHORT_MAX", 100))
        except Exception:
            fg_min, fg_max = 20, 100

        if fg < fg_min:
            return False, f"Fear & Greed {fg} < {fg_min} (min SHORT) → troppo fearful per short"
        if fg > fg_max:
            return False, f"Fear & Greed {fg} > {fg_max} → fuori range SHORT"
        if score > 80:
            return True, f"Extreme Greed ({score}) → contrarian SHORT favorito"
        return True, f"Sentiment OK ({score}) — F&G={fg} in range [{fg_min}, {fg_max}]"

    def confidence_modifier(self, signal_side: str) -> float:
        """
        Ritorna un moltiplicatore per la confidence del segnale
        basato sul sentiment corrente.
        Range: 0.7 (contrario) - 1.3 (favorevole)
        """
        s = self.get_sentiment()
        score = s["score"]

        if signal_side == "buy":
            if score < 25:    return 1.3   # fear → LONG favorito
            if score > 75:    return 0.75  # greed → LONG rischioso
            return 1.0

        elif signal_side == "sell":
            if score > 75:    return 1.3   # greed → SHORT favorito
            if score < 25:    return 0.75  # fear → SHORT rischioso
            return 1.0

        return 1.0

    # ─── Computation ─────────────────────────────────────────────────────────

    def _compute_sentiment(self) -> dict:
        signals = []
        score_components = []

        # 1. Fear & Greed Index
        fg = self._fetch_fear_greed()
        fg_score = fg.get("value", 50)
        fg_label = fg.get("value_classification", "Neutral")
        score_components.append(("fear_greed", fg_score, 0.5))   # peso 50%

        if fg_score < 25:
            signals.append(f"😱 Fear & Greed: {fg_label} ({fg_score}) → segnale contrarian LONG")
        elif fg_score > 75:
            signals.append(f"🤑 Fear & Greed: {fg_label} ({fg_score}) → segnale contrarian SHORT")
        else:
            signals.append(f"😐 Fear & Greed: {fg_label} ({fg_score})")

        # 2. Funding Rates
        funding = self._fetch_funding_rates()
        funding_btc = funding.get("BTC", 0.0)
        funding_eth = funding.get("ETH", 0.0)

        # Funding positivo = mercato long → bearish contrarian signal
        # Funding negativo = mercato short → bullish contrarian signal
        avg_funding = (funding_btc + funding_eth) / 2
        if avg_funding > 0.05:
            funding_score = 75    # molti long → mercato overbought
            signals.append(f"📈 Funding elevato BTC={funding_btc:.3f}% ETH={funding_eth:.3f}% → mercato overlong")
        elif avg_funding < -0.02:
            funding_score = 25    # molti short → mercato oversold
            signals.append(f"📉 Funding negativo BTC={funding_btc:.3f}% ETH={funding_eth:.3f}% → mercato overshort")
        else:
            funding_score = 50
            signals.append(f"⚖️ Funding neutro BTC={funding_btc:.3f}% ETH={funding_eth:.3f}%")
        score_components.append(("funding", funding_score, 0.3))   # peso 30%

        # 3. Long/Short Ratio
        ls_ratio = self._fetch_ls_ratio()
        ls_btc = ls_ratio.get("BTC", 1.0)
        if ls_btc > 1.5:
            ls_score = 75    # molti più long che short
            signals.append(f"🐂 L/S Ratio BTC={ls_btc:.2f} → dominanza long")
        elif ls_btc < 0.7:
            ls_score = 25    # molti più short
            signals.append(f"🐻 L/S Ratio BTC={ls_btc:.2f} → dominanza short")
        else:
            ls_score = 50
            signals.append(f"⚖️ L/S Ratio BTC={ls_btc:.2f} → bilanciato")
        score_components.append(("ls_ratio", ls_score, 0.2))   # peso 20%

        # Calcola score ponderato
        total_weight = sum(w for _, _, w in score_components)
        weighted_score = sum(s * w for _, s, w in score_components) / total_weight
        final_score = round(weighted_score, 1)

        # Label
        if final_score < 25:   label = "Extreme Fear"
        elif final_score < 45: label = "Fear"
        elif final_score < 55: label = "Neutral"
        elif final_score < 75: label = "Greed"
        else:                  label = "Extreme Greed"

        # Bias direzionale
        if final_score < 40:   bias = "bullish"    # fear → contrarian bullish
        elif final_score > 60: bias = "bearish"    # greed → contrarian bearish
        else:                  bias = "neutral"

        result = {
            "score":            final_score,
            "label":            label,
            "fear_greed":       fg_score,
            "fear_greed_label": fg_label,
            "funding_btc":      funding_btc,
            "funding_eth":      funding_eth,
            "ls_ratio_btc":     ls_btc,
            "bias":             bias,
            "signals":          signals,
        }

        logger.info(
            f"[SENTIMENT] Score={final_score} | {label} | Bias={bias} | "
            f"F&G={fg_score} | Funding={avg_funding:.3f}% | L/S={ls_btc:.2f}"
        )
        return result

    # ─── Data Fetchers ───────────────────────────────────────────────────────

    def _fetch_fear_greed(self) -> dict:
        """Fetch Fear & Greed Index da alternative.me (gratuito, no auth)."""
        try:
            r = requests.get(
                "https://api.alternative.me/fng/?limit=1",
                timeout=8
            )
            if r.status_code == 200:
                data = r.json().get("data", [{}])[0]
                return {
                    "value":                  int(data.get("value", 50)),
                    "value_classification":   data.get("value_classification", "Neutral"),
                    "timestamp":              data.get("timestamp", ""),
                }
        except Exception as e:
            logger.debug(f"[SENTIMENT] Fear & Greed fetch error: {e}")
        return {"value": 50, "value_classification": "Neutral"}

    def _fetch_funding_rates(self) -> dict[str, float]:
        """Fetch funding rates da Bitget per BTC e ETH futures."""
        result = {}
        symbols = {
            "BTC": "BTCUSDT",
            "ETH": "ETHUSDT",
        }
        for name, symbol in symbols.items():
            try:
                r = requests.get(
                    "https://api.bitget.com/api/v2/mix/market/current-fund-rate",
                    params={"symbol": symbol, "productType": "USDT-FUTURES"},
                    timeout=8
                )
                if r.status_code == 200:
                    data = r.json().get("data", {})
                    if isinstance(data, list) and data:
                        data = data[0]
                    rate = float(data.get("fundingRate", 0) or 0) * 100
                    result[name] = round(rate, 4)
                else:
                    result[name] = 0.0
            except Exception as e:
                logger.debug(f"[SENTIMENT] Funding rate {name} error: {e}")
                result[name] = 0.0
        return result

    def _fetch_ls_ratio(self) -> dict[str, float]:
        """Fetch Long/Short ratio da Bitget."""
        result = {}
        symbols = {
            "BTC": "BTCUSDT",
            "ETH": "ETHUSDT",
        }
        for name, symbol in symbols.items():
            try:
                r = requests.get(
                    "https://api.bitget.com/api/v2/mix/market/long-short-ratio",
                    params={
                        "symbol": symbol,
                        "productType": "USDT-FUTURES",
                        "period": "1d",
                        "limit": 1
                    },
                    timeout=8
                )
                if r.status_code == 200:
                    data = r.json().get("data", [])
                    if data:
                        ratio = float(data[0].get("longShortRatio", 1.0) or 1.0)
                        result[name] = round(ratio, 3)
                    else:
                        result[name] = 1.0
                else:
                    result[name] = 1.0
            except Exception as e:
                logger.debug(f"[SENTIMENT] L/S ratio {name} error: {e}")
                result[name] = 1.0
        return result
