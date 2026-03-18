"""
Sniper Scanner v2.0 — Production-Grade Multi-Factor Selection Engine
════════════════════════════════════════════════════════════════════

3-stage pipeline:
  1. Universe Selection  → batch tickers from exchange futures
  2. Feature Engineering → momentum, volatility, volume surge
  3. Scoring Engine      → weighted multi-factor rank

NEVER returns empty list. Falls back to top volume symbols.
Uses exchange CCXT methods only — no external API dependencies.
"""

import time
import statistics
from loguru import logger

from trading_bot.config import settings


def _safe_float(x, default=0.0):
    try:
        return float(x) if x is not None else default
    except (ValueError, TypeError):
        return default


class SniperScannerV2:

    # Scoring weights (must sum to 100)
    W_MOMENTUM   = 40
    W_VOLUME     = 25
    W_VOLATILITY = 20
    W_CHANGE     = 15

    # Defaults — overridden by settings if available
    DEFAULT_MIN_VOLUME   = 1_000_000   # USD
    DEFAULT_MAX_SPREAD   = 0.003       # 0.3%
    DEFAULT_UNIVERSE     = 100         # symbols to scan
    DEFAULT_OHLCV_LIMIT  = 20          # top candidates for OHLCV
    DEFAULT_RESULTS      = 15          # final output count

    def __init__(self, exchange=None):
        self._exchange = exchange
        self._cache = []
        self._cache_ts = 0
        self._cache_ttl = 25  # seconds — shorter than 30s scan cycle

        # Hardcoded fallback symbols — always tradeable on Bitget futures
        self._fallback_symbols = [
            "BTC/USDT:USDT",
            "ETH/USDT:USDT",
            "SOL/USDT:USDT",
            "XRP/USDT:USDT",
            "DOGE/USDT:USDT",
            "AVAX/USDT:USDT",
            "LINK/USDT:USDT",
            "MATIC/USDT:USDT",
            "ARB/USDT:USDT",
            "OP/USDT:USDT",
        ]

    def set_exchange(self, exchange):
        """Set exchange reference (called after init if not passed to constructor)."""
        self._exchange = exchange

    # ══════════════════════════════════════════════════════════
    # PUBLIC API
    # ══════════════════════════════════════════════════════════

    def scan(self, force=False, regime=None):
        """
        Main entry point. Returns list of scored candidates.
        NEVER returns empty list.
        """
        now = time.time()

        # Cache check
        if not force and self._cache and (now - self._cache_ts) < self._cache_ttl:
            return self._cache

        if not self._exchange:
            logger.error("[SCANNER] no exchange reference — returning fallback")
            return self._build_fallback()

        try:
            results = self._run_pipeline()
        except Exception as e:
            logger.error(f"[SCANNER] pipeline error: {e}")
            results = []

        # SAFETY: never return empty
        if not results:
            logger.warning("[SCANNER] pipeline returned 0 — using fallback")
            results = self._build_fallback()

        self._cache = results
        self._cache_ts = now

        logger.info(f"[SCANNER] selected {len(results)} candidates")
        for c in results[:5]:
            logger.info(
                f"  {c['symbol']} score={c['score']:.1f} "
                f"mom={c['momentum']:.4f} vol={c['volume']/1e6:.1f}M "
                f"vola={c['volatility']:.4f} dir={c['direction']}"
            )

        return results

    # ══════════════════════════════════════════════════════════
    # STAGE 1: UNIVERSE SELECTION
    # ══════════════════════════════════════════════════════════

    def _get_universe(self):
        """Fetch liquid symbols + batch tickers. Returns (symbols, tickers_dict)."""
        universe_size = int(getattr(settings, "EM_MAX_RESULTS", self.DEFAULT_UNIVERSE) or self.DEFAULT_UNIVERSE)
        if universe_size < 30:
            universe_size = 100

        symbols = self._exchange.get_top_liquid_symbols(limit=universe_size)

        if not symbols:
            logger.warning("[SCANNER] no liquid symbols — using fallback list")
            symbols = [s for s in self._fallback_symbols
                       if self._exchange.is_symbol_supported(s, "futures")]

        logger.info(f"[SCANNER] scanning {len(symbols)} symbols")

        # Batch fetch — single API call
        tickers = self._exchange.fetch_tickers_batch(symbols, "futures")

        if not tickers:
            logger.warning("[SCANNER] batch ticker fetch failed")
            return symbols, {}

        return symbols, tickers

    def _filter_universe(self, symbols, tickers):
        """Apply hard filters: volume, spread, price validity."""
        min_volume = _safe_float(
            getattr(settings, "EM_MIN_VOLUME_USD", None),
            self.DEFAULT_MIN_VOLUME
        )
        max_spread = _safe_float(
            getattr(settings, "EMERGING_MAX_SPREAD", None),
            self.DEFAULT_MAX_SPREAD
        )

        candidates = []

        for symbol in symbols:
            ticker = tickers.get(symbol)
            if not ticker:
                continue

            price = _safe_float(ticker.get("last"))
            volume = _safe_float(ticker.get("volume"))
            bid = _safe_float(ticker.get("bid"))
            ask = _safe_float(ticker.get("ask"))

            # Hard filters
            if price <= 0:
                continue
            if volume < min_volume:
                continue

            # Spread check
            spread = 0.0
            if bid > 0 and ask > 0:
                spread = (ask - bid) / ((ask + bid) / 2)
                if spread > max_spread:
                    continue

            candidates.append({
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "bid": bid,
                "ask": ask,
                "spread": spread,
            })

        logger.info(f"[SCANNER] {len(candidates)} passed volume/spread filter (min_vol={min_volume/1e6:.1f}M)")

        return candidates

    # ══════════════════════════════════════════════════════════
    # STAGE 2: FEATURE ENGINEERING
    # ══════════════════════════════════════════════════════════

    def _compute_features(self, candidates):
        """
        Enrich top candidates with momentum + volatility from OHLCV.
        Limits OHLCV calls to top N by volume to stay within rate limits.
        """
        # Sort by volume descending — OHLCV only for top candidates
        candidates.sort(key=lambda x: x["volume"], reverse=True)
        ohlcv_limit = min(self.DEFAULT_OHLCV_LIMIT, len(candidates))

        enriched = []

        for i, c in enumerate(candidates):
            symbol = c["symbol"]

            momentum = 0.0
            volatility = 0.0
            volume_surge = 1.0

            # Fetch OHLCV only for top candidates (rate limit protection)
            if i < ohlcv_limit:
                try:
                    ohlcv = self._exchange.fetch_ohlcv(symbol, "1m", 20, "futures")
                    if ohlcv and len(ohlcv) >= 5:
                        closes = [_safe_float(bar.get("close") if isinstance(bar, dict) else bar[4])
                                  for bar in ohlcv]
                        closes = [c for c in closes if c > 0]

                        if len(closes) >= 5:
                            # Momentum: price change over window
                            momentum = (closes[-1] - closes[0]) / closes[0]

                            # Volatility: coefficient of variation
                            mean_price = statistics.mean(closes)
                            if mean_price > 0:
                                volatility = statistics.stdev(closes) / mean_price

                            # Volume surge: last 5 bars vs first 15 bars
                            volumes = [_safe_float(bar.get("volume") if isinstance(bar, dict) else bar[5])
                                       for bar in ohlcv]
                            if len(volumes) >= 10:
                                recent_vol = sum(volumes[-5:])
                                older_vol = sum(volumes[:-5])
                                if older_vol > 0:
                                    volume_surge = recent_vol / (older_vol / max(1, len(volumes) - 5) * 5)

                except Exception as e:
                    logger.debug(f"[SCANNER] OHLCV failed {symbol}: {e}")

            # Compute 24h change proxy from ticker (bid/ask midpoint vs price)
            change_24h = 0.0
            # Use momentum as primary signal since we have actual candle data
            change_24h = abs(momentum) * 100  # convert to percentage

            c.update({
                "momentum": momentum,
                "volatility": volatility,
                "volume_surge": volume_surge,
                "change": change_24h,
                "direction": "long" if momentum > 0 else "short",
            })

            enriched.append(c)

        return enriched

    # ══════════════════════════════════════════════════════════
    # STAGE 3: SCORING ENGINE
    # ══════════════════════════════════════════════════════════

    def _score_candidates(self, candidates):
        """
        Multi-factor normalized scoring.
        Normalizes each factor to 0-1, then applies weights.
        """
        if not candidates:
            return []

        # Extract raw values for normalization
        momentums = [abs(c["momentum"]) for c in candidates]
        volumes = [c["volume"] for c in candidates]
        volatilities = [c["volatility"] for c in candidates]
        changes = [c["change"] for c in candidates]

        # Min-max normalization helpers
        def normalize(val, values):
            min_v = min(values)
            max_v = max(values)
            if max_v == min_v:
                return 0.5
            return (val - min_v) / (max_v - min_v)

        for c in candidates:
            mom_norm = normalize(abs(c["momentum"]), momentums)
            vol_norm = normalize(c["volume"], volumes)
            vola_norm = normalize(c["volatility"], volatilities)
            chg_norm = normalize(c["change"], changes)

            base_score = (
                mom_norm * self.W_MOMENTUM +
                vol_norm * self.W_VOLUME +
                vola_norm * self.W_VOLATILITY +
                chg_norm * self.W_CHANGE
            )

            # Bonus: volume surge (recent volume spike)
            surge = c.get("volume_surge", 1.0)
            if surge > 2.0:
                base_score += 10  # strong volume spike bonus
            elif surge > 1.5:
                base_score += 5

            # Bonus: strong directional momentum
            if abs(c["momentum"]) > 0.005:  # > 0.5% in 20 candles
                base_score += 5

            # Penalty: very low volatility (dead market)
            if c["volatility"] < 0.0005:
                base_score -= 10

            c["score"] = round(max(0, base_score), 2)

        # Sort descending
        candidates.sort(key=lambda x: x["score"], reverse=True)

        return candidates

    # ══════════════════════════════════════════════════════════
    # PIPELINE
    # ══════════════════════════════════════════════════════════

    def _run_pipeline(self):
        """Execute the full 3-stage pipeline."""
        t0 = time.time()

        # Stage 1: Universe
        symbols, tickers = self._get_universe()
        if not tickers:
            return []

        candidates = self._filter_universe(symbols, tickers)
        if not candidates:
            logger.warning("[SCANNER] 0 candidates after filter — relaxing volume to 500K")
            # Relax filters and retry
            candidates = self._filter_relaxed(symbols, tickers)

        if not candidates:
            return []

        # Stage 2: Features
        enriched = self._compute_features(candidates)

        # Stage 3: Scoring
        scored = self._score_candidates(enriched)

        # Take top N
        max_results = int(getattr(settings, "EM_MAX_RESULTS", self.DEFAULT_RESULTS) or self.DEFAULT_RESULTS)
        if max_results < 5:
            max_results = self.DEFAULT_RESULTS

        results = scored[:max_results]

        elapsed = time.time() - t0
        logger.info(f"[SCANNER] pipeline complete in {elapsed:.1f}s — {len(results)} results")

        return results

    def _filter_relaxed(self, symbols, tickers):
        """Relaxed filter for low-activity markets. Volume threshold halved."""
        candidates = []
        for symbol in symbols:
            ticker = tickers.get(symbol)
            if not ticker:
                continue
            price = _safe_float(ticker.get("last"))
            volume = _safe_float(ticker.get("volume"))
            if price <= 0 or volume < 500_000:
                continue
            candidates.append({
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "bid": _safe_float(ticker.get("bid")),
                "ask": _safe_float(ticker.get("ask")),
                "spread": 0.0,
            })
        return candidates

    # ══════════════════════════════════════════════════════════
    # FALLBACK — guaranteed non-empty
    # ══════════════════════════════════════════════════════════

    def _build_fallback(self):
        """
        Return hardcoded high-liquidity symbols with minimal scoring.
        Called ONLY when everything else fails.
        """
        results = []
        for symbol in self._fallback_symbols:
            results.append({
                "symbol": symbol,
                "score": 10.0,
                "momentum": 0.0,
                "volume": 0,
                "volatility": 0.0,
                "change": 0,
                "direction": "long",
                "volume_surge": 1.0,
                "price": 0.0,
                "spread": 0.0,
                "bid": 0.0,
                "ask": 0.0,
            })
        logger.warning(f"[SCANNER] fallback: {len(results)} hardcoded symbols")
        return results
