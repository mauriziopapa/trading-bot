"""
Shared Bot Reference — Accessibile sia dal main che dal server.
═══════════════════════════════════════════════════════════════
Questo modulo viene importato sia da main.py che da server.py.
Se sono nello stesso processo, condividono l'istanza.
Se sono in processi separati, il server esegue direttamente
via exchange/ccxt senza passare dal bot.
"""

import os
from loguru import logger

# ── Bot reference (settato da main.py) ────────────────────────────────────
_bot = None

def set_bot(bot):
    global _bot
    _bot = bot

def get_bot():
    return _bot


# ── Fallback: operazioni dirette senza bot ────────────────────────────────
# Se il server è in un processo separato, queste funzioni
# creano un client ccxt temporaneo e operano direttamente.

_fallback_exchange = None

def _get_fallback_exchange():
    """Crea un client ccxt diretto per operazioni senza bot."""
    global _fallback_exchange
    if _fallback_exchange is not None:
        return _fallback_exchange
    try:
        import ccxt
        from trading_bot.config import settings
        _fallback_exchange = ccxt.bitget({
            "apiKey": settings.BITGET_API_KEY,
            "secret": settings.BITGET_API_SECRET,
            "password": settings.BITGET_API_PASSPHRASE,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        })
        _fallback_exchange.load_markets()
        logger.info("[SHARED] Fallback exchange creato (processo separato)")
        return _fallback_exchange
    except Exception as e:
        logger.error(f"[SHARED] Fallback exchange fallito: {e}")
        return None


def do_rebalance(keep_spot: float = 5.0) -> dict:
    """
    Rebalance spot→futures. Funziona sia con bot che senza.
    """
    result = {"transferred": 0, "spot_before": 0, "spot_after": 0, "futures_after": 0}
    
    # Prova via bot (stesso processo)
    bot = get_bot()
    if bot:
        try:
            return bot.exchange.auto_rebalance(keep_spot_usdt=keep_spot)
        except Exception as e:
            logger.warning(f"[REBALANCE] Via bot fallito: {e}")

    # Fallback: opera direttamente via ccxt
    try:
        from trading_bot.config import settings
        ex = _get_fallback_exchange()
        if not ex:
            return {"transferred": 0, "error": "Exchange non disponibile"}

        bal = ex.fetch_balance()
        spot_free = float(bal.get("USDT", {}).get("free", 0))
        result["spot_before"] = round(spot_free, 2)
        
        available = spot_free - keep_spot
        if available < 2:
            return result

        amount = round(available, 2)
        
        # Transfer via ccxt
        try:
            ex.transfer("USDT", amount, "spot", "swap")
            result["transferred"] = amount
            logger.info(f"[REBALANCE DIRECT] ✅ {amount:.2f} USDT spot→futures")
        except Exception as e1:
            # Fallback Bitget v2 API
            try:
                import requests
                headers = {"Content-Type": "application/json"}
                # Usa l'API di ccxt per firmare la richiesta
                ex.private_post_spot_wallet_transfer({
                    "coin": "USDT", "fromType": "spot",
                    "toType": "usdt_mix", "amount": str(amount),
                })
                result["transferred"] = amount
                logger.info(f"[REBALANCE DIRECT] ✅ {amount:.2f} USDT (v2 API)")
            except Exception as e2:
                logger.warning(f"[REBALANCE DIRECT] ❌ {e1} / {e2}")
        
        result["spot_after"] = round(spot_free - result["transferred"], 2)
        
    except Exception as e:
        logger.error(f"[REBALANCE DIRECT] Errore: {e}")
        result["error"] = str(e)

    return result


def do_sync() -> dict:
    """
    Sync con Bitget. Funziona sia con bot che senza.
    """
    bot = get_bot()
    if bot:
        return {"ok": True, "has_bot": True}
    
    # Senza bot: ritorna solo balance
    try:
        ex = _get_fallback_exchange()
        if not ex:
            return {"ok": False, "error": "Exchange non disponibile"}
        
        bal = ex.fetch_balance()
        spot = float(bal.get("USDT", {}).get("free", 0))
        
        return {
            "ok": True, "has_bot": False,
            "balance": {"spot": round(spot, 2)},
            "note": "Bot in processo separato — sync parziale"
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
