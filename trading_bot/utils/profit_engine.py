class ProfitEngine:

    def __init__(self):
        pass

    def update_trade(self, trade, price):

        entry = trade.get("entry")
        side = trade.get("side")

        if not entry or entry <= 0:
            return None

        pnl_pct = ((price - entry) / entry) * 100

        if side == "sell":
            pnl_pct *= -1

        # ==================================================
        # 🔥 BREAK EVEN EARLY (FAST PROTECTION)
        # ==================================================

        if pnl_pct > 0.8 and not trade.get("be_done"):

            trade["stop_loss"] = entry
            trade["be_done"] = True

        # ==================================================
        # 🔥 TRAILING SMART (DYNAMIC)
        # ==================================================

        if pnl_pct > 1.5:

            if side == "buy":
                new_sl = price * (0.997 - (pnl_pct / 200))
                trade["stop_loss"] = max(trade.get("stop_loss", 0), new_sl)

            else:
                new_sl = price * (1.003 + (pnl_pct / 200))
                trade["stop_loss"] = min(trade.get("stop_loss", price), new_sl)

        # ==================================================
        # 🔥 PROFIT LOCK LEVELS (SCALING)
        # ==================================================

        # TP1 — riduzione rischio
        if pnl_pct > 1.5 and not trade.get("tp1_done"):

            trade["tp1_done"] = True
            return "partial_close_30"

        # TP2 — monetizzazione
        if pnl_pct > 3 and not trade.get("tp2_done"):

            trade["tp2_done"] = True
            return "partial_close_50"

        # TP3 — trend exploitation
        if pnl_pct > 5 and not trade.get("tp3_done"):

            trade["tp3_done"] = True
            return "partial_close_80"

        # ==================================================
        # 🔥 HARD EXIT (ANTI REVERSAL)
        # ==================================================

        if pnl_pct < -1.2:
            return "force_close"

        # ==================================================
        # 🔥 TIME EXIT (DEAD TRADE)
        # ==================================================

        created = trade.get("created_at", 0)

        if created:

            import time
            age = time.time() - created

            # 15 min senza movimento → chiudi
            if age > 900 and abs(pnl_pct) < 0.3:
                return "force_close"

        return None