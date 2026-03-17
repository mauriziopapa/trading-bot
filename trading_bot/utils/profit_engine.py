class ProfitEngine:

    def __init__(self):
        pass

    def update_trade(self, trade, price):

        entry = trade["entry"]
        side = trade["side"]

        pnl_pct = ((price - entry) / entry) * 100

        if side == "sell":
            pnl_pct *= -1

        # --------------------------------------------------
        # BREAK EVEN
        # --------------------------------------------------

        if pnl_pct > 1.5:
            trade["stop_loss"] = entry

        # --------------------------------------------------
        # TRAILING STOP
        # --------------------------------------------------

        if pnl_pct > 2:

            if side == "buy":
                new_sl = price * 0.985
                trade["stop_loss"] = max(trade["stop_loss"], new_sl)

            else:
                new_sl = price * 1.015
                trade["stop_loss"] = min(trade["stop_loss"], new_sl)

        # --------------------------------------------------
        # PARTIAL TAKE PROFIT FLAG
        # --------------------------------------------------

        if pnl_pct > 3 and not trade.get("tp1_done"):

            trade["tp1_done"] = True
            return "partial_close"

        return None