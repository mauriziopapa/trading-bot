
import json, time

FILE="dashboard_state.json"

def write_state(bot):
    data={"timestamp2":time.time(),
        "timestamp":time.time(),
        "signals":getattr(bot,"_recent_signals",[]),
        "logs":getattr(bot,"_recent_logs",[])
    }
    with open(FILE,"w") as f:
        json.dump(data,f)
