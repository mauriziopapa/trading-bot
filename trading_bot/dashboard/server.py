
from fastapi import FastAPI
import json, os

app=FastAPI()

STATE="dashboard_state.json"

@app.get("/")
def root():
    if not os.path.exists(STATE):
        return {"status":"starting"}
    with open(STATE) as f:
        return json.load(f)
