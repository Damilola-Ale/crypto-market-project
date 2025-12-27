from flask import Flask
from execution.hourly_runner import run_hourly

app = Flask(__name__)

@app.route("/health")
def health():
    return {"status": "alive"}, 200

@app.route("/run")
def run():
    run_hourly()
    return {"status": "executed"}, 200
