from flask import Flask, request, abort
from execution.hourly_runner import run_hourly
import os

app = Flask(__name__)

@app.route("/health")
def health():
    return {"status": "alive"}, 200

@app.route("/run")
def run():
    if request.args.get("key") != os.getenv("RUN_KEY", "local"):
        abort(403)

    run_hourly()
    return {"status": "executed"}, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
