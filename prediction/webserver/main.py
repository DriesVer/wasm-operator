from datetime import datetime as dt
from datetime import timedelta, timezone
import signal
import sys

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request
from statsmodels.tsa.api import ExponentialSmoothing, Holt, SimpleExpSmoothing
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.tsa.arima.model import ARIMA

app = Flask(__name__)


def dateToRust(date):
    ## 2023-01-23T13:04:03.182471956Z
    return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def predictAutoReg(diff):
    # if not enough data/lags take  min
    lags = 10
    model = AutoReg(diff, lags=lags).fit()
    predction = model.predict(start=len(diff), end=len(diff), dynamic=False)[0]
    return predction


def predictARIMA(diff):
    model = ARIMA(diff, order=(2, 0, 0)).fit()
    predction = model.predict(start=len(diff), end=len(diff), dynamic=False)[0]
    return predction


def predictSarima(diff):
    pass


def predictSES(diff):
    model = SimpleExpSmoothing(diff, initialization_method="estimated").fit()
    predction = model.predict(start=len(diff), end=len(diff))[0]
    return predction


def predictHolt(diff):
    model = Holt(diff, initialization_method="estimated").fit()
    predction = model.predict(start=len(diff), end=len(diff))[0]
    return predction


def predictWinter(diff):
    model = ExponentialSmoothing(diff, initialization_method="estimated").fit()
    predction = model.predict(start=len(diff), end=len(diff))[0]
    return predction


predictionFunctions = {
    "AutoReg": predictAutoReg,
    "ARIMA": predictARIMA,
    "SARIMA": predictSarima,
    "SES": predictSES,
    "Holt": predictHolt,
    "Winter": predictWinter,
}


@app.route("/prediction", methods=["post"])
def predict():
    history = request.json["history"]

    dates = [dt.fromisoformat(date) for date in history]

    ## not enough data just return 3 secs
    if len(dates) == 0 or len(dates) == 1:
        min_dt = dateToRust(dt.min)
        return jsonify({"prediction": min_dt})

    lastEvent = dates[-1]
    diff = [(dates[i] - dates[i - 1]).total_seconds() for i in range(1, len(dates))]
    prediction = 0

    f = predictAutoReg
    if "function" in request.json:
        if request.json["function"] in predictionFunctions:
            f = predictionFunctions[request.json["function"]]
        else:
            print("error function does not exist", flush=True)
            return jsonify({"error": "function does not exist"})

    try:
        prediction = f(diff)

    except Exception as e:
        print("error in model, taking  mean, error is ", e)

        prediction = np.mean(diff)

    now = lastEvent
    now += timedelta(seconds=prediction)
    now = dateToRust(now)
    # print(f"prediction is  {prediction} with {diff}",flush=True)
    print(f"{history,now}", flush=True)
    # print(f"diff {diff}",flush=True)

    return jsonify({"prediction": now})


def graceful_shutdown(signum, _):
    signal_name = signal.Signals(signum).name
    print(
        f"Received signal '{signal_name}', initiating graceful shutdown...",
        flush=True,
    )
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    app.run(host="0.0.0.0", port=5000, threaded=True)
