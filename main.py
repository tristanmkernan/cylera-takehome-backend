from flask import Flask, jsonify, request, abort
from flask_cors import CORS

import time

from bandwidths import BANDWIDTHS

app = Flask(__name__)
CORS(app)


@app.route("/bandwidth")
def bandwidth():
    device_uuid = request.args.get("device_uuid", default=None)

    # Device UUID is a required parameter
    if device_uuid is None or device_uuid == "":
        return abort(400)

    # epoch timestamp of the last time we want to return, default now
    # must be in seconds
    end_time = request.args.get("end_time", int(time.time()), type=int)

    # window in seconds, default 60 seconds
    window_time = request.args.get("window_time", 60, type=int)

    # number of windows i.e., data points, to return, default 10
    num_windows = request.args.get("num_windows", 10, type=int)

    # apply the filters to the dataset
    data = [datum for datum in BANDWIDTHS if datum["device_id"] == device_uuid and datum["timestamp"] <= end_time]

    # aggregate the dataset
    aggregate_data = []

    # initialize the buckets in time-descending order
    for i in range(num_windows):
        aggregate_data.append({
            "bytes_ts": 0,
            "bytes_fs": 0,
            "timestamp": end_time - (i * window_time),
        })

    for datum in data:
        bucket = int(end_time - datum["timestamp"]) // window_time

        if bucket < len(aggregate_data):
            aggregate_data[bucket]["bytes_ts"] += datum["bytes_ts"]
            aggregate_data[bucket]["bytes_fs"] += datum["bytes_fs"]

    # reverse the dataset, so that values are returned ascending by time
    aggregate_data = list(reversed(aggregate_data))

    # return the data as JSON
    return jsonify({
        "data": aggregate_data
    })
