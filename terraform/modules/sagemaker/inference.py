import os
import pickle
import numpy as np
import pandas as pd
from io import StringIO
import json
import logging

logger = logging.getLogger(__name__)

# Lambda invoke_endpoint()
#     ↓
# input_fn(request_body, content_type)   # bytes → DataFrame
#     ↓
# predict_fn(input_data, model)          # DataFrame → predictions
#     ↓
# output_fn(prediction, accept)          # predictions → bytes
#     ↓
# return to Lambda
#
# model_fn: only run when triggered - load model from /opt/ml/model/model.pkl

FEATURE_COLUMNS = [
    "currency", "merchant", "location", "mcc", "device_id",
    "amount",
    "account_age_days", "is_frequent_traveler", "avg_transaction",
    "hour", "day_of_week", "is_weekend", "is_night",
    "amount_ratio", "is_home_country", "is_small_amount", "is_round_amount",
    "signal_amount_anomaly_3x", "signal_amount_anomaly_5x",
    "signal_geo_anomaly", "signal_high_risk_merchant",
    "signal_velocity_10min_high", "signal_velocity_1h_high",
    "signal_device_change", "signal_night_transaction",
    "signal_new_account", "signal_high_risk_mcc",
]

def model_fn(model_dir):
    model_path = os.path.join(model_dir, "model.pkl")
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    logger.info("Model loaded from %s", model_path)
    return model


def input_fn(request_body, content_type):
    if content_type == "text/csv":
        df = pd.read_csv(StringIO(request_body))
    elif content_type == "application/json":
        data = json.loads(request_body)
        df = pd.DataFrame(data if isinstance(data, list) else [data])
    else:
        raise ValueError(f"Unsupported content type: {content_type}")

    missing = set(FEATURE_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")

    return df[FEATURE_COLUMNS]


def predict_fn(input_data, model):
    return model.predict_proba(input_data)[:, 1]



def output_fn(prediction, accept):
    
    if accept in ("text/plain", "*/*", ""):
        body = "\n".join(str(round(float(p), 6)) for p in prediction)
        return body, "text/plain"
    if accept == "application/json":
        body = json.dumps({"predictions": [round(float(p), 6) for p in prediction]})
        return body, "application/json"
    raise ValueError(f"Unsupported accept type: {accept}")