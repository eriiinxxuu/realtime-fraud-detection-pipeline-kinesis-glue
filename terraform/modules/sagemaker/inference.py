import os
import pickle
import numpy as np
import pandas as pd
from io import StringIO

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
    """
    SageMaker calls this once at endpoint startup.
    Loads model.pkl from /opt/ml/model/ (extracted from model.tar.gz).
    """
    model_path = os.path.join(model_dir, "model.pkl")
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    return model


def input_fn(request_body, content_type):
    """
    Deserializes the incoming request.
    Lambda sends CSV with header row.
    """
    if content_type == "text/csv":
        df = pd.read_csv(StringIO(request_body))
        return df[FEATURE_COLUMNS]
    raise ValueError(f"Unsupported content type: {content_type}")


def predict_fn(input_data, model):
    """
    Runs model.predict_proba on the input DataFrame.
    Returns fraud probability (positive class).
    """
    proba = model.predict_proba(input_data)[:, 1]
    return proba


def output_fn(prediction, accept):
    """
    Serializes the prediction back to Lambda.
    Returns one score per line as plain text.
    """
    return "\n".join(str(round(float(p), 6)) for p in prediction)