import os
import json
import boto3
import logging
import pandas as pd
import pyarrow.parquet as pq
import io
import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client        = boto3.client("s3")
sagemaker_client = boto3.client("sagemaker-runtime")
sns_client       = boto3.client("sns")

SAGEMAKER_ENDPOINT = os.environ["SAGEMAKER_ENDPOINT_NAME"]
SNS_TOPIC_ARN      = os.environ["SNS_TOPIC_ARN"]
PREDICTIONS_BUCKET = os.environ["S3_PREDICTIONS_BUCKET"]
FRAUD_THRESHOLD    = float(os.environ.get("FRAUD_THRESHOLD", "0.86"))
BATCH_SIZE         = int(os.environ.get("BATCH_SIZE", "1000"))

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


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    return pq.read_table(buffer).to_pandas()


def invoke_sagemaker_batch(df_batch: pd.DataFrame) -> list:
    payload = df_batch[FEATURE_COLUMNS].to_csv(index=False)
    response = sagemaker_client.invoke_endpoint(
        EndpointName=SAGEMAKER_ENDPOINT,
        ContentType="text/csv",
        Body=payload,
    )
    result = response["Body"].read().decode("utf-8")
    scores = [float(x) for x in result.strip().split("\n") if x]
    return scores


def publish_fraud_alert(row: dict, fraud_score: float) -> None:
    message = {
        "transaction_id": row.get("transaction_id"),
        "user_id":        row.get("user_id"),
        "amount":         row.get("amount"),
        "currency":       row.get("currency"),
        "merchant":       row.get("merchant"),
        "fraud_score":    round(fraud_score, 4),
        "threshold":      FRAUD_THRESHOLD,
    }
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Fraud Detected",
        Message=json.dumps(message, default=str),
    )
    logger.info("Fraud alert published: transaction_id=%s score=%.4f",
                row.get("transaction_id"), fraud_score)


def write_predictions_to_s3(df: pd.DataFrame, source_key: str) -> None:
    output_key = source_key.replace("features/", "predictions/")
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(
        Bucket=PREDICTIONS_BUCKET,
        Key=output_key,
        Body=buffer.getvalue(),
    )
    logger.info("Predictions written to s3://%s/%s", PREDICTIONS_BUCKET, output_key)


def handler(event, context):
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]

        logger.info("Processing s3://%s/%s", bucket, key)

        df = read_parquet_from_s3(bucket, key)
        logger.info("Loaded %d rows", len(df))

        all_scores = []

        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i : i + BATCH_SIZE]
            scores = invoke_sagemaker_batch(batch)
            all_scores.extend(scores)
            logger.info("Batch %d/%d scored", i // BATCH_SIZE + 1,
                        int(np.ceil(len(df) / BATCH_SIZE)))

        df["fraud_score"] = all_scores
        df["fraud_pred"]  = (df["fraud_score"] >= FRAUD_THRESHOLD).astype(int)

        fraud_rows = df[df["fraud_pred"] == 1]
        logger.info("Fraud detected: %d / %d rows", len(fraud_rows), len(df))

        for _, row in fraud_rows.iterrows():
            publish_fraud_alert(row.to_dict(), row["fraud_score"])

        write_predictions_to_s3(df, key)

    return {"statusCode": 200, "body": "OK"}
