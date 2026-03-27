import os
import json
import boto3
import logging
import pandas as pd
import pyarrow.parquet as pq
import io
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.parse
from urllib.parse import unquote_plus


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client        = boto3.client("s3")
sagemaker_client = boto3.client("sagemaker-runtime")
sns_client       = boto3.client("sns")

SAGEMAKER_ENDPOINT = os.environ["SAGEMAKER_ENDPOINT_NAME"]
SNS_TOPIC_ARN      = os.environ["SNS_TOPIC_ARN"]
PREDICTIONS_BUCKET = os.environ["S3_PREDICTIONS_BUCKET"]
FRAUD_THRESHOLD    = float(os.environ.get("FRAUD_THRESHOLD", "0.86"))
BATCH_SIZE         = int(os.environ.get("BATCH_SIZE", "500"))


FEATURE_COLUMNS = [
    "amount", "currency", "merchant", "location", "device_id", "mcc",
    "account_age_days", "is_frequent_traveler", "avg_transaction",
    "signal_amount_anomaly_3x", "signal_amount_anomaly_5x",
    "signal_geo_anomaly", "signal_high_risk_merchant",
    "signal_velocity_10min_high", "signal_velocity_1h_high",
    "signal_device_change", "signal_night_transaction",
    "signal_new_account", "signal_high_risk_mcc",
    "hour", "day_of_week", "is_weekend", "is_night",
    "amount_ratio", "is_home_country", "is_small_amount", "is_round_amount"
]


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pq.read_table(io.BytesIO(response["Body"].read())).to_pandas()


def invoke_sagemaker_batch(df_batch: pd.DataFrame) -> list[float]:
    df_inference = df_batch[FEATURE_COLUMNS].fillna(0)
    payload = df_inference.to_json(orient='records')

    try:
        response = sagemaker_client.invoke_endpoint(
            EndpointName=SAGEMAKER_ENDPOINT,
            ContentType="application/json",
            Body=payload
    )
        res_body = json.loads(response["Body"].read().decode("utf-8"))
        return res_body.get("predictions", res_body)
    except Exception as e:
        logger.error("Invoke failed: %s", str(e))
        raise


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
    logger.info("Alert sent: %s score=%.4f", row.get("transaction_id"), fraud_score)


def write_predictions_to_s3(df: pd.DataFrame, source_key: str) -> None:
   
    output_key = source_key.replace("features/", "predictions/", 1)
    if output_key == source_key:
        
        output_key = f"predictions/{os.path.basename(source_key)}"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3_client.put_object(
        Bucket=PREDICTIONS_BUCKET,
        Key=output_key,
        Body=buffer.getvalue(),
    )
    logger.info("Written to s3://%s/%s", PREDICTIONS_BUCKET, output_key)


def handler(event, context):
    try:
        raw_key = event['Records'][0]['s3']['object']['key']
        bucket = event['Records'][0]['s3']['bucket']['name']
        
        
        key = urllib.parse.unquote_plus(raw_key)
        
        logger.info(f"Raw Key: {raw_key}")
        logger.info(f"Decoded Key: {key}")

       
        if not key.endswith('.parquet'):
            logger.info(f"Skipping non-parquet object: {key}")
            return {"statusCode": 200, "body": "Skipped"}



        df = read_parquet_from_s3(bucket, key)
        logger.info("Loaded %d rows", len(df))

        
        missing = set(FEATURE_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing feature columns in parquet: {missing}")
        


       
        all_scores: list[float] = []
        total_batches = int(np.ceil(len(df) / BATCH_SIZE))

        for i in range(0, len(df), BATCH_SIZE):
            scores = invoke_sagemaker_batch(df.iloc[i : i + BATCH_SIZE])
            all_scores.extend(scores)
            logger.info("Batch %d/%d done", i // BATCH_SIZE + 1, total_batches)

        
        if len(all_scores) != len(df):
            raise ValueError(f"Total score mismatch: {len(all_scores)} != {len(df)}")

        df["fraud_score"] = all_scores
        df["fraud_pred"]  = (df["fraud_score"] >= FRAUD_THRESHOLD).astype(int)

        fraud_rows = df[df["fraud_pred"] == 1]
        logger.info("Fraud: %d / %d", len(fraud_rows), len(df))

       
        if not fraud_rows.empty:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(publish_fraud_alert, row.to_dict(), row["fraud_score"]): idx
                    for idx, row in fraud_rows.iterrows()
                }
                for future in as_completed(futures):
                    if future.exception():
                        logger.error("SNS publish failed: %s", future.exception())

        write_predictions_to_s3(df, key)
    except Exception as e:
        logger.error(f"Error processing S3 object: {str(e)}")
        raise e

    return {"statusCode": 200, "body": "OK"}