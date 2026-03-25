"""
streaming_job.py
────────────────
Glue Streaming Job (Glue 4.0)

Flow:
  Kinesis Data Streams (transactions)
      → Glue native connector reads raw JSON
      → manually parse JSON with from_json
      → feature engineering (26 features, same as training)
      → write parquet to S3 raw-transactions/features/
          partitioned by year / month / day
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
    BooleanType, ArrayType
)

# ── Job args ──────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'KINESIS_STREAM_NAME',
    'AWS_REGION',
    'S3_OUTPUT_PATH',
    'WINDOW_SIZE',
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

KINESIS_STREAM_NAME = args['KINESIS_STREAM_NAME']
AWS_REGION          = args['AWS_REGION']
S3_OUTPUT_PATH      = args['S3_OUTPUT_PATH']
WINDOW_SIZE         = args['WINDOW_SIZE']

# ── Transaction schema ────────────────────────────────────────────────────────

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id",        IntegerType()),
    StructField("amount",         DoubleType()),
    StructField("currency",       StringType()),
    StructField("timestamp",      StringType()),
    StructField("merchant",       StringType()),
    StructField("location",       StringType()),
    StructField("device_id",      StringType()),
    StructField("mcc",            StringType()),
    StructField("is_fraud",       IntegerType()),
    StructField("fraud_type",     StringType()),
    StructField("risk_signals",   ArrayType(StringType())),
    StructField("risk_score",     DoubleType()),
    StructField("user_profile_summary", StructType([
        StructField("account_age_days",     IntegerType()),
        StructField("is_frequent_traveler", BooleanType()),
        StructField("avg_transaction",      DoubleType()),
        StructField("home_country",         StringType()),
    ])),
])

RISK_SIGNALS = [
    "amount_anomaly_3x",
    "amount_anomaly_5x",
    "geo_anomaly",
    "high_risk_merchant",
    "velocity_10min_high",
    "velocity_1h_high",
    "device_change",
    "night_transaction",
    "new_account",
    "high_risk_mcc",
]

COLS_TO_DROP = [
    "is_fraud", "fraud_type", "fraud_details",
    "risk_signals", "user_profile_summary",
    "risk_score",
    "home_country",
    "event_time",
    "json_str", "txn",
]

# ── Feature engineering ───────────────────────────────────────────────────────

def add_features(df):
    # Flatten user_profile_summary
    df = (df
        .withColumn("account_age_days",
                    F.col("user_profile_summary.account_age_days"))
        .withColumn("is_frequent_traveler",
                    F.col("user_profile_summary.is_frequent_traveler").cast(IntegerType()))
        .withColumn("avg_transaction",
                    F.col("user_profile_summary.avg_transaction"))
        .withColumn("home_country",
                    F.col("user_profile_summary.home_country"))
    )

    # Expand risk_signals to signal_* binary columns
    for signal in RISK_SIGNALS:
        df = df.withColumn(
            f"signal_{signal}",
            F.array_contains(F.col("risk_signals"), F.lit(signal)).cast(IntegerType())
        )

    # Time features
    df = (df
        .withColumn("event_time",  F.to_timestamp(F.col("timestamp")))
        .withColumn("hour",        F.hour(F.col("event_time")))
        .withColumn("day_of_week", F.dayofweek(F.col("event_time")))
        .withColumn("is_weekend",
                    F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
        .withColumn("is_night",
                    F.when(
                        (F.col("hour") >= 21) | (F.col("hour") < 5), 1
                    ).otherwise(0))
    )

    # Amount / geo features
    df = (df
        .withColumn("amount_ratio",
                    F.col("amount") / F.when(
                        F.col("avg_transaction") == 0, F.lit(1.0)
                    ).otherwise(F.col("avg_transaction")))
        .withColumn("is_home_country",
                    (F.col("location") == F.col("home_country")).cast(IntegerType()))
        .withColumn("is_small_amount",
                    (F.col("amount") < 5).cast(IntegerType()))
        .withColumn("is_round_amount",
                    (F.floor(F.col("amount")) == F.col("amount")).cast(IntegerType()))
    )

    # Partition columns
    df = (df
        .withColumn("year",  F.year(F.col("event_time")))
        .withColumn("month", F.month(F.col("event_time")))
        .withColumn("day",   F.dayofmonth(F.col("event_time")))
    )

    return df

# ── Batch processing function ─────────────────────────────────────────────────

def process_batch(data_frame, batch_id):
    if data_frame.count() == 0:
        return

    # Glue native connector returns a single column with raw JSON
    # Column name contains special characters, use columns[0] to get it
    raw_col = data_frame.columns[0]

    # Parse JSON manually
    parsed = (data_frame
        .withColumn("json_str", F.col(f"`{raw_col}`").cast(StringType()))
        .withColumn("txn",      F.from_json(F.col("json_str"), TRANSACTION_SCHEMA))
        .select("txn.*")
        .filter(F.col("transaction_id").isNotNull())
    )

    # Feature engineering
    featured = add_features(parsed)

    # Drop columns not needed for inference
    cols_to_drop = [c for c in COLS_TO_DROP if c in featured.columns]
    featured_clean = featured.drop(*cols_to_drop)

    # Write to S3 partitioned by year/month/day
    (featured_clean.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(S3_OUTPUT_PATH)
    )

# ── Read from Kinesis using Glue native connector ─────────────────────────────

kinesis_stream = glueContext.create_data_frame_from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData":       "kinesis",
        "streamARN":        f"arn:aws:kinesis:{AWS_REGION}:402705369995:stream/{KINESIS_STREAM_NAME}",
        "classification":   "json",
        "startingPosition": "LATEST",
        "inferSchema":      "false",
    },
    transformation_ctx="kinesis_stream"
)

# ── Start streaming with Glue forEachBatch ────────────────────────────────────

glueContext.forEachBatch(
    frame=kinesis_stream,
    batch_function=process_batch,
    options={
        "windowSize":         WINDOW_SIZE,
        "checkpointLocation": f"{S3_OUTPUT_PATH}_checkpoint/",
    }
)

job.commit()