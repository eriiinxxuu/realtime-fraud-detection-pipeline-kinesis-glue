"""
streaming_job.py
────────────────
Glue Streaming Job (Glue 4.0 / Spark 3.3)

Flow:
  Kinesis Data Streams (transactions)
      → parse JSON
      → feature engineering (26 features, same as training)
      → write parquet to S3 raw-transactions/features/
          partitioned by year / month / day

CloudWatch metric --enable-metrics exposes:
  glue.driver.aggregate.recordsRead  ← used by cloudwatch module alarm
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
    BooleanType, ArrayType, TimestampType
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
S3_OUTPUT_PATH      = args['S3_OUTPUT_PATH']   # s3://bucket/features/
WINDOW_SIZE         = args['WINDOW_SIZE']       # "30 seconds"

# ── Transaction schema (matches producer output) ──────────────────────────────

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

# ── Risk signals list (same order as training) ────────────────────────────────

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

HIGH_RISK_MCC = ["6211", "5962", "7995", "7801"]


def add_features(df):
    """
    Feature engineering — identical to training pipeline.
    Input:  raw parsed DataFrame
    Output: DataFrame with 26 feature columns + metadata
    """

    # ── Flatten user_profile_summary ─────────────────────────────
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

    # ── Expand risk_signals array into binary columns ─────────────
    for signal in RISK_SIGNALS:
        df = df.withColumn(
            f"signal_{signal}",
            F.array_contains(F.col("risk_signals"), F.lit(signal)).cast(IntegerType())
        )

    # ── Time features ─────────────────────────────────────────────
    df = (df
        .withColumn("event_time",  F.to_timestamp(F.col("timestamp")))
        .withColumn("hour",        F.hour(F.col("event_time")))
        .withColumn("day_of_week", F.dayofweek(F.col("event_time")))
        .withColumn("is_weekend",
                    F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
        .withColumn("is_night",
                    F.when(F.col("hour").between(2, 5), 1).otherwise(0))
    )

    # ── Amount features ───────────────────────────────────────────
    df = (df
        .withColumn("amount_ratio",
                    F.col("amount") / F.when(F.col("avg_transaction") == 0, F.lit(1.0))
                                       .otherwise(F.col("avg_transaction")))
        .withColumn("is_home_country",
                    (F.col("location") == F.col("home_country")).cast(IntegerType()))
        .withColumn("is_small_amount",
                    (F.col("amount") < 5).cast(IntegerType()))
        .withColumn("is_round_amount",
                    (F.col("amount") % 10 == 0).cast(IntegerType()))
    )

    # ── Partition columns ─────────────────────────────────────────
    df = (df
        .withColumn("year",  F.year(F.col("event_time")))
        .withColumn("month", F.month(F.col("event_time")))
        .withColumn("day",   F.dayofmonth(F.col("event_time")))
    )

    return df


def process_batch(micro_batch_df, batch_id):
    """
    Called by Spark for each micro-batch.
    Parses raw Kinesis bytes → feature engineering → write parquet.
    """
    if micro_batch_df.isEmpty():
        return

    # Parse JSON from Kinesis `data` column (base64-decoded by Spark)
    parsed = (micro_batch_df
        .withColumn("json_str", F.col("data").cast(StringType()))
        .withColumn("txn",      F.from_json(F.col("json_str"), TRANSACTION_SCHEMA))
        .select("txn.*")
        .filter(F.col("transaction_id").isNotNull())
    )

    # Feature engineering
    featured = add_features(parsed)

    # Write to S3 partitioned by year/month/day
    (featured.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(S3_OUTPUT_PATH)
    )


# ── Read from Kinesis ─────────────────────────────────────────────────────────

kinesis_stream = (spark.readStream
    .format("kinesis")
    .option("streamName",        KINESIS_STREAM_NAME)
    .option("startingPosition",  "LATEST")
    .option("region",            AWS_REGION)
    .option("awsStsRoleArn",     "")           # uses task role automatically
    .load()
)

# ── Start streaming query ─────────────────────────────────────────────────────

query = (kinesis_stream
    .writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime=WINDOW_SIZE)
    .option("checkpointLocation", f"{S3_OUTPUT_PATH}_checkpoint/")
    .start()
)

query.awaitTermination()
job.commit()