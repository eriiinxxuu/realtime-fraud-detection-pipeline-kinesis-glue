import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ── 1. CONFIGURATION & JOB INIT ───────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'KINESIS_STREAM_NAME', 'AWS_REGION', 'S3_OUTPUT_PATH', 'WINDOW_SIZE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

KINESIS_STREAM_NAME = args['KINESIS_STREAM_NAME']
AWS_REGION = args['AWS_REGION']
S3_OUTPUT_PATH = args['S3_OUTPUT_PATH']
WINDOW_SIZE = args['WINDOW_SIZE'] if "second" in args['WINDOW_SIZE'].lower() else f"{args['WINDOW_SIZE']} seconds"

# ── 2. SCHEMA DEFINITION ──────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("timestamp", StringType()),
    StructField("merchant", StringType()),
    StructField("location", StringType()),
    StructField("device_id", StringType()),
    StructField("mcc", StringType()),
    StructField("is_fraud", IntegerType()),
    StructField("fraud_type", StringType()),
    StructField("risk_signals", ArrayType(StringType())),
    StructField("risk_score", DoubleType()),
    StructField("user_profile_summary", StructType([
        StructField("account_age_days", IntegerType()),
        StructField("is_frequent_traveler", BooleanType()),
        StructField("avg_transaction", DoubleType()),
        StructField("home_country", StringType()),
    ])),
])

RISK_SIGNALS = [
    "amount_anomaly_3x", "amount_anomaly_5x", "geo_anomaly",
    "high_risk_merchant", "velocity_10min_high", "velocity_1h_high",
    "device_change", "night_transaction", "new_account", "high_risk_mcc"
]

# Ensure we DO NOT drop the new features required by Lambda/SageMaker
COLS_TO_DROP = [
    "risk_signals", "user_profile_summary", "event_time", 
    "json_str", "txn", "data", "partitionKey"
]

# ── 3. TRANSFORMATION LOGIC (FEATURE ENGINEERING) ─────────────────────────────

def add_features(df):
    # A. Flatten nested user profile
    df = (df
        .withColumn("account_age_days", F.col("user_profile_summary.account_age_days"))
        .withColumn("is_frequent_traveler", F.col("user_profile_summary.is_frequent_traveler").cast("int"))
        .withColumn("avg_transaction", F.col("user_profile_summary.avg_transaction"))
        .withColumn("home_country", F.col("user_profile_summary.home_country"))
    )

    # B. Expand existing risk_signals into binary features
    for signal in RISK_SIGNALS:
        df = df.withColumn(f"signal_{signal}", F.array_contains(F.col("risk_signals"), signal).cast("int"))

    # C. Create new features based on domain knowledge
    df = (df
        # 1. is_round_amount: Fraudsters often use round numbers like 100, 500
        .withColumn("is_round_amount", F.when(F.col("amount") % 10 == 0, 1).otherwise(0))
        
        # 2. is_home_country: Check if transaction location matches user's home country
        .withColumn("is_home_country", F.when(F.col("location").contains(F.col("home_country")), 1).otherwise(0))
        
        # 3. is_small_amount: Carding attacks often start with tiny "test" amounts
        .withColumn("is_small_amount", F.when(F.col("amount") < 5.0, 1).otherwise(0))
    )

    # D. Time features and business metrics
    df = (df
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .withColumn("hour", F.hour(F.col("event_time")))
        .withColumn("day_of_week", F.dayofweek(F.col("event_time")))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
        .withColumn("is_night", F.when((F.col("hour") >= 21) | (F.col("hour") < 5), 1).otherwise(0))
        .withColumn("amount_ratio", F.col("amount") / F.when(F.col("avg_transaction") == 0, 1.0).otherwise(F.col("avg_transaction")))
        .withColumn("year", F.year(F.col("event_time")))
        .withColumn("month", F.month(F.col("event_time")))
        .withColumn("day", F.dayofmonth(F.col("event_time")))
    )
    return df

# ── 4. ADAPTIVE BATCH PROCESSING ──────────────────────────────────────────────
def process_batch(data_frame, batch_id):
    logger = glueContext.get_logger()
    count = data_frame.count()
    if count == 0: return

    actual_columns = data_frame.columns
    logger.info(f"--- Processing Batch {batch_id} | Rows: {count} ---")

    # Adaptive Parsing Logic
    if "transaction_id" in actual_columns:
        parsed = data_frame
    elif "data" in actual_columns:
        parsed = (data_frame
            .withColumn("json_str", F.col("data").cast("string"))
            .withColumn("txn", F.from_json(F.col("json_str"), TRANSACTION_SCHEMA))
            .select("txn.*")
        )
    else:
        first_col = actual_columns[0]
        parsed = (data_frame
            .withColumn("json_str", F.col(f"`{first_col}`").cast("string"))
            .withColumn("txn", F.from_json(F.col("json_str"), TRANSACTION_SCHEMA))
            .select("txn.*")
        )

    parsed_valid = parsed.filter(F.col("transaction_id").isNotNull())
    
    if parsed_valid.count() == 0:
        logger.warn(f"Batch {batch_id} dropped: No valid transactions found.")
        return

    # Add features and write
    featured = add_features(parsed_valid)
    final_output = featured.drop(*[c for c in COLS_TO_DROP if c in featured.columns])

    (final_output.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(S3_OUTPUT_PATH)
    )
    logger.info(f"--- Successfully wrote Batch {batch_id} to S3 ---")

# ── 5. DATA SOURCE & EXECUTION ────────────────────────────────────────────────
kinesis_source = glueContext.create_data_frame_from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": f"arn:aws:kinesis:{AWS_REGION}:402705369995:stream/{KINESIS_STREAM_NAME}",
        "classification": "json",
        "startingPosition": "TRIM_HORIZON",
        "inferSchema": "true",
    },
    transformation_ctx="kinesis_source"
)

glueContext.forEachBatch(
    frame=kinesis_source,
    batch_function=process_batch,
    options={
        "windowSize": WINDOW_SIZE,
        "checkpointLocation": f"{S3_OUTPUT_PATH}_checkpoint/",
    }
)

job.commit()