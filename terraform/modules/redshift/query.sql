-- CREATE EXTERNAL SCHEMA spectrum_fraud
-- FROM DATA CATALOG
-- DATABASE 'fraud_db'
-- IAM_ROLE 'arn:aws:iam::402705369995:role/fraud-detection-kinesis-glue-redshift-role'
-- CREATE EXTERNAL DATABASE IF NOT EXISTS;



-- SELECT * FROM SVV_EXTERNAL_TABLES WHERE schemaname = 'spectrum_fraud';


--marchant

CREATE VIEW fraud_by_merchant AS
SELECT
    merchant,
    COUNT(*)                                                          AS total_txn,
    SUM(fraud_pred)                                                   AS fraud_count,
    ROUND(100.0 * SUM(fraud_pred) / COUNT(*), 2)                     AS fraud_rate_pct,
    ROUND(AVG(fraud_score), 4)                                        AS avg_score,
    ROUND(AVG(CASE WHEN fraud_pred=1 THEN amount END), 2)            AS avg_fraud_amount
FROM spectrum_fraud.predictions
GROUP BY merchant
ORDER BY fraud_rate_pct DESC
WITH NO SCHEMA BINDING;

-- location
CREATE VIEW fraud_by_location AS
SELECT
    location,
    COUNT(*)                                                          AS total_txn,
    SUM(fraud_pred)                                                   AS fraud_count,
    ROUND(100.0 * SUM(fraud_pred) / COUNT(*), 2)                     AS fraud_rate_pct,
    ROUND(AVG(amount), 2)                                             AS avg_amount,
    ROUND(SUM(CASE WHEN fraud_pred=1 THEN amount ELSE 0 END), 2)     AS total_fraud_amount
FROM spectrum_fraud.predictions
GROUP BY location
ORDER BY fraud_count DESC
WITH NO SCHEMA BINDING;

-- MCC
CREATE VIEW fraud_by_mcc AS
SELECT
    mcc,
    COUNT(*)                                                          AS total_txn,
    SUM(fraud_pred)                                                   AS fraud_count,
    ROUND(100.0 * SUM(fraud_pred) / COUNT(*), 2)                     AS fraud_rate_pct,
    ROUND(AVG(amount), 2)                                             AS avg_amount
FROM spectrum_fraud.predictions
GROUP BY mcc
ORDER BY fraud_rate_pct DESC
WITH NO SCHEMA BINDING;

-- model performance
CREATE VIEW daily_model_performance AS
SELECT
    year, month, day,
    COUNT(*)                                                          AS total_predictions,
    SUM(fraud_pred)                                                   AS flagged_fraud,
    ROUND(100.0 * SUM(fraud_pred) / COUNT(*), 2)                     AS fraud_rate_pct,
    ROUND(AVG(fraud_score), 4)                                        AS avg_score,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fraud_score), 4) AS p95_score,
    ROUND(MAX(fraud_score), 4)                                        AS max_score
FROM spectrum_fraud.predictions
GROUP BY year, month, day
ORDER BY year, month, day
WITH NO SCHEMA BINDING;

-- high risk signal analysis
CREATE VIEW fraud_signal_analysis AS
SELECT
    signal_amount_anomaly_3x,
    signal_geo_anomaly,
    signal_device_change,
    signal_high_risk_merchant,
    signal_velocity_1h_high,
    COUNT(*)                                                          AS total_txn,
    SUM(fraud_pred)                                                   AS fraud_count,
    ROUND(100.0 * SUM(fraud_pred) / COUNT(*), 2)                     AS fraud_rate_pct
FROM spectrum_fraud.predictions
GROUP BY
    signal_amount_anomaly_3x,
    signal_geo_anomaly,
    signal_device_change,
    signal_high_risk_merchant,
    signal_velocity_1h_high
HAVING COUNT(*) > 10
ORDER BY fraud_rate_pct DESC
WITH NO SCHEMA BINDING;

-- score bucket
CREATE VIEW fraud_score_distribution AS
SELECT
    CASE
        WHEN fraud_score < 0.2  THEN '0.0-0.2'
        WHEN fraud_score < 0.4  THEN '0.2-0.4'
        WHEN fraud_score < 0.6  THEN '0.4-0.6'
        WHEN fraud_score < 0.8  THEN '0.6-0.8'
        WHEN fraud_score < 0.86 THEN '0.8-0.86'
        ELSE '0.86+'
    END                                                               AS score_bucket,
    COUNT(*)                                                          AS txn_count,
    ROUND(AVG(amount), 2)                                             AS avg_amount,
    SUM(fraud_pred)                                                   AS flagged_count
FROM spectrum_fraud.predictions
GROUP BY 1
ORDER BY 1
WITH NO SCHEMA BINDING;
