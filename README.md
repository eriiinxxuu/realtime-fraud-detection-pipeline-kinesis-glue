# Real-Time Fraud Detection Pipeline with Kinesis and Glue streaming job

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![AWS](https://img.shields.io/badge/AWS-Kinesis_&_ECS_Fargate-232F3E.svg)
![PySpark](https://img.shields.io/badge/PySpark-Glue_Streaming-E25A1C.svg)
![LightGBM](https://img.shields.io/badge/LightGBM-SageMaker_Endpoint-0194E2.svg)
![Redshift](https://img.shields.io/badge/Redshift-Spectrum_Analytics-8C4FFF.svg)
![CI/CD](https://img.shields.io/badge/CI/CD-GitHub_Actions-2088FF.svg)
![Terraform](https://img.shields.io/badge/Terraform-Infrastructure_as_Code-623CE4.svg)
![Security](https://img.shields.io/badge/Network-Private_VPC-green.svg)

## Introduction

A real-time fraud detection system built on AWS, ingesting synthetic transaction events through a Kinesis streaming pipeline, scoring each transaction against a LightGBM model triggered by Lambda, and surfacing analytics via a Redshift Spectrum dashboard.

This project is an evolution of `v1`: [realtime-fraud-detection-pipeline](https://github.com/eriiinxxuu/realtime-fraud-detection-pipeline), which trained the LightGBM model on the same synthetic dataset. The model artifact from `v1` is deployed here as a SageMaker real-time endpoint, with the pipeline rebuilt on **Kinesis + Glue Streaming** replacing the original Kafka + Spark architecture.

## Architecture
```bash
ECS Producer (Fargate ARM64)
    │  synthetic transactions
    ▼
Kinesis Data Streams (2 shards)
    │
    ▼
Glue Streaming Job (PySpark)
    │  feature engineering → parquet
    ▼
S3 (raw-transactions/features/)
    │
    ▼
Lambda (S3 trigger)
    │  batch inference
    ▼
SageMaker Endpoint (LightGBM)
    │
    ├── fraud_score ≥ 0.86 → SNS fraud-alerts → Ops team
    │
    ▼
S3 (fraud-predictions/)
    │
    ▼
Glue Crawler → Glue Data Catalog
    │
    ▼
Redshift Spectrum → QuickSight Dashboard
```
## Techinical Skills
- **Cloud & Infrastructure**: AWS (Kinesis, Glue, SageMaker, Lambda, ECS Fargate, ECR, S3, Redshift Serverless, SNS, CloudWatch)
- **Data Engineering**: PySpark, Glue Streaming, Apache Parquet, Feature Engineering, Redshift Spectrum, Glue Data Catalog
- **Machine Learning**: LightGBM, SageMaker Endpoint, Model Packaging, Real-time Inference
- **DevOps**: Docker CI/CD, Terraform, GitHub Actions
- **Programming**: Python, PySpark, SQL, HCL (Terraform)
  
## Infrastructure
All infrastructure managed as Terraform modules:
```bash

aws_deploy_screenshots  resources screenshots
src
├──producer             producer image
terraform/
├── modules/
│   ├── networking/     VPC, subnets, security groups, VPC endpoints, IGW
│   ├── kinesis/        Data stream
│   ├── ecr/            Container registry
│   ├── ecs/            Fargate cluster and producer service
│   ├── s3/             4 buckets: model-artifacts, raw-transactions, predictions, glue-assets
│   ├── iam/            7 roles with least-privilege policies
│   ├── glue/           Streaming job and script
│   ├── sagemaker/      Model, endpoint config, endpoint
│   ├── lambda/         Inference trigger function
│   ├── sns/            fraud-alerts and ops-alerts topics
│   ├── redshift/       Serverless namespace and workgroup
│   └── cloudwatch/     Alarms
└── envs/prod/          Root module wiring all components
    │
    ▼
Redshift Spectrum → QuickSight Dashboard
    │
    ▼
QuickSight
```

CI/CD via GitHub Actions:

| Workflow | Trigger | Action |
|----------|---------|--------|
| `terraform.yml` | Push to `terraform/**` | Plan and apply infrastructure |
| `deploy-glue.yml` | Push to `terraform/modules/glue/**` | Upload Glue script to S3, restart job |
| `deploy-producer.yml` | Push to `terraform/src/producer/**` | Build ARM64 image, push to ECR, update ECS |

## Monitoring

| Alarm | Metric | Threshold |
|-------|--------|-----------|
| kinesis-consumer-lag | `GetRecords.IteratorAgeMilliseconds` | > 60,000 ms for 3 min |
| glue-no-throughput | `glue.driver.aggregate.recordsRead` | = 0 for 15 min |
| lambda-errors | `Errors` | > 5 in 5 min |
| sagemaker-latency | `ModelLatency`p99 | > 2,000 ms for 5 min |

All alarms publish to the `ops-alerts` SNS topic.

## Setup

### Prerequisites

- AWS CLI configured
- Terraform >= 1.5
- Docker with buildx

### GitHub Secrets

| Secret | Value |
|--------|-------|
| `AWS_ROLE_ARN` | OIDC role ARN for GitHub Actions |
| `AWS_ACCOUNT_ID` | AWS account ID |
| `TF_STATE_BUCKET` | S3 bucket for Terraform state |
| `REDSHIFT_ADMIN_PASSWORD` | Redshift admin password |
| `GLUE_ASSETS_BUCKET` | `fraud-detection-kinesis-glue-glue-assets` |
| `PROJECT_NAME` | `fraud-detection-kinesis-glue` |

### Deploy

```bash
# 1. Upload model artifact
cd terraform/src/sagemaker
COPYFILE_DISABLE=1 tar -czf model.tar.gz equirements.txt model.pkl inference.py
aws s3 cp model.tar.gz s3://fraud-detection-kinesis-glue-model-artifacts/model/model.tar.gz

# 2. Initialise and apply infrastructure
cd terraform/envs/prod
terraform init -backend-config="bucket=<tf-state-bucket>"
terraform apply
```

