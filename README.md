# Uber End-to-End Data Engineering Platform

> A production-grade, cloud-native data engineering platform built on AWS, simulating Uber's ride analytics pipeline. Showcases **18+ technologies** across batch processing, real-time streaming, data warehousing, and ML feature engineering.

---

## 🏗️ Architecture

<p align="center">
  <img src="docs/images/architecture_diagram.png" alt="System Architecture" width="700"/>
</p>

The platform follows a **layered architecture** with clear separation between ingestion, processing, warehousing, and orchestration:

```
Data Sources → Ingestion (API Gateway + Lambda)
    ↓
Real-Time: Kinesis → Lambda → Firehose → S3 + DynamoDB
Batch: S3 Bronze → Glue ETL → S3 Silver → S3 Gold
    ↓
Warehouse: Redshift (Star Schema) + Snowflake + Athena
    ↓
Orchestration: Airflow + Step Functions + EventBridge
Monitoring: CloudWatch + SNS Alerts + Great Expectations
```

---

## 🔄 Data Pipeline

<p align="center">
  <img src="docs/images/pipeline_flow.png" alt="End-to-End Pipeline" width="700"/>
</p>

### Medallion Architecture

| Layer | Purpose | Format | Partitioning |
|:---|:---|:---|:---|
| 🥉 **Bronze** | Raw, unprocessed events | JSON/CSV | `year/month/day` |
| 🥈 **Silver** | Cleaned, validated, enriched | Parquet (Snappy) | `event_date/city` |
| 🥇 **Gold** | Business-ready aggregations & star schema | Parquet | `ride_date` |

---

## 🛠️ Technology Stack

<p align="center">
  <img src="docs/images/tech_stack.png" alt="Technology Stack" width="600"/>
</p>

| Category | Technologies |
|:---|:---|
| **Storage** | Amazon S3 (Data Lake – Bronze/Silver/Gold), DynamoDB |
| **Compute** | AWS Lambda, EC2, EMR (Spark), AWS Glue (PySpark) |
| **Streaming** | Amazon Kinesis Data Streams, Kinesis Firehose, Apache Kafka (MSK) |
| **Data Warehouse** | Amazon Redshift, Snowflake |
| **Transformation** | dbt, AWS Glue ETL, PySpark |
| **Orchestration** | AWS Step Functions, Apache Airflow (MWAA), Amazon EventBridge |
| **API** | Amazon API Gateway |
| **Analytics** | Amazon Athena, Redshift Spectrum |
| **Data Quality** | Great Expectations |
| **Monitoring** | Amazon CloudWatch, Amazon SNS |
| **Infrastructure** | Terraform (IaC) |
| **CI/CD** | GitHub Actions |
| **Containers** | Docker, Amazon ECR |
| **Data Catalog** | AWS Glue Data Catalog |

---

## 📁 Project Structure

```
uber-data-engineering-platform/
│
├── terraform/                          # 🔧 Infrastructure as Code
│   ├── main.tf                         # Root configuration (S3 backend)
│   ├── variables.tf / outputs.tf       # Input/output definitions
│   └── modules/
│       ├── vpc/                        # VPC, subnets, NAT, security groups
│       ├── s3/                         # Bronze/Silver/Gold + lifecycle policies
│       ├── kinesis/                    # Data Streams + Firehose delivery
│       ├── redshift/                   # Cluster + Spectrum + IAM
│       ├── glue/                       # Crawlers, ETL jobs, catalog DB
│       ├── lambda/                     # 3 functions + API Gateway
│       ├── emr/                        # Spark cluster + auto-scaling
│       ├── dynamodb/                   # Live ride metrics table
│       ├── step_functions/             # ETL orchestration state machine
│       └── monitoring/                 # CloudWatch dashboard + SNS alarms
│
├── data_sources/                       # 📊 Data Generation
│   ├── ride_event_generator.py         # Multi-mode: batch / stream / local
│   ├── historical_data_generator.py    # Partitioned Parquet output
│   └── schemas/ride_event_schema.json  # JSON schema definition
│
├── ingestion/                          # 📥 Data Ingestion Layer
│   ├── lambda_kinesis_producer/        # REST API → Kinesis producer
│   ├── lambda_s3_trigger/              # S3 event → validate → Bronze
│   └── api_gateway/api_spec.yaml       # OpenAPI 3.0 specification
│
├── streaming/                          # ⚡ Real-Time Pipeline
│   ├── lambda_stream_processor/        # Enrich + Firehose + DynamoDB
│   ├── kinesis_analytics/              # SQL tumbling/sliding windows
│   └── kafka_producer/                 # MSK/Kafka alternative
│
├── batch_processing/                   # ⚙️ Batch ETL
│   ├── glue_jobs/
│   │   ├── bronze_to_silver.py         # PySpark: clean, dedupe, validate
│   │   └── silver_to_gold.py           # PySpark: star schema + KPIs
│   ├── glue_crawlers/                  # Crawler configurations
│   └── emr_jobs/
│       ├── heavy_aggregation.py        # Geospatial + driver scoring + ML
│       └── bootstrap.sh               # EMR dependency installer
│
├── data_warehouse/                     # 🏢 Data Warehouse
│   ├── redshift/
│   │   ├── ddl/schema.sql              # Star schema (6 tables)
│   │   ├── copy_commands.sql           # S3 → Redshift COPY
│   │   ├── analytical_queries.sql      # 6 complex BI queries
│   │   └── spectrum_setup.sql          # Federated S3 queries
│   ├── snowflake/
│   │   ├── setup.sql                   # Warehouse + external stages
│   │   └── pipes.sql                   # Snowpipe + CDC streams
│   └── athena/queries.sql              # Serverless analytics
│
├── dbt_models/                         # 🔀 dbt Transformations
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/stg_rides.sql       # Standardize raw data
│       ├── intermediate/int_ride_metrics.sql  # Derived metrics
│       ├── marts/fct_daily_rides.sql   # Daily aggregated facts
│       ├── marts/dim_drivers.sql       # Driver performance dimension
│       └── schema.yml                  # Tests + column documentation
│
├── orchestration/                      # 🎯 Pipeline Orchestration
│   ├── step_functions/etl_pipeline.json  # AWS state machine
│   ├── airflow/dags/
│   │   ├── uber_etl_dag.py             # Daily batch ETL pipeline
│   │   └── streaming_monitor_dag.py    # 15-min health checks
│   └── eventbridge/rules.json          # Scheduled triggers
│
├── data_quality/                       # ✅ Data Quality
│   └── great_expectations/
│       ├── expectations/               # 19 validation rules
│       ├── checkpoints/                # Daily validation runs
│       └── great_expectations.yml      # S3-based stores
│
├── docker/                             # 🐳 Local Development
│   ├── docker-compose.yml              # LocalStack + Kafka + Airflow + Spark
│   ├── localstack/init.sh              # AWS resource bootstrap
│   └── Dockerfile.etl                  # PySpark container
│
├── .github/workflows/                  # 🚀 CI/CD
│   ├── ci.yml                          # Lint + validate + scan
│   └── deploy.yml                      # Terraform + Lambda + Glue deploy
│
├── docs/images/                        # 📸 Architecture diagrams
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- **AWS Account** with appropriate IAM permissions
- **Terraform** >= 1.5.0
- **Python** >= 3.12
- **Docker** & Docker Compose
- **AWS CLI** configured with credentials

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/uber-data-engineering-platform.git
cd uber-data-engineering-platform
```

### 2. Local Development (Docker)

```bash
# Start all local services (LocalStack, Kafka, Airflow, Spark, PostgreSQL)
cd docker
docker compose up -d

# Verify LocalStack resources
awslocal s3 ls
awslocal kinesis list-streams
awslocal dynamodb list-tables

# Access Airflow UI at http://localhost:8080 (admin/admin)
```

### 3. Generate Sample Data

```bash
pip install -r requirements.txt

# Preview sample events
cd data_sources
python ride_event_generator.py --mode local --count 5

# Generate 100K batch records
python ride_event_generator.py --mode batch --count 100000 --output ../sample_data/rides.json

# Generate historical Parquet dataset
python historical_data_generator.py --events-per-year 100000 --years 2022 2023 2024
```

### 4. Deploy to AWS

```bash
# Initialize and deploy infrastructure
cd terraform
terraform init
terraform plan
terraform apply

# Deploy Lambda functions
for func in lambda_kinesis_producer lambda_s3_trigger; do
    cd ../ingestion/$func
    zip -r /tmp/$func.zip handler.py
    aws lambda update-function-code --function-name uber-de-dev-$func --zip-file fileb:///tmp/$func.zip
    cd ../../
done

# Upload Glue scripts
aws s3 sync batch_processing/glue_jobs/ s3://uber-de-dev-scripts/glue/
```

### 5. Run the Pipeline

```bash
# Start streaming events to Kinesis
python data_sources/ride_event_generator.py --mode stream --stream-name uber-de-dev-ride-events --eps 50 --duration 300

# Trigger batch ETL via Step Functions
aws stepfunctions start-execution \
    --state-machine-arn arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:uber-de-dev-etl-pipeline

# Run dbt models
cd dbt_models
dbt run --target prod
dbt test
```

---

## 📊 Star Schema

```
                    ┌──────────────┐
                    │  dim_time    │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌───────▼───────┐   ┌──────────────────┐
│  dim_riders  │───│  fact_rides   │───│  dim_vehicle_types│
└──────────────┘   └───────┬───────┘   └──────────────────┘
                           │
┌──────────────┐   ┌───────▼───────┐
│ dim_drivers  │───│ dim_locations │
└──────────────┘   └───────────────┘
```

| Table | Records | Description |
|:---|:---|:---|
| `fact_rides` | Millions | Individual completed ride records with all metrics |
| `dim_drivers` | Thousands | Driver profiles with performance tiers (PLATINUM→BRONZE) |
| `dim_riders` | Thousands | Rider profiles with preferences |
| `dim_time` | ~4,000 | Calendar dimension (2020–2030) |
| `dim_vehicle_types` | 6 | UberX, UberXL, UberBlack, Pool, Comfort, Green |
| `dim_locations` | Dynamic | Geographic grid zones |

---

## 📈 Key Analytics & Insights

| Analysis | Description | Tool |
|:---|:---|:---|
| **Revenue Trends** | Monthly revenue by city with MoM growth | Redshift |
| **Peak Hour Surge** | Hourly demand with P50/P95 fare distributions | Redshift |
| **Driver Scoring** | Composite performance with 4-tier classification | EMR Spark |
| **Weather Impact** | Demand & pricing shifts by weather conditions | Redshift |
| **Rider Retention** | Weekly cohort analysis with retention curves | Redshift |
| **Geospatial Heatmaps** | Pickup/dropoff hotspot grid analysis | EMR Spark |
| **Route Popularity** | Top origin-destination pairs by city | EMR Spark |
| **Real-Time Metrics** | Live rides/min, surge hotspots per city | Kinesis Analytics |

---

## 🔄 CI/CD Pipeline

| Stage | Tool | Checks |
|:---|:---|:---|
| **Lint** | flake8, black, isort | Code quality + formatting |
| **Validate** | Terraform validate | Infrastructure configs |
| **Syntax** | py_compile | All Python files |
| **Schema** | JSON/YAML validators | All config files |
| **Security** | Trivy | Vulnerability scanning |
| **Deploy** | Terraform apply | Infrastructure provisioning |
| **Release** | AWS CLI | Lambda + Glue script deployment |

---

## 📊 Monitoring & Alerting

- **CloudWatch Dashboard**: Lambda metrics, Kinesis throughput & lag, Glue job status
- **Alarms**: Consumer lag > 5min, Lambda error rate > 5%, Glue job failures
- **SNS Notifications**: Email/Slack alerts on any pipeline failure
- **Airflow Health DAG**: Automated Kinesis + Lambda + DynamoDB checks every 15 minutes
- **Great Expectations**: 19 data quality rules validated daily on Silver layer

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License.

---

> **⚠️ Note**: Replace `ACCOUNT_ID` placeholders with your actual AWS account ID before deploying.

---

**Built with ❤️ for Data Engineering**
