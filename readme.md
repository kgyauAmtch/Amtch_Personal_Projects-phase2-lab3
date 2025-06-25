# Music Streaming Data Pipeline Project

## Overview
This project implements a data pipeline for processing music streaming data using AWS services and Apache Airflow. The pipeline ingests raw data from S3 buckets, validates and transforms it using AWS Glue jobs, calculates Key Performance Indicators (KPIs), and stores the results in DynamoDB.

## Architecture
- **Data Sources**: Raw data is stored in two S3 buckets (`S3 Bucket Raw streams` and `S3 Bucket songs and user`).
- **Ingestion Tools**: 
  - `S3KeySensor` detects new files in the raw S3 bucket.
  - Apache Airflow orchestrates the workflow.
  - `AWS Glue Transformation` processes and transforms data.
- **Processing**: 
  - Python operators validate and archive data.
  - Glue jobs clean data and compute KPIs.
- **Storage**: Processed data is archived in `S3 Archive`, and KPIs are loaded into `DynamoDB`.

![Architecture Diagram](attachment://Users/gyauk/github/phase2-labs/Amtch_Personal_Projects-phase2-lab3/Architecture_diagram.png)

## Components

### 1. **Airflow DAG (`dag.py`)**
- Defines the ETL workflow with tasks for detecting new files, validating columns, running Glue jobs, and archiving data.
- Uses `S3KeySensor` to monitor new `.csv` files every 10 minutes.
- Includes branching logic to skip processed files.

### 2. **Validation Script (`validation.py`)**
- Validates CSV files against required columns.
- Cleans data by removing rows with missing values in critical columns.
- Writes validated data to a processed S3 bucket.

### 3. **Transformation Job (`transformation_job.py`)**
- A Glue job that normalizes and transforms streams, songs, and users data.
- Converts data types, handles duplicates, and writes results as Parquet files to `S3 Bucket processed`.

### 4. **KPI Calculation Script (`daily_kpi.py`)**
- Computes six KPIs using PySpark:
  - Listen count per genre per day.
  - Unique listeners per genre per day.
  - Total listening time per genre per day.
  - Average listening time per user per genre per day.
  - Top 3 songs per genre per day.
  - Top 5 genres per day.
- Stores results in a DynamoDB table (`lab3_kpis`).

## Setup Instructions

### Prerequisites
- AWS account with appropriate permissions.
- Apache Airflow installed and configured with AWS credentials.
- AWS Glue jobs and DynamoDB table set up.
- S3 buckets created with the specified prefixes.

### Configuration
- Update `dag.py` with your:
  - `BUCKET_NAME`
  - `AWS_REGION`
  - `GLUE_ROLE_NAME`
  - `GLUE_CLEAN_JOB` and `GLUE_KPI_JOB` names.
- Ensure AWS credentials are set in Airflow's `aws_default` connection.

### Running the Pipeline
1. Deploy the DAG to your Airflow environment.
2. Upload raw `.csv` files to the `raw/streams/` S3 prefix.
3. Monitor the Airflow UI for task execution.
4. Check `S3 Archive` for archived files and `DynamoDB` for KPI results.

## Usage Notes
- The pipeline runs every 10 minutes to detect new files.
- Validation ensures required columns are present before processing.
- Archived files are stored with a date-based prefix for organization.

## Troubleshooting
- Check Airflow logs for task failures.
- Verify S3 bucket permissions and Glue job configurations.
- Ensure DynamoDB table exists and has sufficient throughput.

## Contributing
Feel free to submit issues or pull requests for enhancements.

## License
This project is licensed under the MIT License.