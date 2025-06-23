# 🎧 Music Streaming ETL & KPI Pipeline

This project implements a fully automated ETL pipeline to process streaming music data, compute daily KPIs, and store insights in DynamoDB using AWS‑native services (Airflow MWAA, AWS Glue and S3).

---

##  Project Overview

1. **Monitor** an S3 prefix for new stream files (`raw/streams/*.csv`).
2. **Trigger** an Airflow DAG when a new file arrives.
3. **Clean & transform** raw data with a Glue Spark job.
4. **Compute daily KPIs** with a second Glue job.
5. **Store** KPIs in a single DynamoDB table.
6. **Archive** the processed file in S3.

---

## Stack

| Layer          | Service                     | Purpose                                 |
|----------------|-----------------------------|-----------------------------------------|
| Orchestration  | Amazon MWAA (Airflow)       | DAG scheduling & dependency management  |
| Processing     | AWS Glue (Spark)            | ETL + KPI calculations                  |
| Storage        | Amazon S3                   | Raw, processed & archived data          |
| Analytics DB   | Amazon DynamoDB             | Fast key‑value KPI store                |
| Optional       | EMR Serverless              | Scalable ad‑hoc Spark workloads         |

---

## S3 Layout

```text
s3://lab3-bucket/
├── raw/
│   └── streams/
    └── songs/
    └── users/
├── processed/
│   ├── streams/
|    ├── users/
│   └── songs/
└── archive/
    └── streams/YYYY‑MM‑DD/
```

---

## KPIs

| KPI                              | Description                                    |
|----------------------------------|------------------------------------------------|
| **Listen Count**                 | Total plays per genre/day                      |
| **Unique Listeners**             | Distinct users per genre/day                   |
| **Total Listening Time**         | Sum of duration per genre/day                  |
| **Avg Listening Time per User**  | Listening time ÷ unique listeners             |
| **Top 3 Songs / Genre / Day**    | Rank 1‑3 tracks by plays within each genre/day |
| **Top 5 Genres / Day**           | Rank 1‑5 genres by total plays per day         |

---

##  DynamoDB Schema

| Attribute        | Notes                               |
|------------------|-------------------------------------|
| `day_pk` (PK)    | `YYYY‑MM‑DD`                        |
| `sort_key` (SK)  | `kpi_type#genre[#track_id]`         |
| Other columns    | metric_value, rank, track_id, etc.  |
| Billing mode     | `PAY_PER_REQUEST`                   |

Example item:

```json
{
  "day_pk": "2025-06-21",
  "sort_key": "top_3_songs#pop#track_123",
  "kpi_type": "top_3_songs",
  "track_genre": "pop",
  "track_id": "track_123",
  "metric_value": 512,
  "rank": 1
}
```

---

## 🔁 Airflow DAG: `glue_stream_etl_pipeline`

```mermaid
graph LR
A(Wait S3) --> B(Detect file) --> C(Glue: Clean) --> D(Glue: KPIs) --> E(Archive)
```

- **Schedule**: hourly (`@hourly`)  
- **S3KeySensor** in *reschedule* mode (efficient polling)  
- **Max active runs** set to 1 to avoid overlap.

---

## Deployment Steps

1. **Upload scripts**  
   - `generate_daily_kpis.py` → `s3://lab3-bucket/scripts/`
2. **Create Glue jobs**  
   - `Transformation_job` (clean)  
   - `dailykpis` (KPI calc)  
3. **Grant IAM permissions**  
   - Airflow role → `glue:StartJobRun`, `glue:GetJob*`, `s3:*`, `dynamodb:*`
4. **Run the DAG** – new CSV in `raw/streams/` triggers full pipeline.

---



