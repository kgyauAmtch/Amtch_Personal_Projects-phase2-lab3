import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Glue setup ───────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

# ── Constants ────────────────────────────────────────────────────────────────
REGION = "eu-north-1"
TABLE_KPI = "lab3_kpis"
SOURCE_PARQUET = "s3://lab3-bucket/processed/streams/"
SONGS_PARQUET = "s3://lab3-bucket/processed/songs/"

# ── Logging setup ────────────────────────────────────────────────────────────
logger = logging.getLogger("kpi_job")
logger.setLevel(logging.INFO)

# ── Ensure DynamoDB Table Exists ─────────────────────────────────────────────
def create_table_if_absent(table_name: str):
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "uuid", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "uuid", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 1000}
        ).meta.client.get_waiter("table_exists").wait(TableName=table_name)
        logger.info(f"DynamoDB table {table_name} created.")
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        logger.info(f"DynamoDB table {table_name} already exists.")

create_table_if_absent(TABLE_KPI)

# ── Load Streams Data ────────────────────────────────────────────────────────
logger.info("Loading streams data from S3...")
df_raw = (
    spark.read.parquet(SOURCE_PARQUET)
         .filter(F.col("record_type") == "stream")
)

# ── Load Songs Data and Join ─────────────────────────────────────────────────
logger.info("Loading songs and joining genres...")
songs_df = (
    spark.read.parquet(SONGS_PARQUET)
         .select("track_id", "track_genre")
         .dropDuplicates(["track_id"])
         .withColumn("track_genre", F.lower(F.col("track_genre")))
)

stream_df = (
    df_raw.join(songs_df, on="track_id", how="left")
          .withColumn("track_genre", F.coalesce(F.col("track_genre"), F.lit("unknown")))
          .withColumn("duration_ms", F.lit(180_000))  # Default duration placeholder
          .withColumn("day", F.date_format("listen_time", "yyyy-MM-dd"))
)

# ── KPI Calculations ─────────────────────────────────────────────────────────
logger.info("Computing KPIs...")

kpi1 = (
    stream_df.groupBy("day", "track_genre")
             .agg(F.count("*").alias("listen_count"))
             .withColumn("kpi_type", F.lit("listen_count"))
)

kpi2 = (
    stream_df.groupBy("day", "track_genre")
             .agg(F.countDistinct("user_id").alias("unique_listeners"))
             .withColumn("kpi_type", F.lit("unique_listeners"))
)

kpi3 = (
    stream_df.groupBy("day", "track_genre")
             .agg(F.sum("duration_ms").alias("total_listening_time_ms"))
             .withColumn("kpi_type", F.lit("total_listening_time"))
)

kpi4 = (
    kpi2.join(kpi3, ["day", "track_genre"])
        .withColumn("avg_listening_time_ms",
                    F.col("total_listening_time_ms") / F.col("unique_listeners"))
        .select("day", "track_genre", "avg_listening_time_ms")
        .withColumn("kpi_type", F.lit("avg_listening_time"))
)

song_counts = (
    stream_df.groupBy("day", "track_genre", "track_id")
             .agg(F.count("*").alias("play_count"))
)

kpi5 = (
    song_counts
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("day", "track_genre")
                  .orderBy(F.col("play_count").desc())))
        .filter("rank <= 3")
        .withColumn("kpi_type", F.lit("top_3_songs"))
)

genre_counts = (
    stream_df.groupBy("day", "track_genre")
             .agg(F.count("*").alias("genre_count"))
)

kpi6 = (
    genre_counts
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("day")
                  .orderBy(F.col("genre_count").desc())))
        .filter("rank <= 5")
        .withColumn("kpi_type", F.lit("top_5_genres"))
)

kpi_list = [kpi1, kpi2, kpi3, kpi4, kpi5, kpi6]

# ── Write KPIs to DynamoDB ───────────────────────────────────────────────────
for idx, kpi_df in enumerate(kpi_list, 1):
    try:
        kpi_df = kpi_df.withColumn("uuid", F.expr("uuid()"))  # UUID as primary key

        dynf = DynamicFrame.fromDF(kpi_df, glue, f"kpi{idx}")
        glue.write_dynamic_frame.from_options(
            frame=dynf,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": TABLE_KPI,
                "dynamodb.throughput.write.percent": "1.0",
                "dynamodb.region": REGION,
            },
        )
        logger.info(f"✓ KPI {idx} written to DynamoDB ({kpi_df.count()} records)")
    except Exception as e:
        logger.error(f"✗ Failed to write KPI {idx}: {str(e)}")

job.commit()
logger.info("✔ All KPIs computed and written to DynamoDB.")
