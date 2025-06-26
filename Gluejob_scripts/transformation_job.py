import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from pyspark.context import SparkContext
from pyspark.sql.functions import *

# ──────────────────────────────────────────────────────────────────────────────
# 1. Args & logging
# ──────────────────────────────────────────────────────────────────────────────
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "INPUT_FILE_PATH"])
    streams_input_path = args["INPUT_FILE_PATH"]
    job_name = args["JOB_NAME"]
    logger_msg = f"INPUT_FILE_PATH = {streams_input_path}"
except Exception:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = args["JOB_NAME"]
    streams_input_path = "s3://lab3-bucket/validated/streams/streams1.csv"  # fallback
    logger_msg = "No --INPUT_FILE_PATH passed; using fallback test file."

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info(f"[GLUE] Job '{job_name}' started")
logger.info(logger_msg)

# ──────────────────────────────────────────────────────────────────────────────
# 2. Glue / Spark context
# ──────────────────────────────────────────────────────────────────────────────
sc           = SparkContext()
glueContext  = GlueContext(sc)
spark        = glueContext.spark_session
job          = Job(glueContext)
job.init(job_name, args)

Validated_base         = "s3://lab3-bucket/validated"
PROCESSED_BASE = "s3://lab3-bucket/processed"

# ──────────────────────────────────────────────────────────────────────────────
# 3. Helper: column normaliser
# ──────────────────────────────────────────────────────────────────────────────
def _normalise_columns(df):
    """Trim + lowercase all column names."""
    return df.select([col(c).alias(c.strip().lower()) for c in df.columns])

# ──────────────────────────────────────────────────────────────────────────────
# 4. Stream transform
# ──────────────────────────────────────────────────────────────────────────────
def transform_streams(glue_ctx, dfc) -> DynamicFrame:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = _normalise_columns(df)

    df = df.withColumn("user_id", col("user_id").cast("int")) \
          .withColumn("track_id",  trim(lower(col("track_id"))))\
          .withColumn("listen_time", to_timestamp(col("listen_time")))\
          .dropna(subset=["user_id", "track_id", "listen_time"])\
          .dropDuplicates(["user_id", "track_id", "listen_time"])\
          .withColumn("record_type", lit("stream"))\
          .withColumn("uuid",
                      concat_ws("_", lit("stream"), col("user_id"), col("track_id")))
    return DynamicFrame.fromDF(df, glue_ctx, "cleaned_streams")

# ──────────────────────────────────────────────────────────────────────────────
# 5. Songs transform
# ──────────────────────────────────────────────────────────────────────────────
def transform_songs(glue_ctx, dfc) -> DynamicFrame:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = _normalise_columns(df)

    df = df.withColumn(
            "explicit",
            when(lower(trim(col("explicit"))) == "true",  True)\
            .when(lower(trim(col("explicit"))) == "false", False)\
            .otherwise(col("explicit").cast("boolean"))
          )

    for c in ["track_id", "artists", "album_name", "track_name", "track_genre"]:
        df = df.withColumn(c, trim(lower(col(c))))

    cast_map = {
        "popularity": "int", "duration_ms": "int", "danceability": "double",
        "energy": "double", "key": "int", "loudness": "double", "mode": "int",
        "speechiness": "double", "acousticness": "double",
        "instrumentalness": "double", "liveness": "double",
        "valence": "double", "tempo": "double", "time_signature": "int"
    }
    for c, t in cast_map.items():
        df = df.withColumn(c, col(c).cast(t))

    df = (df.dropDuplicates(["track_id"])
            .dropna(subset=["track_id", "track_name"])
            .withColumn("record_type", lit("song")))
    return DynamicFrame.fromDF(df, glue_ctx, "cleaned_songs")

# ──────────────────────────────────────────────────────────────────────────────
# 6. Users transform
# ──────────────────────────────────────────────────────────────────────────────
def transform_users(glue_ctx, dfc) -> DynamicFrame:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = _normalise_columns(df)

    df = df.withColumn("user_id",    col("user_id").cast("int"))\
          .withColumn("user_age",   col("user_age").cast("int"))\
          .withColumn("user_name",  trim(lower(col("user_name"))))\
          .withColumn("user_country", trim(lower(col("user_country"))))\
          .withColumn("created_at", to_date(col("created_at")))\
          .dropna(subset=["user_id", "user_name", "created_at"])\
          .dropDuplicates(["user_id"])\
          .withColumn("account_age_days",
                      datediff(current_date(), col("created_at")))\
          .withColumn("record_type", lit("user"))
    
    return DynamicFrame.fromDF(df, glue_ctx, "cleaned_users")

# ──────────────────────────────────────────────────────────────────────────────
# 7. Read → wrap → transform → write
# ──────────────────────────────────────────────────────────────────────────────
def read_wrap(path):
    """Read CSV, return one-item DynamicFrameCollection."""
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options={"withHeader": True, "quoteChar": "\"", "separator": ","},
        connection_options={"paths": [path]},
    )
    return DynamicFrameCollection({"default": dyf}, glueContext)

# STREAMS
streams_clean = transform_streams(glueContext, read_wrap(streams_input_path)).coalesce(1)
glueContext.write_dynamic_frame.from_options(
    frame=streams_clean,
    connection_type="s3",
    format="parquet",
    connection_options={"path": f"{PROCESSED_BASE}/streams/"},
)

# SONGS
songs_clean = transform_songs(glueContext, read_wrap(f"{Validated_base}/songs/songs.csv"))
glueContext.write_dynamic_frame.from_options(
    frame=songs_clean,
    connection_type="s3",
    format="parquet",
    connection_options={"path": f"{PROCESSED_BASE}/songs/"},
)

# USERS
users_clean = transform_users(glueContext, read_wrap(f"{Validated_base}/users/users.csv"))
glueContext.write_dynamic_frame.from_options(
    frame=users_clean,
    connection_type="s3",
    format="parquet",
    connection_options={"path": f"{PROCESSED_BASE}/users/"},
)

# ──────────────────────────────────────────────────────────────────────────────
# 8. Finish
# ──────────────────────────────────────────────────────────────────────────────
job.commit()
logger.info("🏁 Glue job finished successfully")