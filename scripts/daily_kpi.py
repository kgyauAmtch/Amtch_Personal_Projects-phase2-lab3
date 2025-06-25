import boto3
import logging
import pandas as pd
import uuid
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow.fs as fs

# ── Config ──────────────────────────────────────────────────────────────────
REGION = "eu-north-1"
TABLE_KPI = "lab3_kpis"
STREAMS_PATH = "lab3-bucket/processed/streams/"
SONGS_PATH = "lab3-bucket/processed/songs/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kpi_loader")

# ── DynamoDB Helper function ────────────────────────────────────────────────────────
def create_table_if_absent():
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    try:
        table = dynamodb.create_table(
            TableName=TABLE_KPI,
            KeySchema=[{"AttributeName": "uuid", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "uuid", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 1000}
        )
        table.meta.client.get_waiter("table_exists").wait(TableName=TABLE_KPI)
        logger.info("DynamoDB table created.")
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        table = dynamodb.Table(TABLE_KPI)
        logger.info("DynamoDB table already exists.")
    return table

# ── Read Parquet from S3 ─────────────────────────────────────────────────────
def load_parquet_df(s3_path):
    s3fs = fs.S3FileSystem(region=REGION)
    dataset = pq.ParquetDataset(s3_path, filesystem=s3fs)
    return dataset.read().to_pandas()

# ── KPI Calculation ─────────────────────────────────────────────────────────
def compute_kpis(streams_df, songs_df):
    streams_df = streams_df[streams_df["record_type"] == "stream"].copy()

    df = streams_df.merge(
        songs_df.drop_duplicates("track_id"),
        on="track_id",
        how="left"
    )
    df["track_genre"] = df["track_genre"].fillna("unknown").str.lower()
    df["track_name"] = df["track_name"].fillna("unknown_song")
    df["duration_ms"] = 180_000  # default duration placeholder
    df["day"] = pd.to_datetime(df["listen_time"]).dt.date

    kpi1 = df.groupby(["day", "track_genre"], as_index=False).agg(listen_count=("track_id", "count"))
    kpi2 = df.groupby(["day", "track_genre"], as_index=False).agg(unique_listeners=("user_id", pd.Series.nunique))
    kpi3 = df.groupby(["day", "track_genre"], as_index=False).agg(total_listening_time_ms=("duration_ms", "sum"))

    kpi4 = kpi2.merge(kpi3, on=["day", "track_genre"])
    kpi4["avg_listening_time_ms"] = kpi4["total_listening_time_ms"] / kpi4["unique_listeners"]
    kpi4 = kpi4[["day", "track_genre", "avg_listening_time_ms"]]

    song_counts = df.groupby(["day", "track_genre", "track_name"], as_index=False).agg(play_count=("track_id", "count"))
    song_counts["rank"] = song_counts.groupby(["day", "track_genre"]).play_count.rank(method="first", ascending=False)
    top3 = song_counts[song_counts["rank"] <= 3][["day", "track_genre", "track_name", "play_count"]]

    genre_counts = df.groupby(["day", "track_genre"], as_index=False).agg(genre_count=("track_id", "count"))
    genre_counts["rank"] = genre_counts.groupby("day").genre_count.rank(method="first", ascending=False)
    top5 = genre_counts[genre_counts["rank"] <= 5][["day", "track_genre", "genre_count"]]

    return {
        "listen_count": kpi1,
        "unique_listeners": kpi2,
        "total_listening_time_ms": kpi3,
        "avg_listening_time_ms": kpi4,
        "top_3_songs": top3,
        "top_5_genres": top5,
    }

# ── Write to DynamoDB ────────────────────────────────────────────────────────
def write_to_dynamodb(kpi_dict, table):
    client = boto3.client("dynamodb", region_name=REGION)

    for kpi_type, df in kpi_dict.items():
        for _, row in df.iterrows():
            item = {
                "uuid": {"S": str(uuid.uuid4())},
                "kpi_type": {"S": kpi_type},
                "day": {"S": str(row["day"])},
            }

            for col in row.index:
                if col in ["day"]:
                    continue
                val = row[col]
                if pd.isnull(val):
                    continue
                item[col] = {"N": str(round(val, 2))} if isinstance(val, (int, float)) else {"S": str(val)}

            client.put_item(TableName=TABLE_KPI, Item=item)

# ── Run ──────────────────────────────────────────────────────────────────────
logger.info("Loading cleaned data...")
streams_df = load_parquet_df(STREAMS_PATH)
songs_df = load_parquet_df(SONGS_PATH)

logger.info("Computing KPIs...")
kpis = compute_kpis(streams_df, songs_df)

logger.info("Writing KPIs to DynamoDB...")
table = create_table_if_absent()
write_to_dynamodb(kpis, table)
logger.info("All KPIs successfully written with descriptive columns.")