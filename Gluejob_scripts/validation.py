import sys
import boto3
import pandas as pd
import io
import logging
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def s3_read_csv(s3_client, bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()))

def s3_write_csv(s3_client, df, bucket, key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

def validate_and_clean(df, required_columns):
    # Check required columns
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    # Drop rows with nulls in required columns
    df_clean = df.dropna(subset=required_columns)
    return df_clean

def process_file(s3_client, input_path, output_path, required_columns):
    parsed_in = urlparse(input_path)
    bucket_in, key_in = parsed_in.netloc, parsed_in.path.lstrip("/")

    parsed_out = urlparse(output_path)
    bucket_out, key_out = parsed_out.netloc, parsed_out.path.lstrip("/")

    logger.info(f"Reading {input_path}")
    df = s3_read_csv(s3_client, bucket_in, key_in)
    logger.info(f"Validating columns in {input_path}")
    df_clean = validate_and_clean(df, required_columns)
    logger.info(f"Writing validated data to {output_path}")
    s3_write_csv(s3_client, df_clean, bucket_out, key_out)
    logger.info(f"Processed {input_path} successfully")

if __name__ == "__main__":
    s3 = boto3.client("s3")

    # Define file paths and required columns
    files_info = [
        {
            "input": "s3://lab3-bucket/raw/streams/streams1.csv",
            "output": "s3://lab3-bucket/validated/streams/streams1.csv",
            "required": ["user_id", "track_id", "listen_time"]
        },
        {
            "input": "s3://lab3-bucket/raw/songs/songs.csv",
            "output": "s3://lab3-bucket/validated/songs/songs.csv",
            "required": ["id", "track_id", "artists", "album_name", "track_name", "popularity", "duration_ms", "explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature", "track_genre"]
        },
        {
            "input": "s3://lab3-bucket/raw/users/users.csv",
            "output": "s3://lab3-bucket/validated/users/users.csv",
            "required": ["user_id","user_name","user_age","user_country","created_at"]
        }
    ]

    for f in files_info:
        try:
            process_file(s3, f["input"], f["output"], f["required"])
        except Exception as e:
            logger.error(f"Error processing {f['input']}: {e}")
            sys.exit(1)

    logger.info("All files validated and saved successfully.")
