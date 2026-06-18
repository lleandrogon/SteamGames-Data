import pandas as pd
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

storage_options = {
    "key": os.getenv("MINIO_ROOT_USER"),
    "secret": os.getenv("MINIO_ROOT_PASSWORD"),
    "client_kwargs": {
        "endpoint_url": f"http://{os.getenv('MINIO_ENDPOINT')}"
    }
}

bucket = os.getenv("MINIO_BUCKET")

df = pd.read_parquet(
    f"s3://{bucket}/medallion/silver/top_games_2026.parquet",
    storage_options = storage_options
)

df = duckdb.sql("""--sql
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY peak_ccu DESC) AS peak_ccu_rank,
        CURRENT_TIMESTAMP AS gold_ingest_date
    FROM df;
""").df()

df.to_parquet(
    f"s3://{bucket}/medallion/gold/top_games_2026.parquet",
    storage_options = storage_options
)