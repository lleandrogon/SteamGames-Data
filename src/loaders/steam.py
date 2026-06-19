import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from dotenv import load_dotenv

load_dotenv()

def load_games():

    storage_options = {
        "key": os.getenv("MINIO_ROOT_USER"),
        "secret": os.getenv("MINIO_ROOT_PASSWORD"),
        "client_kwargs": {
            "endpoint_url": f"http://{os.getenv('MINIO_ENDPOINT')}"
        }
    }

    bucket = os.getenv("MINIO_BUCKET")

    df = pd.read_parquet(
        f"s3://{bucket}/medallion/gold/top_games_2026.parquet",
        storage_options = storage_options
    )

    hook = PostgresHook(postgres_conn_id = "steam", schema = "public")

    query = """--sql
        INSERT INTO steam (
            app_id,
            name,
            release_date,
            coming_soon,
            price_usd,
            is_free,
            discount_pct,
            developer,
            publisher,
            genres,
            categories,
            tags,
            windows,
            mac,
            linux,
            metacritic_score.
            recommendations,
            positive_reviews,
            negative_reviews,
            estimated_owners,
            avg_playtime_forever,
            avg_playtime__2weeks,
            median_playtime,
            peak_ccu,
            required_age,
            dlc_count,
            achievements,
            short_description,
            header_image,
            platforms,
            min_estimated_owners,
            max_estimated_owners,
            avg_estimated_owners,
            peak_ccu_rank,
            gold_ingest_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """