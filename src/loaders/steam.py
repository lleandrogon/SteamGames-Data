import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import numpy as np
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

    def to_native(value):
        if pd.isna(value):
            return None

        if isinstance(value, np.integer):
            return int(value)

        if isinstance(value, np.floating):
            return float(value)

        if isinstance(value, np.bool_):
            return bool(value)

        return value

    hook = PostgresHook(postgres_conn_id = "steam", schema = "public")

    query = """--sql
        INSERT INTO public.top_games_2026 (
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
            metacritic_score,
            recommendations,
            positive_reviews,
            negative_reviews,
            estimated_owners,
            avg_playtime_forever,
            avg_playtime_2weeks,
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
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (app_id) DO UPDATE SET
            app_id = EXCLUDED.app_id
    """

    for row in df[
        [
            "app_id",
            "name",
            "release_date",
            "coming_soon",
            "price_usd",
            "is_free",
            "discount_pct",
            "developer",
            "publisher",
            "genres",
            "categories",
            "tags",
            "windows",
            "mac",
            "linux",
            "metacritic_score",
            "recommendations",
            "positive_reviews",
            "negative_reviews",
            "estimated_owners",
            "avg_playtime_forever",
            "avg_playtime_2weeks",
            "median_playtime",
            "peak_ccu",
            "required_age",
            "dlc_count",
            "achievements",
            "short_description",
            "header_image",
            "platforms",
            "min_estimated_owners",
            "max_estimated_owners",
            "avg_estimated_owners",
            "peak_ccu_rank",
            "gold_ingest_date"
        ]
    ].itertuples(index = False):
        params = tuple(to_native(v) for v in row)
        hook.run(query, parameters = params)