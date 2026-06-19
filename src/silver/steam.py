import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def transform_silver_games():

    storage_options = {
        "key": os.getenv("MINIO_ROOT_USER"),
        "secret": os.getenv("MINIO_ROOT_PASSWORD"),
        "client_kwargs": {
            "endpoint_url": f"http://{os.getenv('MINIO_ENDPOINT')}"
        }
    }

    bucket = os.getenv("MINIO_BUCKET")

    df = pd.read_parquet(
        f"s3://{bucket}/medallion/bronze/top_games_2026.parquet",
        storage_options = storage_options
    )

    df["release_date"] = pd.to_datetime(df["release_date"], format = "%b %d, %Y", errors = "coerce")

    mask = (df["price_usd"] == 0) & (df["is_free"] == False)
    df.loc[mask, "is_free"] = True

    mask = (df["price_usd"] != 0) & (df["is_free"] == True)
    df.loc[mask, "is_free"] = False

    df["discount_pct"] = df["discount_pct"] / 100

    df["developer"] = df["developer"].fillna("").astype(str).str.strip()
    df.loc[df["developer"] == "", "developer"] = "Unknown"

    df["publisher"] = df["publisher"].fillna("").astype(str).str.strip()
    df.loc[df["publisher"] == "", "publisher"] = "Unknown"

    df["genres"] = df["genres"].fillna("").astype(str).str.strip()
    df.loc[df["genres"] == "", "genres"] = "Unknown"

    df["categories"] = df["categories"].fillna("").astype(str).str.strip()
    df.loc[df["categories"] == "", "categories"] = "Unknown"

    df["tags"] = df["tags"].fillna("").astype(str).str.strip()
    df.loc[df["tags"] == "", "tags"] = "Unknown"

    df = df.rename(columns = {
        "platforms_win": "windows",
        "platforms_mac": "mac",
        "platforms_linux": "linux"
    })

    df["platforms"] = (
        df[["windows", "mac", "linux"]]
        .rename(columns = {
            "windows": "Windows",
            "mac": "Mac",
            "linux": "Linux"
        }).apply(lambda r: ", ".join(r.index[r.values]) if r.any() else "Unknown", axis = 1)
    )

    df[["min_estimated_owners", "max_estimated_owners"]] = (
        df["estimated_owners"].str.split(" .. ", expand = True)
    )

    df["min_estimated_owners"] = (
        df["min_estimated_owners"].str.replace(",", "", regex = False) \
            .astype("Int64")
    )

    df["max_estimated_owners"] = (
        df["max_estimated_owners"].str.replace(",", "", regex = False) \
            .astype("Int64")
    )

    df["avg_estimated_owners"] = (
        df["min_estimated_owners"] + df["max_estimated_owners"]
    ) / 2

    df.to_parquet(
        f"s3://{bucket}/medallion/silver/top_games_2026.parquet",
        storage_options = storage_options
    )