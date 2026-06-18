#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
from dotenv import load_dotenv


# In[ ]:


load_dotenv()

storage_options = {
    "key": os.getenv("MINIO_ROOT_USER"),
    "secret": os.getenv("MINIO_ROOT_PASSWORD"),
    "client_kwargs": {
        "endpoint_url": f"http://{os.getenv('MINIO_ENDPOINT')}"
    }
}

bucket = os.getenv("MINIO_BUCKET")


# In[3]:


df = pd.read_csv(
    f"s3://{bucket}/raw/steam_top_games_2026.csv",
    storage_options = storage_options
)


# In[ ]:


display(df)


# In[5]:


df.to_parquet(
    f"s3://{bucket}/medallion/bronze/top_games_2026.parquet",
    storage_options = storage_options
)

