# bronze_dims.py
# Convierte los CSV de:
#   raw/providers-comission/
#   raw/clients-revenue-share/
# a Parquet en bronze/

# Glue Python Shell 4.0 (Python 3.9)
# “Python extra modules”:  pandas,pyarrow==15.0.2

import boto3, io, pandas as pd, pyarrow as pa, pyarrow.parquet as pq

BUCKET = "tapi-challenge"                     # cambialo si tu bucket es otro
SRC_PREFIXES = [
    "raw/providers-comission/",
    "raw/clients-revenue-share/",
]
DST_ROOT = "processed/"

s3 = boto3.client("s3")
paginator = s3.get_paginator("list_objects_v2")

def csv_to_parquet(key: str):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    df = pd.read_csv(io.BytesIO(body), sep=";")
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buffer)
    dest_key = key.replace("raw/", DST_ROOT).replace(".csv", ".parquet")
    s3.put_object(Bucket=BUCKET, Key=dest_key, Body=buffer.getvalue())
    print(f"✓ {dest_key}")

for prefix in SRC_PREFIXES:
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".csv"):
                csv_to_parquet(obj["Key"])

print("🚀  Dimensiones convertidas a Parquet.")
