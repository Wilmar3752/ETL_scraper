"""
Migrates existing CSV files in S3 to Parquet format.
Reads each CSV, cleans newlines in text fields, saves as Parquet, uploads, and deletes the original CSV.
"""
import boto3
import pandas as pd
import io
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BUCKET = 'scraper-meli'
PREFIXES = ['carros/', 'initial_load/']

s3 = boto3.client('s3')


def list_csv_keys(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') or (not key.endswith('/') and '.' not in key.split('/')[-1]):
                keys.append(key)
    return keys


def migrate_key(csv_key):
    logging.info(f"Reading {csv_key}")
    response = s3.get_object(Bucket=BUCKET, Key=csv_key)
    df = pd.read_csv(io.BytesIO(response['Body'].read()))

    # Clean newlines in all string columns
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        df[col] = df[col].str.replace(r'\r?\n', ' ', regex=True)

    parquet_key = csv_key.rsplit('.', 1)[0] + '.parquet' if '.' in csv_key.split('/')[-1] else csv_key + '.parquet'

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    logging.info(f"Uploading {parquet_key}")
    s3.put_object(Bucket=BUCKET, Key=parquet_key, Body=buf.getvalue())

    logging.info(f"Deleting {csv_key}")
    s3.delete_object(Bucket=BUCKET, Key=csv_key)


if __name__ == '__main__':
    for prefix in PREFIXES:
        keys = list_csv_keys(prefix)
        logging.info(f"Found {len(keys)} files in {prefix}")
        for key in keys:
            migrate_key(key)

    logging.info("Migration complete.")
