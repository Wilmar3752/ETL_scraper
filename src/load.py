import io
from datetime import date, timedelta

import boto3
import pandas as pd
from botocore.exceptions import ClientError
import logging


def upload_to_s3(file_name, bucket_name, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_stored_skus(source, days, bucket_name='scraper-meli'):
    """``sku`` values in the daily parquets of ``source`` from the last ``days`` days.

    Reads the keys by date instead of listing the prefix, so the Lambda only needs s3:GetObject on them.
    A missing day (no run, or nothing new that day) is skipped.
    """
    s3_client = boto3.client('s3')
    skus = set()
    for offset in range(days):
        key = f'carros/data_{date.today() - timedelta(days=offset)}_{source}.parquet'
        try:
            body = s3_client.get_object(Bucket=bucket_name, Key=key)['Body'].read()
        except ClientError as e:
            code = e.response['Error']['Code']
            if code not in ('NoSuchKey', 'AccessDenied'):
                raise
            if code == 'AccessDenied':   # also what a missing key returns without s3:ListBucket
                logging.warning(f"Cannot read s3://{bucket_name}/{key} (missing, or no s3:GetObject)")
            continue
        skus.update(pd.read_parquet(io.BytesIO(body), columns=['sku'])['sku'].dropna().astype(str))
    return sorted(skus)
