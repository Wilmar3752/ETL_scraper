import logging
import io
import boto3
from dotenv import load_dotenv
load_dotenv(override=True)
from extract import get_data_from_api, get_carroya_data, get_usados_renting_data, get_vendetunave_data
from transform import transform_json_to_df, transform_carroya_to_df, transform_usados_renting_to_df, transform_vendetunave_to_df
from load import upload_to_s3
from datetime import datetime
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_source(raw_data, transform_fn, slug, now):
    transformed = transform_fn(raw_data)
    transformed['_created'] = now
    transformed['source'] = slug
    file_name = f'data/raw/initial_load_{slug}.parquet'
    transformed.to_parquet(file_name, index=False)
    upload_to_s3(file_name, bucket_name='scraper-meli',
                 object_name=f'initial_load/data_{now}_{slug}.parquet')
    logging.info(f"Initial load completed for {slug}: {len(transformed)} records")

def _paginated_load(s3, get_fn, transform_fn, source_prefix, now, start_page=1):
    """Generic page-by-page load with S3 verification. Stops when API returns empty."""
    page = start_page
    while True:
        logging.info(f"Extracting {source_prefix} page {page}...")
        raw = get_fn(num_search_pages=1, start_page=page)
        if not raw:
            logging.info(f"No more {source_prefix} pages.")
            break

        slug = f"{source_prefix}_p{page}"
        s3_key = f"initial_load/data_{now}_{slug}.parquet"
        load_source(raw, transform_fn, slug, now)

        try:
            obj = s3.get_object(Bucket='scraper-meli', Key=s3_key)
            records = len(pd.read_parquet(io.BytesIO(obj['Body'].read())))
            if records == 0:
                logging.error(f"Page {page}: S3 file has 0 records. Stopping.")
                break
            logging.info(f"Page {page} verified in S3: {records} records OK.")
        except Exception as e:
            logging.error(f"Page {page}: S3 verification failed: {e}. Stopping.")
            break

        page += 1


def main(source="all", carroya_start_page=1, usados_renting_start_page=1, vendetunave_start_page=1):
    now = datetime.now().date()

    if source in ("meli", "all"):
        logging.info("Extracting MercadoLibre...")
        meli_raw = get_data_from_api(num_search_pages="all", product="carros")
        load_source(meli_raw, transform_json_to_df, "meli", now)

    if source in ("carroya", "all"):
        s3 = boto3.client('s3')
        _paginated_load(s3, get_carroya_data, transform_carroya_to_df, "carroya", now, start_page=carroya_start_page)

    if source in ("usados_renting", "all"):
        s3 = boto3.client('s3')
        _paginated_load(s3, get_usados_renting_data, transform_usados_renting_to_df, "usados_renting", now, start_page=usados_renting_start_page)

    if source in ("vendetunave", "all"):
        s3 = boto3.client('s3')
        _paginated_load(s3, get_vendetunave_data, transform_vendetunave_to_df, "vendetunave", now, start_page=vendetunave_start_page)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["meli", "carroya", "usados_renting", "vendetunave", "all"], default="all")
    parser.add_argument("--carroya-start-page", type=int, default=1)
    parser.add_argument("--usados-renting-start-page", type=int, default=1)
    parser.add_argument("--vendetunave-start-page", type=int, default=1)
    args = parser.parse_args()
    main(args.source, carroya_start_page=args.carroya_start_page, usados_renting_start_page=args.usados_renting_start_page, vendetunave_start_page=args.vendetunave_start_page)
