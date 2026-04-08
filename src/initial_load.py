import logging
import io
import boto3
from dotenv import load_dotenv
load_dotenv(override=True)
from extract import get_data_from_api, get_carroya_data
from transform import transform_json_to_df, transform_carroya_to_df
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

def main(source="all"):
    now = datetime.now().date()

    if source in ("meli", "all"):
        logging.info("Extracting MercadoLibre...")
        meli_raw = get_data_from_api(num_search_pages="all", product="carros")
        load_source(meli_raw, transform_json_to_df, "meli", now)

    if source in ("carroya", "all"):
        s3 = boto3.client('s3')
        page = 1
        while True:
            logging.info(f"Extracting Carroya page {page}...")
            raw = get_carroya_data(num_search_pages=1, start_page=page)
            if not raw:
                logging.info("No more Carroya pages.")
                break

            slug = f"carroya_p{page}"
            s3_key = f"initial_load/data_{now}_{slug}.parquet"
            load_source(raw, transform_carroya_to_df, slug, now)

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

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["meli", "carroya", "all"], default="all")
    args = parser.parse_args()
    main(args.source)
