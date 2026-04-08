import logging
from dotenv import load_dotenv
load_dotenv(override=True)
from extract import get_data_from_api, get_carroya_data
from transform import transform_json_to_df, transform_carroya_to_df
from datetime import datetime
from load import upload_to_s3
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(product):
    try:
        raw_data = get_data_from_api(num_search_pages=3, product=product)
        transformed_data = transform_json_to_df(raw_data)
        now = datetime.now().date()
        transformed_data['_created'] = now
        transformed_data['source'] = 'meli'
        file_name = '/tmp/data.parquet'
        transformed_data.to_parquet(file_name, index=False)
        upload_to_s3(file_name, bucket_name='scraper-meli', object_name=f'carros/data_{now}_meli.parquet')
        logging.info(f"Data processed successfully for product: {product}")
    except Exception as e:
        logging.error(f"An error occurred while processing data for product: {product}. Error: {str(e)}")


def main_carroya():
    try:
        raw_data = get_carroya_data(num_search_pages=5)
        transformed_data = transform_carroya_to_df(raw_data)
        now = datetime.now().date()
        transformed_data['_created'] = now
        transformed_data['source'] = 'carroya'
        file_name = '/tmp/data_carroya.parquet'
        transformed_data.to_parquet(file_name, index=False)
        upload_to_s3(file_name, bucket_name='scraper-meli', object_name=f'carros/data_{now}_carroya.parquet')
        logging.info("Data processed successfully for carroya")
    except Exception as e:
        logging.error(f"An error occurred while processing carroya data. Error: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data for a specific product.")
    parser.add_argument("product", help="The product to process (e.g., Carros)")
    args = parser.parse_args()

    main(args.product)
