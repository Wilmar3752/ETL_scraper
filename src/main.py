import logging
from extract import get_data_from_api
from transform import transform_json_to_df
from datetime import datetime
from load import upload_to_s3
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(product):
    try:
        raw_data = get_data_from_api(num_search_pages=1, product=product)
        transformed_data = transform_json_to_df(raw_data)
        now = datetime.now().date()
        transformed_data['_created'] = now  
        file_name = 'data/raw/data.csv'
        transformed_data.to_csv(file_name)   
        upload_to_s3(file_name, bucket_name=f'scraper-meli', object_name=f'{product}/data_{now}.csv')
        logging.info(f"Data processed successfully for product: {product}")
    except Exception as e:
        logging.error(f"An error occurred while processing data for product: {product}. Error: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data for a specific product.")
    parser.add_argument("product", help="The product to process (e.g., Carros)")
    args = parser.parse_args()

    main(args.product)
