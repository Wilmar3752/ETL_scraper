import logging
from dotenv import load_dotenv
load_dotenv()
from extract import get_data_from_api
from transform import transform_json_to_df
from load import upload_to_s3
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    now = datetime.now().date()
    raw_data = get_data_from_api(num_search_pages="all")
    transformed_data = transform_json_to_df(raw_data)
    transformed_data['_created'] = now
    file_name = 'data/raw/initial_load.csv'
    transformed_data.to_csv(file_name)
    upload_to_s3(file_name, bucket_name='scraper-meli', object_name=f'initial_load/data_{now}.csv')
    logging.info("Initial load completed successfully")

if __name__ == "__main__":
    main()