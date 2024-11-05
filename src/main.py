import logging
from extract import get_data_from_api
from transform import transform_json_to_df
from datetime import datetime
from load import upload_to_s3

now = datetime.now().date()

def main():
    raw_data = get_data_from_api(num_search_pages=3)
    transformed_data = transform_json_to_df(raw_data)
    transformed_data['_created'] = now  
    file_name = 'data/raw/data.csv'
    transformed_data.to_csv(file_name)   
    upload_to_s3(file_name, bucket_name='scraper-meli', object_name=f'data_{now}.csv')

if __name__ == "__main__":
    main()
