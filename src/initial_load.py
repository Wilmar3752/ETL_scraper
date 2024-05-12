from extract import get_data_from_api
from transform import transform_json_to_df
from load import upload_to_s3
from datetime import datetime
now = datetime.now().date()

def main():
    raw_data = get_data_from_api(num_search_pages="all")
    transformed_data = transform_json_to_df(raw_data)
    transformed_data['_created'] = now 
    file_name = 'data/raw/initial_load.csv'
    transformed_data.to_csv(file_name)  
    upload_to_s3(transformed_data, bucket_name='scraper-meli', object_name=f'initial_load')

if __name__ == "__main__":
    main()