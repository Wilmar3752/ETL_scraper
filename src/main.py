import logging
from extract import get_data_from_api
from transform import transform_json_to_df
from datetime import datetime

from extract import get_data_from_api
from transform import transform_json_to_df
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

now = datetime.now().date()

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

def main():
    raw_data = get_data_from_api(num_search_pages=1)
    transformed_data = transform_json_to_df(raw_data)
    transformed_data['_created'] = now 
    file_name = 'data/raw/data.csv'
    transformed_data.to_csv(file_name)  
    # Reemplaza 'your_bucket_name' con el nombre de tu bucket de S3
    upload_to_s3(transformed_data, bucket_name='scraper-meli', object_name=f'data_{now}')

if __name__ == "__main__":
    main()