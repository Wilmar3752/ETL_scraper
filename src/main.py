import logging
from extract import get_data_from_api
from transform import transform_json_to_df
from datetime import datetime
from load import upload_to_s3

now = datetime.now().date()

<<<<<<< HEAD
=======
def upload_to_s3(file_name, bucket_name, object_name=None):
    """Upload a file to an S3 bucket using boto3 resource interface

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    if object_name is None:
        object_name = file_name

    s3 = boto3.resource('s3')
    try:
        s3.Bucket(bucket_name).upload_file(file_name, object_name)
        logging.info(f"File {file_name} uploaded to S3 bucket {bucket_name} as {object_name}")
        return True
    except ClientError as e:
        logging.error(f"Error uploading file {file_name} to S3: {e}")
        return False
>>>>>>> 9016dbdb026e8220c06cedc03caa82da5f698bb0

def main():
    raw_data = get_data_from_api(num_search_pages=1)
    transformed_data = transform_json_to_df(raw_data)
    transformed_data['_created'] = now  
    file_name = 'data/raw/data.csv'
    transformed_data.to_csv(file_name)   
    upload_to_s3(file_name, bucket_name='scraper-meli', object_name=f'data_{now}.csv')

if __name__ == "__main__":
    main()
