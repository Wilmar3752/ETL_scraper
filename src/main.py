from extract import get_data_from_api
from transform import transform_json_to_df
from datetime import datetime

now = datetime.now().date()
def main():
    raw_data = get_data_from_api(num_search_pages=1)
    transformed_data = transform_json_to_df(raw_data)
    return transformed_data

if __name__ == "__main__":
    data = main()
    data['_created'] = now
    data.to_csv(f'data/raw/data_{now}.csv')