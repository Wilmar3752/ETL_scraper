import pandas as pd
import re
def transform_json_to_df(json):
    data = pd.DataFrame(json)
    data = data[data['product'].notna()]
    data = get_info_from_name(data)
    data['id'] = [extract_pub_number_from_link(url) for url in data['link']]
    data = clean_locations(data)
    return data

def get_info_from_name(data):
    product_serie = data['product'].str.split(' ')
    data['vehicle_make'] = product_serie.apply(lambda x: x[0])
    data['vehicle_line'] = product_serie.apply(lambda x: x[1])
    data['kilometraje'] = clean_mileage(data['kilometraje'])
    return data


def extract_pub_number_from_link(url):
    match = re.search('MCO-(\d+)', url)
    if match:
        return match.group(1)
    else:
        return None
    
def clean_mileage(col: pd.Series):
    mlieage_clean = col.str.replace('.','').str.split(' ').apply(lambda x: x[0]).astype(int)
    return mlieage_clean
    
def clean_locations(df):
    print("LLEGA")
    df['location_city'] = df['locations'].str.split('-').str[0]
    print('LLEGA')
    print(df['location_city'])
    df['location_state'] = df['locations'].str.split('-').str[1]
    df.drop(columns = 'locations', inplace=True)
    return df