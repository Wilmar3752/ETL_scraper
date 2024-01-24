import pandas as pd
import re
def transform_json_to_df(json):
    data = pd.DataFrame(json)
    data = data[data['product'].notna()]
    data = get_info_from_name(data)
    data['id'] = [extract_pub_number_from_link(url) for url in data['link']]

    return data

def get_info_from_name(data):
    product_serie = data['product'].str.split(' ')
    data['vehicle_make'] = product_serie.apply(lambda x: x[0])
    data['vehicle_line'] = product_serie.apply(lambda x: x[1])
    return data


def extract_pub_number_from_link(url):
    match = re.search('MCO-(\d+)', url)
    if match:
        return match.group(1)
    else:
        return None