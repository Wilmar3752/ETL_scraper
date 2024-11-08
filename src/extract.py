import requests
import json

def get_data_from_api(num_search_pages: int = 1, product: str = 'Carros'):
    url = "http://0.0.0.0:7860/product"
    payload = {
    "product": product,
    "pages":num_search_pages
    }

    response = requests.post(url, data=json.dumps(payload))
    return response.json()

