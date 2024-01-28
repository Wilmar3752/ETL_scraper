import requests
import json

def get_data_from_api(num_search_pages: int = 1):
    url = "http://localhost:8000/product"
    payload = {
    "product": "carro",
    "pages":1
    }

    response = requests.post(url, data=json.dumps(payload))
    return response.json()
