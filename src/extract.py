import requests
import json
import os

def get_data_from_api(num_search_pages: int = 1, product: str = 'carros', items: int = 50):
    url = f"{os.getenv('SCRAPER_API_URL')}/product"
    print("Making request to: ", url)
    payload = {
    "product": product,
    "pages":num_search_pages,
    "items":items
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": os.getenv("API_KEY")
    }

    response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=1800)
    print(f"Response status: {response.status_code}")
    print(f"Response body (first 500 chars): {response.text[:500]}")
    return response.json()

