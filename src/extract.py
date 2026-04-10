import requests
import json
import os

def _post(endpoint: str, payload: dict) -> list:
    url = f"{os.getenv('SCRAPER_API_URL')}{endpoint}"
    print("Making request to: ", url)
    headers = {
        "Content-Type": "application/json",
        "x-api-key": os.getenv("API_KEY")
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=1800)
    print(f"Response status: {response.status_code}")
    print(f"Response body (first 500 chars): {response.text[:500]}")
    return response.json()


def get_data_from_api(num_search_pages: int = 1, product: str = 'carros', items: int = 50):
    return _post("/meli/product", {"product": product, "pages": num_search_pages, "items": items})


def get_carroya_data(num_search_pages: int = 1, items: int = 50, start_page: int = 1):
    return _post("/carroya/vehiculos", {"pages": num_search_pages, "items": items, "start_page": start_page})


def get_usados_renting_data(num_search_pages: int = 1, items: int = 50, start_page: int = 1):
    return _post("/usados-renting/vehiculos", {"pages": num_search_pages, "items": items, "start_page": start_page})

