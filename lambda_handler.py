import sys
import os
import logging

sys.path.insert(0, os.path.join(os.environ.get("LAMBDA_TASK_ROOT", ""), "src"))

from main import main

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def handler(event, context):
    products = ["carros"]
    results = {}

    for product in products:
        try:
            main(product)
            results[product] = "success"
            logger.info(f"ETL completed for {product}")
        except Exception as e:
            results[product] = f"error: {str(e)}"
            logger.error(f"ETL failed for {product}: {str(e)}")

    has_errors = any(v.startswith("error") for v in results.values())

    return {
        "statusCode": 500 if has_errors else 200,
        "body": results,
    }
