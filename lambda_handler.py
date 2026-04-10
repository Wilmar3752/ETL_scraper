import sys
import os
import logging

sys.path.insert(0, os.path.join(os.environ.get("LAMBDA_TASK_ROOT", ""), "src"))

from main import main, main_carroya, main_usados_renting

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

    try:
        main_carroya()
        results["carroya"] = "success"
        logger.info("ETL completed for carroya")
    except Exception as e:
        results["carroya"] = f"error: {str(e)}"
        logger.error(f"ETL failed for carroya: {str(e)}")

    try:
        main_usados_renting()
        results["usados_renting"] = "success"
        logger.info("ETL completed for usados_renting")
    except Exception as e:
        results["usados_renting"] = f"error: {str(e)}"
        logger.error(f"ETL failed for usados_renting: {str(e)}")

    has_errors = any(v.startswith("error") for v in results.values())

    return {
        "statusCode": 500 if has_errors else 200,
        "body": results,
    }
