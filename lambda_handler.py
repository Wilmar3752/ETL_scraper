import sys
import os
import logging

sys.path.insert(0, os.path.join(os.environ.get("LAMBDA_TASK_ROOT", ""), "src"))

from main import main, main_carroya, main_usados_renting, main_vendetunave

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def handler(event, context):
    source = event.get("source", "all")
    results = {}

    if source in ("meli", "all"):
        try:
            main("carros")
            results["meli"] = "success"
            logger.info("ETL completed for meli")
        except Exception as e:
            results["meli"] = f"error: {str(e)}"
            logger.error(f"ETL failed for meli: {str(e)}")

    if source in ("carroya", "all"):
        try:
            main_carroya()
            results["carroya"] = "success"
            logger.info("ETL completed for carroya")
        except Exception as e:
            results["carroya"] = f"error: {str(e)}"
            logger.error(f"ETL failed for carroya: {str(e)}")

    if source in ("usados_renting", "all"):
        try:
            main_usados_renting()
            results["usados_renting"] = "success"
            logger.info("ETL completed for usados_renting")
        except Exception as e:
            results["usados_renting"] = f"error: {str(e)}"
            logger.error(f"ETL failed for usados_renting: {str(e)}")

    if source in ("vendetunave", "all"):
        try:
            main_vendetunave()
            results["vendetunave"] = "success"
            logger.info("ETL completed for vendetunave")
        except Exception as e:
            results["vendetunave"] = f"error: {str(e)}"
            logger.error(f"ETL failed for vendetunave: {str(e)}")

    has_errors = any(v.startswith("error") for v in results.values())

    return {
        "statusCode": 500 if has_errors else 200,
        "body": results,
    }
