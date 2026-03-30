import requests
import json
import logging
import os
from datetime import date
from config_loader import load_config

# Logging Setup
def setup_logging(config: dict) -> logging.Logger:
    logging.basicConfig(
        level=config["logging"]["level"],
        format=config["logging"]["format"]
    )

    return logging.getLogger(__name__)

# API Call (single page)
def fetch_page(base_url: str, indicator: str, page: int, per_page: int, fmt: str) -> tuple:
    """
    Fetch one page from the World Bank API.
    Returns (metadata_dict, records_list).
    Raises an exception if the request fails.
    """
    url = f"{base_url}/country/all/indicator/{indicator}"
    params = {
        "format": fmt,
        "per_page": per_page,
        "page": page
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()

    metadata = data[0]
    records = data[1] if data[1] is not None else []

    return metadata, records


# API Call Paginated Fetch (all pages)
def fetch_all_records(config: dict, logger: logging.Logger) -> list:
    """
    Iterate over every page of the API and return a flat list of all records.
    """
    base_url = config["api"]["base_url"]
    indicator = config["api"]["indicator"]
    per_page = config["api"]["per_page"]
    fmt = config["api"]["format"]

    logger.info(f"Starting extraction - indicator: {indicator}")

    metadata, records = fetch_page(base_url, indicator, page=1, per_page=per_page, fmt=fmt)

    total_pages = metadata["pages"]
    total_records = metadata["total"]

    logger.info(f"Total records: {total_records} across {total_pages} pages")

    all_records = list(records)

    for page in range(2, total_pages + 1):
        logger.info(f"Fetching page {page} of {total_pages}...")
        _, page_records = fetch_page(base_url, indicator, page, per_page, fmt)
        all_records.extend(page_records)

    logger.info(f"Extraction complete - {len(all_records)} total records fetched")

    return all_records


# Save JSON Locally
def save_raw_json(records: list, logger: logging.Logger) -> str:
    """
    Save the raw records to logs/ with today's date in the filename.
    Returns the local file path so then it can be used in s3_upload.
    """
    today = date.today().strftime("%Y-%m-%d")
    filename = f"raw_{today}.json"

    os.makedirs("logs", exist_ok=True)
    filepath = os.path.join("logs", filename)

    with open(filepath, "w") as f:
        json.dump(records, f, indent=2)

    logger.info(f"Raw data saved locally -> {filepath}")

    return filepath


# Main Entrypoint
def run_extract() -> str:
    """
    Full extraction flow. Called directly or by Airflow.
    Returns the local path of the saved JSON file.
    """
    config = load_config()
    logger = setup_logging(config)

    records = fetch_all_records(config, logger)
    filepath = save_raw_json(records, logger)

    return filepath

if __name__ == "__main__":
    path = run_extract()
    print(f"\nDone. File saved at: {path}")