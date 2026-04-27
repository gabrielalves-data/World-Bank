import requests
import json
import logging
import os
from datetime import date
from utils.config_loader import load_config
from utils.logging_config import setup_logging

# API Call (single page)
def fetch_page(base_url: str, indicator: str, page: int, per_page: int, fmt: str) -> tuple:
    """
    Fetch a single page of data from the World Bank API.

    Parameters
    ----------
    base_url : str
        Base URL of the World Bank API.
    indicator : str
        Indicator code (e.g., GDP indicator).
    page : int
        Page number to retrieve.
    per_page : int
        Number of records per page.
    fmt : str
        Response format (e.g., "json").

    Returns
    -------
    tuple
        A tuple containing:
        - metadata : dict
        - records : list

    Raises
    ------
    requests.HTTPError
        If the API request fails.

    Notes
    -----
    The API returns a two-element list:
    [metadata, records]
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
    Retrieve all records from the World Bank API using pagination.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing API settings.
    logger : logging.Logger
        Logger instance for logging progress.

    Returns
    -------
    list
        List of all records retrieved from the API.

    Notes
    -----
    This function:
    - Fetches the first page to determine total pages
    - Iterates through remaining pages
    - Aggregates results into a single list
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
    Save raw API records to a local JSON file.

    Parameters
    ----------
    records : list
        List of records to save.
    logger : logging.Logger
        Logger instance for logging progress.

    Returns
    -------
    str
        File path of the saved JSON file.

    Notes
    -----
    The file is saved in the 'logs/' directory with the format:
    raw_YYYY-MM-DD.json
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
    Execute the full data extraction pipeline.

    Returns
    -------
    str
        Path to the saved raw JSON file.

    Notes
    -----
    This function:
    - Loads configuration
    - Sets up logging
    - Fetches all API records
    - Saves results locally

    Designed to be used as:
    - A standalone script
    - A callable function in orchestration tools (e.g., Airflow)
    """
    
    config = load_config()
    logger = setup_logging(config)

    records = fetch_all_records(config, logger)
    filepath = save_raw_json(records, logger)

    return filepath

if __name__ == "__main__":
    path = run_extract()
    print(f"\nDone. File saved at: {path}")