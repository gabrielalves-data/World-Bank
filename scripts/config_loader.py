import yaml
import os
import logging

logger = logging.getLogger(__name__)

def load_config(config_path: str = None) -> dict:
    """
    Load config.yaml and return it as a dict.
    Automatically finds the config file relative to this script's location.
    """

    if config_path is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, "config", "config.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logger.info(f"Config loaded from {config_path}")

    return config


def get_s3_path(config: dict, layer: str, filename: str = "", date_partition: str = None, ) -> str:
    """"
    Build a full S3 path for a given layer (raw, processed, analytics).

    Examples:
        get_s3_path(config, "raw", "data.json", "2024/03/30")
        → s3://your-bucket/raw/world_bank/year=2024/month=03/day=30/data.json
    """

    bucket = config["s3"]["bucket"]
    prefix = config["s3"][f"{layer}_prefix"]

    if date_partition:
        year, month, day = date_partition.split("/")
        path = f"s3://{bucket}/{prefix}/year={year}/month={month}/day={day}"
    
    else:
        path = f"s3://{bucket}/{prefix}"

    if filename:
        path = f"{path}/{filename}"

    return path