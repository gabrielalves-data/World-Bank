import yaml
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def load_config(config_path: str = None) -> dict:
    """
    Load a YAML configuration file into a dictionary.

    Parameters
    ----------
    config_path : str, optional
        Path to the configuration file. If None, the function automatically
        resolves the path relative to the project root.

    Returns
    -------
    dict
        Parsed configuration dictionary.

    Raises
    ------
    FileNotFoundError
        If the configuration file does not exist.
    yaml.YAMLError
        If the YAML file is malformed.

    Notes
    -----
    The default path resolution assumes the following structure:
    project_root/
        config/
            config.yaml
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
    """
    Construct a fully qualified S3 path for a given data layer.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing S3 settings.
    layer : str
        Data layer name. Must correspond to a key in config["s3"],
        e.g., "raw", "processed", or "analytics".
    filename : str, optional
        File name to append to the path (e.g., "data.json").
    date_partition : str, optional
        Date string in the format "YYYY/MM/DD" used to create
        partitioned folders.

    Returns
    -------
    str
        Fully constructed S3 URI.

    Raises
    ------
    ValueError
        If date_partition is not in the expected format.

    Examples
    --------
    >>> get_s3_path(config, "raw", "data.json", "2024/03/30")
    's3://bucket/raw/world_bank/year=2024/month=03/day=30/data.json'

    Notes
    -----
    Uses Hive-style partitioning (year=YYYY/month=MM/day=DD),
    which is compatible with Spark and AWS Athena.
    """

    bucket = config["s3"]["bucket"]
    prefix = config["s3"][f"{layer}_prefix"]

    if date_partition:
        dt = datetime.strptime(date_partition, "%Y/%m/%d")

        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        path = f"s3://{bucket}/{prefix}/year={year}/month={month}/day={day}"
    
    else:
        path = f"s3://{bucket}/{prefix}"

    if filename:
        path = f"{path}/{filename}"

    return path
