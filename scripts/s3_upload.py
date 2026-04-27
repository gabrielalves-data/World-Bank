import boto3
import logging
import yaml
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from utils.logging_config import setup_logging
from utils.config_loader import load_config, get_s3_path


def split_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Split a full S3 URI into bucket and key.

    Parameters
    ----------
    s3_path : str
        Full S3 path (e.g., "s3://bucket/path/to/file.json").

    Returns
    -------
    tuple of str
        Bucket name and object key.

    Raises
    ------
    ValueError
        If the provided path is not a valid S3 URI.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    
    path = s3_path.replace("s3://", "")
    bucket, key = path.split("/", 1)

    return bucket, key


def upload_to_s3(local_path: str, s3_key: str, bucket_name: str, logger: logging.Logger) -> bool:
    """
    Upload a local file to an S3 bucket.

    Parameters
    ----------
    local_path : str
        Path to the local file.
    s3_key : str
        S3 object key (path inside the bucket).
    bucket_name : str
        Target S3 bucket name.
    logger : logging.Logger
        Logger instance for logging messages.

    Returns
    -------
    bool
        True if upload succeeds.

    Raises
    ------
    FileNotFoundError
        If the local file does not exist.
    NoCredentialsError
        If AWS credentials are not configured.
    PartialCredentialsError
        If AWS credentials are incomplete.
    ClientError
        If an AWS-related error occurs.
    Exception
        For any unexpected errors.
    """
    try:
        local_file = Path(local_path)

        if not local_file.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        file_size_mb = local_file.stat().st_size / (1024 * 1024)

        logger.info(f"Starting upload: {local_path} ({file_size_mb: .2f} MB)")
        logger.info(f"Destination: s3://{bucket_name}/{s3_key}")

        s3_client = boto3.client("s3")

        s3_client.upload_file(
            Filename=str(local_file),
            Bucket=bucket_name,
            Key=s3_key
        )

        logger.info(f"Upload successful")
        logger.info(f"S3 URL: {f"s3://{bucket_name}/{s3_key}"}")

        return True
    
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise

    except NoCredentialsError:
        logger.error("AWS credentials not found. Please configure with 'aws configure'")
        raise

    except PartialCredentialsError:
        logger.error("Incomplete AWS credentials. Please check your configuration")
        raise
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchBucket':
            logger.error(f"S3 bucket does not exist: {bucket_name}")
        
        elif error_code == 'AccessDenied':
            logger.error(f"Access denied to S3 bucket: {bucket_name}")
        
        else:
            logger.error(f"AWS error: {e}")
        
        raise
    
    except Exception as e:
        logger.exception(f"Unexpected error during upload")
        raise


def upload_raw_data(config: dict) -> None:
    """
    Upload raw World Bank data to S3 using date-based partitioning.

    Parameters
    ----------
    config : dict
        Configuration dictionary loaded from YAML.

    Returns
    -------
    None

    Notes
    -----
    The function:
    - Builds a date-partitioned S3 path
    - Uploads the corresponding local raw file
    - Logs progress and errors
    """
    logger = setup_logging(config)
    today = datetime.now()
    date_partition = today.strftime("%Y/%m/%d")

    local_path = f"logs/raw_{today.strftime('%Y-%m-%d')}.json"

    full_s3_path = get_s3_path(
        config,
        layer="raw",
        filename="data.json",
        date_partition=date_partition
    )

    bucket, key = split_s3_path(full_s3_path)

    upload_to_s3(local_path, key, bucket, logger)

    logger.info("Upload completed successfully")


def main() -> None:
    """
    Entry point for local execution.

    Loads configuration and triggers raw data upload.
    """
    config = load_config()
    upload_raw_data(config)


if __name__ == "__main__":
    main()