import sys

from scripts.extract import run_extract
from scripts.s3_upload import upload_raw_data
from scripts.spark_job import WorldBankSparkJob
from utils.config_loader import load_config
from utils.logging_config import setup_logging

def run_pipeline() -> None:
    config = load_config()
    logger = setup_logging(config)
    
    try:
        logger.info('=' * 60)
        logger.info('Starting World Bank GDP Pipeline')
        logger.info('=' * 60)
        
        # Stage 1: Extract data from World Bank API
        logger.info('Stage 1: Extracting data from World Bank API')
        run_extract()

        # Stage 2: Upload raw JSON to S3
        logger.info('Stage 2: Uploading raw data to S3')
        upload_raw_data(config)

        # Stage 3: PySpark: clean, process, and create analytics
        job = WorldBankSparkJob()
        job.run()

        logger.info('=' * 60)
        logger.info('Pipeline completed successfully')
        logger.info('=' * 60)

    except Exception as e:
        logger.error(f'Pipeline failed: {e}')
        sys.exit(1)


if __name__ == '__main__':
    run_pipeline()