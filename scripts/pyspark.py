import logging
import sys
from typing import Dict, Any
from pathlib import Path
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import IntegerType, DoubleType

from utils.config_loader import load_config
from utils.logging_config import setup_logging
from scripts.s3_upload import get_s3_path


class WorlBankSparkJob:
    """PySpark job for processing World Bank GDP data."""

    def __init__(self, config_path: str = None):
        """
        Initialize the Spark job.
        
        Args:
            config_path: Path to configuration file (optional, auto-resolved if None)
        """

        self.config = load_config(config_path)
        self.logger = setup_logging(self.config)
        self.spark = self.create_spark_session()

    @staticmethod
    def convert_to_s3a(s3_path: str) -> str:
        """
        Convert s3:// URI to s3a:// for Spark compatibility.
        
        Args:
            s3_path: S3 path with s3:// protocol
            
        Returns:
            S3 path with s3a:// protocol
        """
        return s3_path.replace("s3://", "s3a://")
    

    def create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session for S3 access.
        
        Returns:
            Configured SparkSession
        """
        try:
            spark = (SparkSession.builder
                    .appName('WorldBankPipeline')
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                           "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .getOrCreate())
            
            self.logger.info("SparkSession created successfully")
            self.logger.info(f"Spark version: {spark.version}")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create SparkSession: {e}")
            raise

    def read_raw_data(self) -> DataFrame:
        """
        Read raw JSON data from S3.
        
        Returns:
            DataFrame containing raw data
        """
        base_path = get_s3_path(self.config, "raw")
        raw_path = f"{base_path}/year=*/month=*/day=*/data.json"

        # Convert to s3a:// protocol for Spark
        raw_path = self.convert_to_s3a(raw_path)

        try:
            self.logger.info(f"Reading raw data from: {raw_path}")

            df = (self.spark.read.option('multiline', 'true').json(raw_path))

            row_count = df.count()
            self.logger.info(f"Successfully read {row_count:,} rows from raw data")
            self.logger.info(f"Schema: {df.printSchema()}")

            return df
        
        except Exception as e:
            self.logger.error(f"Failed to read raw data: {e}")
            raise

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform raw data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        try:
            self.logger.info('Starting data cleaning...')

            # Select and rename columns with proper types
            cleaned_df = df.select(
                col('country.value').alias('country_name'),
                col('country.id').alias('country_iso3'),
                col('indicator.id').alias('indicator_id'),
                col('date').cast(IntegerType()).alias('year'),
                col('value').cast(DoubleType()).alias('gdp_usd')
            )

            # Drop rows with null GDP values
            cleaned_df = cleaned_df.filter(col('gdp_usd').isNotNull())

            row_count = cleaned_df.count()
            null_count = df.count() - row_count

            self.logger.info("Data cleaning complete:")
            self.logger.info(f"  - Rows after cleaning: {row_count:,}")
            self.logger.info(f"  - Rows with null GDP removed: {null_count:,}")

            self.logger.info("Sample of cleaned data:")
            cleaned_df.show(5, truncate=False)

            return cleaned_df
        
        except Exception as e:
            self.logger.error(f"Failed to clean data: {e}")
            raise


    def write_processed_data(self, df: DataFrame) -> None:
        """
        Write cleaned data to S3 as Parquet, partitioned by year.
        
        Args:
            df: Cleaned DataFrame
        """
        processed_path = get_s3_path(self.config, 'processed')
        processed_path = self.convert_to_s3a(processed_path)

        try:
            self.logger.info(f"Writing processed data to: {processed_path}")

            df.write.mode('overwrite').partitionBy('year').parquet(processed_path)

            self.logger.info(f"Successfully wrote processed data (partitioned by year)")

        except Exception as e:
            self.logger.error(f"Failed to write processed data: {e}")
            raise


    def create_analytics(self, df: DataFrame) -> DataFrame:
        """
        Create analytics aggregation: average GDP per country.
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            Analytics DataFrame
        """
        try:
            self.logger.info('Creating analytics layer...')

            analytics_df = (df.groupBy('country_name', 'country_iso3')
                            .agg(
                                avg('gdp_usd').alias('avg_gdp_usd'),
                                count('gdp_usd').alias('year_count')
                                )
                                .orderBy(col('avg_gdp_usd').desc()))
            
            row_count = analytics_df.count()
            self.logger.info("Analytics aggregation complete:")
            self.logger.info(f"  - Countries analyzed: {row_count:,}")

            # Show top 10 countries by average GDP
            self.logger.info("Top 10 countries by average GDP:")
            analytics_df.show(10, truncate=False)

            return analytics_df
        
        except Exception as e:
            self.logger.error(f"Failed to create analytics: {e}")
            raise


    def write_analytics_data(self, df: DataFrame) -> None:
        """
        Write analytics data to S3 as Parquet.
        
        Args:
            df: Analytics DataFrame
        """
        analytics_path = get_s3_path(self.config, 'analytics', 'avg_gdp')
        analytics_path = self.convert_to_s3a(analytics_path)

        try:
            self.logger.info(f"Writing analytics data to: {analytics_path}")

            df.write.mode('overwrite').parquet(analytics_path)

            self.logger.info('Successfully wrote analytics data')

        except Exception as e:
            self.logger.error(f"Failed to write analytics data: {e}")
            raise


    def run(self) -> None:
        """Execute the complete pipeline"""
        try:
            self.logger.info("="*80)
            self.logger.info("Starting World Bank GDP Data Pipeline - PySpark Job")
            self.logger.info("="*80)

            # Step 1: Read raw data
            raw_df = self.read_raw_data()

            # Step 2: Clean raw data
            cleaned_df = self.clean_data(raw_df)

            # Step 3: Write processed/cleaned data
            self.write_processed_data(cleaned_df)

            # Step 4: Create analytics data
            analytics_df = self.create_analytics(cleaned_df)

            # Step 5: Write analytics data
            self.write_analytics_data(analytics_df)

            self.logger.info("="*80)
            self.logger.info("Pipeline completed successfully")
            self.logger.info("="*80)

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise

        finally:
            self.spark.stop()
            self.logger.info('SparkSession stopped')


def main():
    """Main entry point for the Spark job."""
    try:
        job = WorlBankSparkJob()
        job.run()
        sys.exit(0)

    except Exception as e:
        logging.error(f"Job failed with error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()