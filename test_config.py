# test_config.py

from scripts.config_loader import load_config, get_s3_path
from datetime import date

config = load_config()

# Print top-level keys
print("Keys loaded:", list(config.keys()))

# Print specific values
print("S3 bucket:", config["s3"]["bucket"])
print("API indicator:", config["api"]["indicator"])
print("Spark master:", config["spark"]["master"])

# Test path builder
today = date.today().strftime("%Y/%m/%d")

raw_path = get_s3_path(config, "raw", "data.json", today)
processed_path = get_s3_path(config, "processed", "data.parquet", today)
analytics_path = get_s3_path(config, "analytics", "avg_gdp.parquet")

print("\nGenerated S3 paths:")
print("  raw      →", raw_path)
print("  processed→", processed_path)
print("  analytics→", analytics_path)