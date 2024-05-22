# utils.py
"""
Utility functions for the project.
"""
import json

from pyspark.sql import SparkSession

from sentinel.models import Config


def load_config(file_path: str, spark: SparkSession = None) -> Config:
    """
    Load configuration from a JSON file in DBFS.

    Args:
        file_path (str): Path to the JSON file in DBFS.

    Returns:
        Config: The loaded configuration object.
        :param file_path:
        :param spark:
    """
    if not spark:
        spark = SparkSession.builder.getOrCreate()
    df = spark.read.text(file_path)
    json_str = ''.join([row.value for row in df.collect()])
    data = json.loads(json_str)
    config = Config(**data)
    return config
