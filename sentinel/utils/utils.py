"""
Utility functions for the project.
"""
import json

from pyspark.sql import SparkSession

from sentinel.models import Config


def load_files(file_path, spark: SparkSession = None):
    if not spark:
        spark = SparkSession.builder.getOrCreate()
    df = spark.read.text(file_path)
    json_str = ''.join([row.value for row in df.collect()])
    return json.loads(json_str)


def read_config_file(file_path: str, spark: SparkSession = None) -> Config:
    """
    Load configuration from a JSON file in DBFS.

    Args:
        file_path (str): Path to the JSON file in DBFS.

    Returns:
        Config: The loaded configuration object.
        :param file_path:
        :param spark:
    """
    data = load_files(file_path, spark)
    config = Config(**data)
    return config
