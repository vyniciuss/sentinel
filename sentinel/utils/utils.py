"""
Utility functions for the project.
"""
import json
from inspect import Parameter, signature
from typing import Any

from pyspark.sql import SparkSession
from rich.console import Console
from rich.table import Table

from sentinel.config.logging_config import logger
from sentinel.models import Config

console = Console()


def load_files(file_path, spark: SparkSession = None):
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
    if data:
        logger.info(f'--- File {file_path} was found! ---')

    config = Config(**data)
    return config


def add_params_to_table(func: Any, **kwargs: Any) -> None:
    table = Table()

    sig = signature(func)
    for name, param in sig.parameters.items():
        if param.default != Parameter.empty:
            table.add_column(name)

    table.add_row(*[str(kwargs[name]) for name in kwargs])

    console.print(table)
