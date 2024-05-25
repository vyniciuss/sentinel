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


def load_files(file_path: str, spark: SparkSession = None) -> dict:
    """
    Reads a text file from the specified path using Spark, concatenates its content,
    and parses it as JSON.

    Args:
        file_path (str): The path to the text file to be read.
        spark (SparkSession, optional): An existing Spark session. Defaults to None.

    Returns:
        dict: The parsed JSON content of the file.

    Example:
        spark = SparkSession.builder.appName("example").getOrCreate()
        result = load_files("/path/to/file.txt", spark)
    """
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
    """
    Adds parameters with default values and their corresponding kwargs values to a table.

    Args:
        func (Any): The function whose parameters are to be displayed.
        **kwargs (Any): The values for the function's parameters.

    Example:
        def example_function(a, b=1, c=2):
            pass

        add_params_to_table(example_function, b=10, c=20)
    """
    table = Table()

    sig = signature(func)
    for name, param in sig.parameters.items():
        if param.default != Parameter.empty:
            table.add_column(name)

    table.add_row(*[str(kwargs[name]) for name in kwargs])

    console.print(table)
