import sys

from pyspark.sql import SparkSession
from typer import Argument, Typer

from sentinel.config.logging_config import logger
from sentinel.data_quality.validator import (
    create_expectation_suite,
    validate_data,
)
from sentinel.utils.utils import add_params_to_table, read_config_file

app = Typer()


@app.command()
def main(
    jsonpath: str = Argument(help='Path do json de configuracao'),
    source_table_name: str = Argument(
        help='The name of the table that will have the validated data'
    ),
    target_table_name: str = Argument(
        help='The name of the table where the validation results will be saved.'
    ),
):
    try:
        add_params_to_table(
            main,
            jsonpath=jsonpath,
            table_name=source_table_name,
            target_table_name=target_table_name,
        )
        spark = SparkSession.builder.getOrCreate()
        config = read_config_file(jsonpath, spark)
        expectation_suite = create_expectation_suite(config.expectations)
        spark_df = spark.table(source_table_name)

        result_df, success = validate_data(
            spark, spark_df, expectation_suite, source_table_name
        )

        result_df.write.format('delta').mode('append').saveAsTable(
            target_table_name
        )

        if success:
            logger.info('Validation succeeded!')
            sys.exit(0)
        else:
            logger.error(
                f'An error occurred during execution! Please consult '
                f'table {target_table_name} for more information.'
            )
            sys.exit(1)
    except Exception as e:
        logger.error(
            f'An error occurred during execution! Please consult '
            f'table {target_table_name} for more information.'
        )
        logger.info(e)
        sys.exit(1)
