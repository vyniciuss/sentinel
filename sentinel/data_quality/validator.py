from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from sentinel.config.logging_config import logger
from sentinel.data_quality.expectations_validator import (
    create_expectation_suite,
    validate_custom_expectations,
    validate_great_expectations,
)
from sentinel.data_quality.metrics_validator import generate_and_save_metrics
from sentinel.exception.ValidationError import ValidationError
from sentinel.models import Config
from sentinel.utils.utils import read_config_file


def evaluate_all(
    json_path: str,
    source_table_name: str,
    target_table: str,
    process_type: str,
    source_config_name: str,
    checkpoint: Optional[str],
    metric_set_name: Optional[str] = None,
    spark: Optional[SparkSession] = None,
):
    """
    Executes data validation according to the provided configuration.

    Args:
        json_path (str): Path to the configuration JSON file.
        source_table_name (str): Name of the source table to be validated.
        target_table (str): Name of the table where validation results will be saved.
        process_type (str): Type of the process, typically "batch" or "streaming".
        source_config_name (str): The name of the source configuration to use.
        checkpoint (Optional[str]): Path to the checkpoint directory for Spark.
        spark (Optional[SparkSession]): An optional Spark session. If not provided, a new Spark session will be created.

    Raises:
        ValidationError: If the validation process encounters any errors.

    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    config = read_config_file(json_path, spark)

    if 'batch' in process_type:
        logger.info(f'Batch reader created {source_table_name}')
        df = spark.table(source_table_name)
        validate_data(
            spark, config, source_table_name, target_table, df, metric_set_name
        )
    else:
        logger.info(f'Streaming reader created {source_table_name}')
        source_config = config.find_source_config(source_config_name)
        (
            spark.readStream.format('delta')
            .options(**source_config.read_options)
            .table(source_table_name)
            .writeStream.foreachBatch(
                create_process_batch(
                    spark,
                    config,
                    source_table_name,
                    target_table,
                    metric_set_name,
                )
            )
            .option('checkpointLocation', checkpoint)
            .start()
        )


def create_process_batch(
    spark: SparkSession,
    config: Config,
    source_table_name: str,
    target_table: str,
    metric_set_name: Optional[str],
):
    def process_batch(batch_df, batch_id):
        logger.info(f'Processing batch {batch_id}')
        spark.sparkContext.setJobDescription(f'Processing batch {batch_id}')
        validate_data(
            spark,
            config,
            source_table_name,
            target_table,
            batch_df,
            metric_set_name,
        )

    return process_batch


def validate_data(
    spark: SparkSession,
    config: Config,
    source_table_name: str,
    target_table: str,
    dataframe: DataFrame,
    metric_set_name: Optional[str],
) -> bool:
    """
    Performs data validation using both Great Expectations and custom validations.

    Args:
        spark (SparkSession): The Spark session.
        config (Config): Configuration object containing data quality settings.
        source_table_name (str): Name of the source table to be validated.
        target_table (str): Name of the table where validation results will be saved.
        dataframe (DataFrame): The Spark DataFrame to be validated.

    Raises:
        ValidationError: If any validation fails.

    """
    logger.info('Starting validation')

    expectation_suite = create_expectation_suite(
        config.data_quality.great_expectations
    )

    result_df, ge_success = validate_great_expectations(
        spark, dataframe, expectation_suite, source_table_name
    )

    custom_results_df, custom_success = validate_custom_expectations(
        spark, config.data_quality.custom_expectations, source_table_name
    )

    final_result_df = result_df
    if custom_results_df is not None:
        final_result_df = result_df.union(custom_results_df)

    final_result_df.write.format('delta').mode('append').saveAsTable(
        target_table
    )

    generate_and_save_metrics(
        config.data_quality,
        source_table_name,
        spark,
        target_table,
        metric_set_name,
    )

    if not ge_success or not custom_success:
        raise ValidationError(
            f'An error occurred during execution! Please consult '
            f'table {target_table} for more information.'
        )
