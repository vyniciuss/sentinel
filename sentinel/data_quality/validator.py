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
    custom_expectation_name: Optional[str] = None,
    expectation_name: Optional[str] = None,
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
            spark,
            config,
            source_table_name,
            target_table,
            df,
            metric_set_name,
            custom_expectation_name,
            expectation_name,
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
                    custom_expectation_name,
                    expectation_name,
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
    custom_expectation_name: Optional[str],
    expectation_name: Optional[str],
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
            custom_expectation_name,
            expectation_name,
        )

    return process_batch


def validate_data(
    spark: SparkSession,
    config: Config,
    source_table_name: str,
    target_table: str,
    dataframe: DataFrame,
    metric_set_name: Optional[str],
    custom_expectation_name: Optional[str] = None,
    expectation_group_name: Optional[str] = None,
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

    expectation_suite = get_ge_expectations_to_process(
        config, expectation_group_name
    )

    result_df, ge_success = validate_great_expectations(
        spark, dataframe, expectation_suite, source_table_name
    )

    custom_expectations = get_custom_expectation_to_process(
        config, custom_expectation_name
    )

    custom_results_df, custom_success = validate_custom_expectations(
        spark, custom_expectations, source_table_name
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


def get_ge_expectations_to_process(config, expectation_group_name):
    ge_expectations = list()
    if expectation_group_name:
        expectation_group = config.data_quality.find_expectation_group(
            expectation_group_name
        )
        ge_expectations.extend(expectation_group.expectations)
    else:
        great_expectations = config.data_quality.great_expectations
        for ge in great_expectations:
            ge_expectations.extend(ge.expectations)
    expectation_suite = create_expectation_suite(ge_expectations)
    return expectation_suite


def get_custom_expectation_to_process(config, custom_expectation_name):
    custom_expectations = []
    if custom_expectation_name:
        custom_expectation = config.data_quality.find_custom_expectation_group(
            custom_expectation_name
        )
        if not custom_expectation:
            raise ValidationError(
                f"Custom expectation '{custom_expectation_name}' not found."
            )
        custom_expectations = custom_expectation.expectations
    else:
        for group in config.data_quality.custom_expectations:
            custom_expectations.extend(group.expectations)
    return custom_expectations
