import json
import time
from datetime import datetime
from typing import List, Optional, Tuple

import great_expectations as ge
import pyspark.sql.functions as F
from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from sentinel.config.logging_config import logger
from sentinel.exception.ValidationError import ValidationError
from sentinel.models import Config, CustomExpectation, Expectations
from sentinel.utils.utils import read_config_file


def evaluate(
    json_path: str,
    source_table_name: str,
    target_table: str,
    process_type: str,
    source_config_name: str,
    checkpoint: Optional[str],
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
        validate_data(spark, config, source_table_name, target_table, df)
    else:
        logger.info(f'Streaming reader created {source_table_name}')
        source_config = config.find_source_config(source_config_name)
        (
            spark.readStream.format('delta')
            .options(**source_config.read_options)
            .table(source_table_name)
            .writeStream.foreachBatch(
                create_process_batch(
                    spark, config, source_table_name, target_table
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
        )

    return process_batch


def validate_data(
    spark: SparkSession,
    config: Config,
    source_table_name: str,
    target_table: str,
    dataframe: DataFrame,
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

    logger.info(f'Final Dataframe {final_result_df.count()}')
    final_result_df.write.format('delta').mode('append').saveAsTable(
        target_table
    )

    if not ge_success or not custom_success:
        raise ValidationError(
            f'An error occurred during execution! Please consult '
            f'table {target_table} for more information.'
        )


def validate_great_expectations(
    spark: SparkSession,
    spark_df: DataFrame,
    expectation_suite: ExpectationSuite,
    table_name: str,
) -> None:
    """
    Validates a Spark DataFrame using Great Expectations and logs the results.

    Args:
        spark (SparkSession): The Spark session.
        spark_df (DataFrame): The Spark DataFrame to be validated.
        expectation_suite (Any): The expectation suite to be used for validation.
        table_name (str): The name of the table being validated.

    Returns:
        Tuple[DataFrame, bool]: A tuple containing the DataFrame of validation results and a boolean indicating overall success.

    """
    logger.info('Starting validation with great expectations')
    ge_df = ge.dataset.SparkDFDataset(spark_df)
    start_time = time.time()
    result = ge_df.validate(expectation_suite=expectation_suite)
    validation_time = time.time() - start_time

    records = []
    validation_id = f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    for res in result['results']:
        record = {
            'expectation_type': res['expectation_config']['expectation_type'],
            'kwargs': json.dumps(res['expectation_config']['kwargs']),
            'success': res['success'],
            'error_message': res['result'].get('unexpected_list', None),
            'observed_value': str(res['result'].get('observed_value', None)),
            'severity': 'critical' if not res['success'] else 'info',
            'table_name': table_name,
            'execution_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'validation_id': validation_id,
            'validation_time': validation_time,
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'datasource_name': table_name,
            'dataset_name': 'mock_dataset',
            'expectation_suite_name': expectation_suite.expectation_suite_name,
        }
        records.append(record)

    schema = generate_target_schema()

    result_df = spark.createDataFrame(records, schema)
    return result_df, result['success']


def validate_custom_expectations(
    spark: SparkSession,
    custom_expectations: List[CustomExpectation],
    source_table_name: str,
) -> Tuple[DataFrame, bool]:
    """Validates data using custom SQL queries.

    Args:
        spark (SparkSession): Spark session.
        custom_expectations (List[CustomExpectation]): List of custom SQL expectations.
        source_table_name (str): Name of the source table being validated.

    Returns:
        Tuple[DataFrame, bool]: DataFrame with validation results and success status.
    """
    logger.info('Starting validation with custom expectations')

    records = []
    custom_success = True

    for expectation in custom_expectations:
        validation_id = (
            f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        logger.info(f'Executing query for expectation: {expectation.name}')
        sql_query = expectation.sql
        logger.info(sql_query)
        start_time = time.time()
        result_df = spark.sql(sql_query)
        success = result_df.filter(F.col('validation_result') == 1).count() > 0
        validation_time = time.time() - start_time

        record = {
            'expectation_type': expectation.name,
            'kwargs': json.dumps({'sql': sql_query}),
            'success': success,
            'error_message': None if success else 'Validation failed',
            'observed_value': None,
            'severity': 'critical' if not success else 'info',
            'table_name': source_table_name,
            'execution_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'validation_id': validation_id,
            'validation_time': validation_time,
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'datasource_name': source_table_name,
            'dataset_name': source_table_name,
            'expectation_suite_name': expectation.name,
        }
        records.append(record)

        if not success:
            custom_success = False

    schema = generate_target_schema()

    result_df = spark.createDataFrame(records, schema)
    return result_df, custom_success


def generate_target_schema() -> StructType:
    """Generates the schema for the validation results DataFrame.

    Returns:
        StructType: Schema for the validation results DataFrame.
    """
    schema = StructType(
        [
            StructField('expectation_type', StringType(), True),
            StructField('kwargs', StringType(), True),
            StructField('success', BooleanType(), True),
            StructField('error_message', StringType(), True),
            StructField('observed_value', StringType(), True),
            StructField('severity', StringType(), True),
            StructField('table_name', StringType(), True),
            StructField('execution_date', StringType(), True),
            StructField('validation_id', StringType(), True),
            StructField('validation_time', FloatType(), True),
            StructField('batch_id', StringType(), True),
            StructField('datasource_name', StringType(), True),
            StructField('dataset_name', StringType(), True),
            StructField('expectation_suite_name', StringType(), True),
        ]
    )
    return schema


def create_expectation_suite(
    expectations: List[Expectations], suite_name='default_suite'
) -> ExpectationSuite:
    """Creates an ExpectationSuite from a list of expectation configurations.

    Args:
        expectations (List[ExpectationConfiguration]): List of expectation configurations.
        suite_name (str): Name of the expectation suite.

    Returns:
        ExpectationSuite: Created ExpectationSuite.
    """
    suite = ge.core.ExpectationSuite(expectation_suite_name=suite_name)
    for expectation in expectations:
        expectation_config = ExpectationConfiguration(
            expectation_type=expectation.expectation_type,
            kwargs=expectation.kwargs,
        )
        suite.add_expectation(expectation_config)
    return suite
