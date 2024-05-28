import json
import time
from datetime import datetime
from typing import List, Optional, Tuple

import great_expectations as ge
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
from sentinel.models import CustomExpectation, DataQualityConfig, Expectations
from sentinel.utils.utils import read_config_file


def evaluate(
    jsonpath: str,
    source_table_name: str,
    target_table: str,
    spark: Optional[SparkSession] = None,
):
    """Executes data validation according to the provided configuration.

    Args:
        :param target_table: Name of the table where validation results will be saved.
        :param source_table_name: Name of the source table to be validated.
        :param jsonpath: Path to the configuration JSON file
        :param spark: .
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    config = read_config_file(jsonpath, spark)
    validate_data(spark, config.data_quality, source_table_name, target_table)


def validate_data(
    spark: SparkSession,
    config: DataQualityConfig,
    source_table_name: str,
    target_table: str,
):
    """Performs data validation using both Great Expectations and custom validations.

    Args:
        :param spark: Spark session.
        :param config: Data quality configuration.
        :param source_table_name: Name of the source table to be validated.
        :param target_table: Name of the table where validation results will be saved.
    """
    logger.info('Starting validation')
    expectation_suite = create_expectation_suite(config.great_expectations)
    spark_df = spark.table(source_table_name)
    result_df, success = validate_great_expectations(
        spark, spark_df, expectation_suite, source_table_name
    )

    custom_results_df, custom_success = validate_custom_expectations(
        spark, config.custom_expectations, source_table_name
    )

    final_result_df = result_df
    if custom_results_df is not None:
        final_result_df = result_df.union(custom_results_df)

    if not success or not custom_success:
        raise ValidationError(
            f'An error occurred during execution! Please consult '
            f'table {target_table} for more information.'
        )

    final_result_df.write.format('delta').mode('append').saveAsTable(
        target_table
    )


def validate_great_expectations(
    spark, spark_df, expectation_suite, table_name
):
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
            'error_message': res['result'].get('unexpected_count', None),
            'observed_value': str(res['result'].get('observed_value', None)),
            'severity': 'critical' if not res['success'] else 'info',
            'table_name': table_name,
            'execution_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'validation_id': validation_id,
            'validation_time': validation_time,
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'datasource_name': 'mock_datasource',
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

    for custom in custom_expectations:
        sql_query = custom.sql
        expected_columns_list = custom.expected_results
        result_df = spark.sql(sql_query)
        result = result_df.collect()

        expected_column_names = set()
        for expected_columns in expected_columns_list:
            expected_column_names.update(expected_columns.keys())
        expected_column_names = list(expected_column_names)

        actual_columns = result_df.columns
        missing_columns = [
            col for col in expected_column_names if col not in actual_columns
        ]
        column_validation_success = not missing_columns

        value_validation_success = True
        if column_validation_success:
            for expected in expected_columns_list:
                match_found = any(
                    all(
                        row[col] == val
                        for col, val in expected.items()
                        if col in actual_columns
                    )
                    for row in result
                )
                if not match_found:
                    value_validation_success = False
                    break

        success = column_validation_success and value_validation_success

        record = {
            'expectation_type': custom.name,
            'kwargs': json.dumps(
                {'sql': sql_query, 'expected_columns': expected_columns_list}
            ),
            'success': success,
            'error_message': None
            if success
            else f'Expected result: {expected_columns_list}, got: {result}. Expected columns: {expected_column_names}, got: {actual_columns}. Missing columns: {missing_columns}',
            'observed_value': str(
                json.dumps([row.asDict() for row in result])
            ),
            'severity': 'critical' if not success else 'info',
            'table_name': source_table_name,
            'execution_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'validation_id': f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'validation_time': None,
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'datasource_name': source_table_name,
            'dataset_name': None,
            'expectation_suite_name': None,
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
