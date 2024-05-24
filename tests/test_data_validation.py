import os
import shutil
from unittest.mock import patch

import pytest
from pyspark.sql.utils import AnalysisException
from typer.testing import CliRunner

from sentinel.main import app
from sentinel.models import DataQualityConfig

runner = CliRunner()


@pytest.fixture
def setup_data(spark):
    spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    spark.sql('USE test_db')

    data = [
        ('Alice', 30, 'single'),
        ('Bob', 45, 'married'),
        ('Charlie', 45, 'divorced'),
    ]
    columns = ['name', 'age', 'status']
    df = spark.createDataFrame(data, columns)
    df.write.format('delta').mode('overwrite').saveAsTable(
        'test_db.source_table'
    )

    result_table_path = os.path.join(
        'spark-warehouse', 'test_db.db', 'result_table'
    )
    if os.path.exists(result_table_path):
        shutil.rmtree(result_table_path)

    try:
        spark.sql(
            'CREATE TABLE IF NOT EXISTS test_db.result_table ('
            'expectation_type STRING, kwargs STRING, success BOOLEAN, error_message STRING, '
            'observed_value FLOAT, severity STRING, table_name STRING, execution_date STRING, '
            'validation_id STRING, validation_time FLOAT, batch_id STRING, '
            'datasource_name STRING, dataset_name STRING, expectation_suite_name STRING) '
            'USING delta'
        )
    except AnalysisException:
        spark.sql('DROP TABLE IF EXISTS test_db.result_table')
        spark.sql(
            'CREATE TABLE test_db.result_table ('
            'expectation_type STRING, kwargs STRING, success BOOLEAN, error_message STRING, '
            'observed_value FLOAT, severity STRING, table_name STRING, execution_date STRING, '
            'validation_id STRING, validation_time FLOAT, batch_id STRING, '
            'datasource_name STRING, dataset_name STRING, expectation_suite_name STRING) '
            'USING delta'
        )

    yield

    spark.sql('DROP DATABASE IF EXISTS test_db CASCADE')


@pytest.fixture
def mock_config():
    return DataQualityConfig(
        great_expectations=[
            {
                'expectation_type': 'expect_column_to_exist',
                'kwargs': {'column': 'name'},
            },
            {
                'expectation_type': 'expect_column_values_to_not_be_null',
                'kwargs': {'column': 'age'},
            },
            {
                'expectation_type': 'expect_column_values_to_be_in_set',
                'kwargs': {
                    'column': 'status',
                    'value_set': ['single', 'married', 'divorced'],
                },
            },
        ],
        custom_expectations=[],
    )


@patch('sentinel.data_quality.validator.SparkSession', autospec=True)
def test_full_validation_process(mock_spark_session, spark, setup_data):
    mock_spark_session.builder.getOrCreate.return_value = spark

    result = runner.invoke(
        app,
        [
            # 'main',:
            '--jsonpath',
            'E:\\sentinel\\tests\\resources\\process.json',
            '--source-table-name',
            'test_db.source_table',
            '--target-table-name',
            'test_db.result_table',
        ],
        catch_exceptions=False,
    )

    # print(result.stdout)
    # if result.stderr:
    #     print(result.stderr)

    # assert result.exit_code == 0

    # Validate the results in the target table
    result_df = spark.table('test_db.result_table')
    result_df.show()
    # assert result_df.count() > 0
