import os
import uuid
from os import path
from unittest.mock import patch

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from typer.testing import CliRunner

from sentinel.data_quality.validator import create_process_batch
from sentinel.main import app

runner = CliRunner()


@patch('sentinel.data_quality.validator.SparkSession', autospec=True)
def test_full_validation_process(
    mock_spark_session, spark, setup_data, file_path
):
    spark.catalog.clearCache()
    spark.sql('REFRESH TABLE test_db.source_table')
    mock_spark_session.builder.getOrCreate.return_value = spark
    checkpoint = path.join(os.getcwd(), 'checkpoint')
    result = runner.invoke(
        app,
        [
            '--json-path',
            file_path,
            '--source-table-name',
            'test_db.source_table',
            '--target-table-name',
            'test_db.result_table',
            '--process-type',
            'batch',
            '--source-config-name',
            'tableA',
            '--checkpoint',
            f'{checkpoint}\\process_{uuid.uuid4()}',
            '--metric-set-name',
            'basic_metrics',
            '--custom-expectation-name',
            'validation2',
        ],
        catch_exceptions=False,
    )

    result_df = spark.table('test_db.result_table_metrics')
    result_df.show(10, False)
    extract_and_display_metrics(spark, 'test_db.result_table_metrics')
    assert result_df.count() > 0
    assert result.exit_code == 0


@patch('sentinel.data_quality.validator.SparkSession', autospec=True)
def test_full_validation_streaming_process(
    mock_spark_session, spark, setup_data, file_path
):
    spark.catalog.clearCache()
    spark.sql('REFRESH TABLE test_db.source_table')
    mock_spark_session.builder.getOrCreate.return_value = spark

    checkpoint = path.join(os.getcwd(), 'checkpoint')

    result = runner.invoke(
        app,
        [
            '--json-path',
            file_path,
            '--source-table-name',
            'test_db.source_table',
            '--target-table-name',
            'test_db.result_table',
            '--process-type',
            'streaming',
            '--source-config-name',
            'tableA',
            '--checkpoint',
            f'{checkpoint}\\process_{uuid.uuid4()}',
            '--custom-expectation-name',
            'validation2',
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def extract_and_display_metrics(
    spark: SparkSession, target_table_name: str
) -> DataFrame:
    spark.catalog.clearCache()
    spark.sql('REFRESH TABLE test_db.source_table')
    metrics_df = spark.table(target_table_name)
    metrics_df = metrics_df.withColumn(
        'metrics_exploded',
        F.explode(
            F.map_entries(F.from_json(F.col('metrics'), 'map<string, string>'))
        ),
    )
    exploded_df = metrics_df.select(
        'table_name',
        'execution_date',
        'validation_id',
        'metric_set_name',
        F.col('metrics_exploded.key').alias('metric_name'),
        F.col('metrics_exploded.value').alias('metric_value'),
    )
    exploded_df.show(10, truncate=False)
    return exploded_df


def test_create_process_batch(spark, setup_data, file_path):
    from sentinel.utils.utils import read_config_file

    config = read_config_file(file_path, spark)
    process_batch = create_process_batch(
        spark,
        config,
        'test_db.source_table',
        'test_db.result_table',
        None,
        None,
    )
    process_batch(spark.table('test_db.source_table'), 0)
    result_df = spark.table('test_db.result_table')
    result_df.show()
    assert result_df.count() > 0
