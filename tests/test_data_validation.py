import os
import uuid
from os import path
from unittest.mock import patch

from typer.testing import CliRunner

from sentinel.main import app

runner = CliRunner()


@patch('sentinel.data_quality.validator.SparkSession', autospec=True)
def test_full_validation_process(
    mock_spark_session, spark, setup_data, file_path
):
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
        ],
        catch_exceptions=False,
    )

    result_df = spark.table('test_db.result_table')
    result_df.show()
    assert result_df.count() > 0
    assert result.exit_code == 0


@patch('sentinel.data_quality.validator.SparkSession', autospec=True)
def test_full_validation_streaming_process(
    mock_spark_session, spark, setup_data, file_path
):
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
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
