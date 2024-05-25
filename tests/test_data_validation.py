from unittest.mock import patch

from typer.testing import CliRunner

from sentinel.main import app

runner = CliRunner()


@patch('sentinel.data_quality.validator.SparkSession', autospec=True)
def test_full_validation_process(mock_spark_session, spark, setup_data):
    mock_spark_session.builder.getOrCreate.return_value = spark

    result = runner.invoke(
        app,
        [
            '--jsonpath',
            'E:\\sentinel\\tests\\resources\\process.json',
            '--source-table-name',
            'test_db.source_table',
            '--target-table-name',
            'test_db.result_table',
        ],
        catch_exceptions=False,
    )

    result_df = spark.table('test_db.result_table')
    result_df.show()
    assert result_df.count() > 0
