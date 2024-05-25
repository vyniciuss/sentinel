from sentinel.config.logging_config import logger
from sentinel.data_quality.validator import (
    create_expectation_suite,
    validate_great_expectations,
)
from sentinel.utils.utils import read_config_file


def test_great_expectation_validation(spark, file_path):
    json_path = file_path
    table_name = 'contas'
    config = read_config_file(json_path, spark)
    expectation_suite = create_expectation_suite(
        config.data_quality.great_expectations
    )
    data = [
        ('Alice', 30, 'single'),
        ('Bob', 45, 'married'),
        ('Charlie', None, 'divorced'),
    ]

    columns = ['name', 'age', 'status']
    df = spark.createDataFrame(data, columns)
    df.show(truncate=False)
    result_df, success = validate_great_expectations(
        spark, df, expectation_suite, table_name
    )
    logger.info(success)
    result_df.show(truncate=False)
    total = result_df.filter("success = 'false'").count()
    logger.info(f'--------------- {total}')
    assert total == 1
