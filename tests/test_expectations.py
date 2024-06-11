from sentinel.config.logging_config import logger
from sentinel.data_quality.expectations_validator import (
    create_expectation_suite,
    generate_target_schema,
    validate_custom_expectations,
    validate_great_expectations,
)
from sentinel.models import CustomExpectation, Expectations
from sentinel.utils.utils import read_config_file


def test_great_expectation_validation(spark, file_path):
    json_path = file_path
    table_name = 'test_db.source_table'
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


def test_custom_expectation_group_validation(spark, file_path, setup_data):
    json_path = file_path
    table_name = 'test_db.source_table'
    config = read_config_file(json_path, spark)
    custom_expectation_group_name = 'validation2'

    custom_expectation_group = (
        config.data_quality.find_custom_expectation_group(
            custom_expectation_group_name
        )
    )
    custom_expectations = custom_expectation_group.expectations

    data = [
        ('Alice', 30, 'single'),
        ('Alice', 30, 'married'),
        ('Bob', 45, 'married'),
        ('Charlie', 45, 'divorced'),
    ]

    columns = ['name', 'age', 'status']
    df = spark.createDataFrame(data, columns)
    df.show(truncate=False)
    result_df, success = validate_custom_expectations(
        spark, custom_expectations, table_name
    )
    logger.info(success)
    result_df.show(truncate=False)
    assert success is True


def test_generate_target_schema():
    schema = generate_target_schema()
    assert schema is not None
    assert len(schema.fields) > 0


def test_create_expectation_suite():
    expectations = [
        Expectations(
            expectationType='expect_column_to_exist',
            kwargs={'column': 'name'},
        )
    ]
    suite = create_expectation_suite(expectations)
    assert suite is not None
    assert len(suite.expectations) == 1
