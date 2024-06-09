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


def test_validate_custom_expectations(spark, setup_data):
    custom_expectations = [
        CustomExpectation(
            name='test_expectation',
            sql='SELECT CASE WHEN COUNT(*) = 2 THEN 1 ELSE 0 END as validation_result from test_db.source_table where age=45',
        )
    ]
    source_table_name = 'test_db.source_table'
    result_df, success = validate_custom_expectations(
        spark, custom_expectations, source_table_name
    )
    result_df.show()
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
