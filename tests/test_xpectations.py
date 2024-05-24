from sentinel.data_quality.validator import (
    create_expectation_suite,
    validate_data,
)
from sentinel.utils.utils import read_config_file


def test_column_presence(spark):
    json_path = 'E:\\sentinel\\tests\\resources\\process.json'
    table_name = 'contas'
    config = read_config_file(json_path, spark)
    expectation_suite = create_expectation_suite(config.expectations)

    data = [
        ('Alice', 30, 'single'),
        ('Bob', 45, 'married'),
        ('Charlie', None, 'divorced'),
    ]

    columns = ['name', 'age', 'status']
    df = spark.createDataFrame(data, columns)

    result_df, success = validate_data(
        spark, df, expectation_suite, table_name
    )
    total = result_df.filter("success = 'false'").count()
    assert total == 1
