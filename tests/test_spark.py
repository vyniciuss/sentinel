from sentinel.config.logging_config import logger


def test_spark_session(spark):
    data = [('Alice', 1), ('Bob', 2)]
    df = spark.createDataFrame(data, ['name', 'value'])
    result = df.count()
    df.show()
    assert result == 2


def test_another_example(spark):
    data = [('Charlie', 3), ('David', 4)]
    df = spark.createDataFrame(data, ['name', 'value'])

    result = df.where('value > 2').count()

    df.show()
    assert result == 2
