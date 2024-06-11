from datetime import datetime

from sentinel.data_quality.metrics_validator import (
    calculate_metrics_in_sql,
    generate_and_save_metrics,
    generate_metrics_query,
    save_metrics,
)
from sentinel.models.metrics import MetricCondition, MetricSet, ValidationRule


def test_generate_metrics_query():
    metric_set = MetricSet(
        id='1',
        name='test_metrics',
        group_by=None,
        metrics={
            'accuracy': [
                MetricCondition(
                    column_name='accuracy_age', condition='age > 0'
                )
            ]
        },
        validation_rules=[
            ValidationRule(
                name='age_check',
                condition='accuracy_age > 0.95',
                error_message='Age accuracy is below 95%',
            )
        ],
    )
    query = generate_metrics_query('test_db.source_table', metric_set)
    assert 'accuracy_age' in query
    assert 'age_check_validation' in query


def test_save_metrics(spark, setup_data):
    data = [
        (
            'accuracy_age',
            0.96,
            'accuracy_name',
            0.91,
            'age_check_validation',
            1,
            'name_check_validation',
            1,
        )
    ]
    columns = [
        'accuracy_age',
        'accuracy_age_value',
        'accuracy_name',
        'accuracy_name_value',
        'age_check_validation',
        'age_check_validation_value',
        'name_check_validation',
        'name_check_validation_value',
    ]
    metrics_df = spark.createDataFrame(data, columns)
    metrics_df = metrics_df.drop(
        'accuracy_age',
        'accuracy_name',
        'age_check_validation',
        'name_check_validation',
    )

    save_metrics(
        metrics_df,
        'test_table',
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'validation_12345',
        'test_db.result_table_metrics',
        'test_metrics',
        '1',
    )
    result_df = spark.table('test_db.result_table_metrics')
    result_df.show(10, False)
    assert result_df.count() > 0


def test_calculate_metrics_in_sql(spark, setup_data):
    metric_set = MetricSet(
        id='1',
        name='test_metrics',
        group_by=None,
        metrics={
            'accuracy': [
                MetricCondition(
                    column_name='accuracy_age', condition='age > 0'
                )
            ]
        },
        validation_rules=[
            ValidationRule(
                name='age_check',
                condition='accuracy_age > 0.95',
                error_message='Age accuracy is below 95%',
            )
        ],
    )
    result_df = calculate_metrics_in_sql(
        spark, 'test_db.source_table', metric_set
    )
    result_df.show()
    assert result_df.count() > 0


def test_generate_and_save_metrics(spark, setup_data, file_path):
    from sentinel.utils.utils import read_config_file

    config = read_config_file(file_path, spark)
    generate_and_save_metrics(
        config.data_quality,
        'test_db.source_table',
        spark,
        'test_db.result_table',
    )
    result_df = spark.table('test_db.result_table_metrics')
    result_df.show()
    assert result_df.count() > 0
