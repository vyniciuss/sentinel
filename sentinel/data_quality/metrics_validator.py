import uuid
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from sentinel.config.logging_config import logger
from sentinel.models import DataQualityConfig
from sentinel.models.metrics import MetricSet


def generate_metrics_query(
    source_table_name: str, metric_set: MetricSet
) -> str:
    """
    Generate a SQL query to calculate metrics and validation rules.

    This function constructs a SQL query string that calculates metrics and validation rules
    for a given source table based on the provided metric set configuration. The query will
    generate columns for each metric and validation rule, applying the specified conditions.

    Args:
        source_table_name (str): The name of the source table from which to calculate metrics.
        metric_set (MetricSet): The metric set configuration containing metrics and validation rules.

    Returns:
        str: A SQL query string to calculate the metrics and validation rules.

    """
    select_statements = []
    group_by_clause = ''

    if metric_set.group_by:
        group_by_clause = f'GROUP BY {metric_set.group_by}'

    for metric, conditions in metric_set.metrics.items():
        for condition in conditions:
            column_name = condition.column_name
            condition_query = condition.condition
            escaped_condition_query = repr(condition_query)
            select_statements.append(
                f'SUM(CASE WHEN {condition_query} THEN 1 ELSE 0 END) / COUNT(*) AS {column_name}, '
                f"'metric' AS source_type_{column_name}, "
                f'{escaped_condition_query} AS condition_{column_name}'
            )

    for rule in metric_set.validation_rules:
        validation_column = f'{rule.name}_validation'
        condition = rule.condition
        escaped_condition = repr(condition)
        select_statements.append(
            f'CASE WHEN {condition} THEN 1 ELSE 0 END AS {validation_column}, '
            f"'validation_rule' AS source_type_{validation_column}, "
            f'{escaped_condition} AS condition_{validation_column}'
        )

    metrics_query = f"SELECT {', '.join(select_statements)} FROM {source_table_name} {group_by_clause}"
    logger.info(metrics_query)
    return metrics_query


def save_metrics(
    metrics_df: DataFrame,
    table_name: str,
    execution_date: str,
    validation_id: str,
    target_table_name: str,
    metric_set_name: str,
    metric_run_id: str,
) -> None:
    """
    Save metrics to the target table in Delta format.

    This function takes a DataFrame containing metrics, adds metadata columns such as table name,
    execution date, validation ID, and metric set details, and then saves the DataFrame to the specified
    target table in Delta format.

    Args:
        metrics_df (DataFrame): The DataFrame containing the calculated metrics.
        table_name (str): The name of the source table.
        execution_date (str): The execution date and time as a string.
        validation_id (str): The unique identifier for the validation run.
        target_table_name (str): The name of the target table where the metrics will be saved.
        metric_set_name (str): The name of the metric set used.
        metric_run_id (str): The unique identifier for the metric set.
    """
    metrics_df = metrics_df.withColumn(
        'metrics', F.to_json(F.struct(*metrics_df.columns))
    )
    metrics_df = metrics_df.withColumn('table_name', F.lit(table_name))
    metrics_df = metrics_df.withColumn('execution_date', F.lit(execution_date))
    metrics_df = metrics_df.withColumn('validation_id', F.lit(validation_id))
    metrics_df = metrics_df.withColumn('metric_run_id', F.lit(metric_run_id))
    metrics_df = metrics_df.withColumn(
        'metric_set_name', F.lit(metric_set_name)
    )

    final_df = metrics_df.select(
        'table_name',
        'execution_date',
        'validation_id',
        'metric_run_id',
        'metric_set_name',
        'metrics',
    )

    final_df.write.format('delta').mode('append').saveAsTable(
        target_table_name
    )


def calculate_metrics_in_sql(
    spark: SparkSession, source_table_name: str, metric_set: MetricSet
) -> DataFrame:
    """
    Calculate metrics for a given source table based on the specified metric set.

    This function generates a SQL query to calculate metrics and validation rules
    defined in the given MetricSet for the source table, executes the query using
    Spark, and returns the resulting DataFrame.

    Args:
        spark (SparkSession): The Spark session to use for executing the SQL query.
        source_table_name (str): The name of the source table from which to calculate metrics.
        metric_set (MetricSet): The set of metrics and validation rules to calculate.

    Returns:
        DataFrame: A DataFrame containing the calculated metrics and validation results.
    """
    metrics_query = generate_metrics_query(source_table_name, metric_set)
    metrics_df = spark.sql(metrics_query)
    return metrics_df


def generate_and_save_metrics(
    data_quality: DataQualityConfig,
    source_table_name: str,
    spark: SparkSession,
    target_table: str,
    metric_set_name='default_name',
) -> None:
    """
    Generate and save metrics for a given source table based on the specified metric set.

    This function generates metrics and validation results for a source table using
    the defined metric sets in the data quality configuration. The results are then
    saved to the target table.

    Args:
        data_quality (DataQualityConfig): The data quality configuration containing metric sets.
        source_table_name (str): The name of the source table from which to calculate metrics.
        spark (SparkSession): The Spark session to use for executing SQL queries.
        target_table (str): The name of the target table where the metrics and validation results will be saved.
        metric_set_name (str): The name of the metric set to use for generating metrics. If not specified, all metric sets will be used.

    Raises:
        ValueError: If the specified metric set name is not found in the data quality configuration.
    """
    all_metrics_df = None

    if metric_set_name and 'default_name' not in metric_set_name:
        metric_set = next(
            (
                ms
                for ms in data_quality.metrics_config
                if ms.name == metric_set_name
            ),
            None,
        )
        if metric_set:
            metrics_df = calculate_metrics_in_sql(
                spark, source_table_name, metric_set
            )
            all_metrics_df = metrics_df
        else:
            raise ValueError(
                f"Metric set with name '{metric_set_name}' not found."
            )
    else:
        for metric_set in data_quality.metrics_config:
            metrics_df = calculate_metrics_in_sql(
                spark, source_table_name, metric_set
            )
            if all_metrics_df is None:
                all_metrics_df = metrics_df
            else:
                all_metrics_df = all_metrics_df.union(metrics_df)

    if all_metrics_df is not None:
        save_metrics(
            all_metrics_df,
            source_table_name,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            f'validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            f'{target_table}_metrics',
            metric_set_name,
            str(uuid.uuid4()),
        )
