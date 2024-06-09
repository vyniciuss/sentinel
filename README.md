# Sentinel: Data Quality Validation Project

## Overview

Sentinel is a data quality validation project designed to ensure the integrity and accuracy of your data. It uses a combination of Great Expectations for standardized data validations and custom SQL-based expectations to handle unique validation scenarios. This project is particularly useful for organizations that need to maintain high data quality standards across various data pipelines.

## Features

- **Great Expectations Integration**: Leverage the robust validation framework of Great Expectations to perform comprehensive data quality checks.
- **Custom Expectations**: Define and execute custom SQL-based validations tailored to your specific data quality requirements.
- **Metrics Calculation and Validation**: Compute and validate data metrics as part of the validation process.
- **Comprehensive Reporting**: Detailed logging and reporting of validation results, including success and failure details.

## Getting Started

### Prerequisites

- Python 3.11
- Apache Spark
- Great Expectations
- Delta Lake

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/vyniciuss/sentinel
   cd sentinel
   ```

2. Install dependencies using Poetry:

   ```bash
   poetry install
   ```

### Configuration

Create a JSON configuration file (e.g., `process.json`) to define your data quality checks. Below is an example configuration:

```json
{
  "great_expectations": [
    {
      "expectation_type": "expect_column_to_exist",
      "kwargs": {
        "column": "name"
      }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "age"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": {
        "column": "status",
        "value_set": ["single", "married", "divorced"]
      }
    }
  ],
  "customExpectations": [
    {
      "name": "total_custom",
      "sql": "SELECT CASE WHEN COUNT(*) = 2 THEN 1 ELSE 0 END as validation_result from test_db.source_table where age=45"
    },
    {
      "name": "total_custom2",
      "sql": "SELECT CASE WHEN COUNT(*) = 1 THEN 1 ELSE 0 END as validation_result from test_db.source_table where name = 'Alice' and status = 'single'"
    },
    {
      "name": "total_custom3",
      "sql": "SELECT CASE WHEN COUNT(*) = 1 THEN 1 ELSE 0 END as validation_result from test_db.source_table where name = 'Alice' and status = 'married'"
    }
  ],
  "metrics_config": [
    {
      "id": "1",
      "name": "test_metrics",
      "group_by": "status",
      "metrics": {
        "accuracy_age": [
          {
            "column_name": "accuracy_age",
            "condition": "age BETWEEN 0 AND 120"
          }
        ],
        "completeness_name": [
          {
            "column_name": "completeness_name",
            "condition": "name IS NOT NULL"
          }
        ]
      },
      "validation_rules": [
        {
          "name": "age_range_check",
          "condition": "accuracy_age > 0.95",
          "error_message": "Age accuracy is below 95%"
        }
      ]
    }
  ]
}
```

### Explanation of Metrics Configuration

The `metrics_config` section allows you to define sets of metrics and their associated validation rules. Each metric set includes:

- `id`: A unique identifier for the metric set.
- `name`: The name of the metric set.
- `group_by` (optional): A column name to group the metrics by.
- `metrics`: A dictionary where keys are metric names and values are lists of conditions used to compute the metrics. Each condition includes:
  - `column_name`: The name of the column for the metric.
  - `condition`: The SQL condition used to calculate the metric.
- `validation_rules`: A list of validation rules applied to the calculated metrics. Each validation rule includes:
  - `name`: The name of the validation rule.
  - `condition`: The condition that must be met for the metric to pass validation.
  - `error_message`: The error message to be logged if the validation fails.

#### Example

In the example above, `test_metrics` is a metric set that:
- Calculates the accuracy of the `age` column (`accuracy_age`), ensuring values are between 0 and 120.
- Calculates the completeness of the `name` column (`completeness_name`), ensuring no values are null.
- Applies a validation rule (`age_range_check`) that ensures the `accuracy_age` is greater than 0.95. If this condition is not met, an error message "Age accuracy is below 95%" is logged.

### Usage

Run the validation process by specifying the path to your configuration file and the tables involved:

```bash
poetry run sentinel --jsonpath /path/to/process.json --source-table-name test_db.source_table --target-table-name test_db.result_table
```

### Detailed Example

Here is an example of how to use Sentinel in a typical data validation scenario.

1. **Create the Configuration File**:

   ```json
   {
     "great_expectations": [
       {
         "expectation_type": "expect_column_to_exist",
         "kwargs": {
           "column": "name"
         }
       },
       {
         "expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {
           "column": "age"
         }
       }
     ],
     "customExpectations": [
       {
         "name": "total_custom",
         "sql": "SELECT CASE WHEN COUNT(*) = 2 THEN 1 ELSE 0 END as validation_result from test_db.source_table where age=45"
       }
     ],
     "metrics_config": [
       {
         "id": "1",
         "name": "test_metrics",
         "group_by": "status",
         "metrics": {
           "accuracy_age": [
             {
               "column_name": "accuracy_age",
               "condition": "age BETWEEN 0 AND 120"
             }
           ],
           "completeness_name": [
             {
               "column_name": "completeness_name",
               "condition": "name IS NOT NULL"
             }
           ]
         },
         "validation_rules": [
           {
             "name": "age_range_check",
             "condition": "accuracy_age > 0.95",
             "error_message": "Age accuracy is below 95%"
           }
         ]
       }
     ]
   }
   ```

2. **Run the Validation**:

   ```bash
   poetry run sentinel --jsonpath config.json --source-table-name test_db.source_table --target-table-name test_db.result_table
   ```

3. **Review the Results**:

   Check the target table (`test_db.result_table`) for detailed results of the validation process.

### Reporting

The validation results are saved in a Delta table specified by the `--target-table-name` parameter. The schema of this table is as follows:

- `expectation_type` (StringType): The type of expectation that was validated.
- `kwargs` (StringType): The keyword arguments for the expectation.
- `success` (BooleanType): Whether the validation was successful.
- `error_message` (StringType): Error message if the validation failed.
- `observed_value` (StringType): The observed value that was validated.
- `severity` (StringType): The severity of the validation (e.g., critical, info).
- `table_name` (StringType): The name of the table that was validated.
- `execution_date` (StringType): The date and time of the validation execution.
- `validation_id` (StringType): A unique ID for the validation.
- `validation_time` (FloatType): The time taken for the validation.
- `batch_id` (StringType): The batch ID for the validation.
- `datasource_name` (StringType): The name of the data source.
- `dataset_name` (StringType): The name of the dataset.
- `expectation_suite_name` (StringType): The name of the expectation suite.

### Metrics Reporting

The metrics results are also saved in a Delta table specified by the `--target-table-name` parameter with a `_metrics` suffix. The schema of this table is as follows:

- `table_name` (StringType): The name of the table that was validated.
- `execution_date` (StringType): The date and time of the metrics calculation.
- `validation_id` (StringType): A unique ID for the validation.
- `metric_set_name` (StringType): The name of the metric set.
- `metric_set_id` (StringType): The ID of the metric set.
- `metrics` (StringType): A JSON string containing the calculated metrics.

To extract and display the metrics in a more readable format, you can use the `extract_and_display_metrics` function:

```python
def extract_and_display_metrics(spark: SparkSession, target_table_name: str) -> DataFrame:
    metrics_df = spark.table(target_table_name)
    metrics_df = metrics_df.withColumn("metrics_exploded", F.explode(F.map_entries(F.from_json(F.col("metrics"), "map<string, string>"))))
    exploded_df = metrics_df.select(
        "table_name",
        "execution_date",
        "validation_id",
        "metric_set_name",
        F.col("metrics_exploded.key").alias("metric_name"),
        F.col

("metrics_exploded.value").alias("metric_value")
    )
    exploded_df.show(10, truncate=False)
    return exploded_df
```

### Example of Metrics Results

```plaintext
| table_name | execution_date     | validation_id   | metric_set_name | metric_name         | metric_value        |
|------------|--------------------|-----------------|-----------------|---------------------|---------------------|
| test_table | 2024-06-09 15:09:55| validation_12345| test_metrics    | accuracy_age        | 0.96                |
| test_table | 2024-06-09 15:09:55| validation_12345| test_metrics    | completeness_name   | 1.0                 |
| test_table | 2024-06-09 15:09:55| validation_12345| test_metrics    | age_range_check     | 1                   |
```

## Testing

To ensure the robustness of Sentinel, unit tests are included in the project. You can run the tests using `pytest`:

```bash
poetry run pytest tests/
```

## Contributing

We welcome contributions to Sentinel! Please fork the repository and submit a pull request with your changes. Make sure to include tests for any new features or bug fixes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
