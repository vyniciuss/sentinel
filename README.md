# Sentinel: Data Quality Validation Project

## Overview

Sentinel is a data quality validation project designed to ensure the integrity and accuracy of your data. It uses a combination of Great Expectations for standardized data validations and custom SQL-based expectations to handle unique validation scenarios. This project is particularly useful for organizations that need to maintain high data quality standards across various data pipelines.

## Features

- **Great Expectations Integration**: Leverage the robust validation framework of Great Expectations to perform comprehensive data quality checks.
- **Custom Expectations**: Define and execute custom SQL-based validations tailored to your specific data quality requirements.
- **Comprehensive Reporting**: Detailed logging and reporting of validation results, including success and failure details.
- **Metrics Calculation**: Calculate and save custom metrics to track data quality over time.

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
  "custom_expectations": [
    {
      "name": "validation1",
      "expectations": [
        {
          "name": "total_custom",
          "sql": "SELECT CASE WHEN COUNT(*) = 2 THEN 1 ELSE 0 END as validation_result from test_db.source_table where age=45"
        }
      ]
    },
    {
      "name": "validation2",
      "expectations": [
        {
          "name": "total_custom1",
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
      ]
    }
  ],
  "metrics_config": [
    {
      "id": "metric_set_1",
      "name": "test_metrics",
      "group_by": "status",
      "metrics": {
        "accuracy": [
          {
            "column_name": "accuracy_name",
            "condition": "name LIKE '%@%.%'"
          },
          {
            "column_name": "accuracy_age",
            "condition": "age BETWEEN 0 AND 120"
          }
        ],
        "completeness": [
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
          "error_message": "Age accuracy is below threshold"
        }
      ]
    }
  ]
}
```

### Usage

Run the validation process by specifying the path to your configuration file and the tables involved:

```bash
poetry run sentinel --jsonpath /path/to/process.json --source-table-name test_db.source_table --target-table-name test_db.result_table --metric-set-name test_metrics --custom-expectation-group-name validation1
```

### Parameters

| Parameter                 | Type             | Description                                                            |
|---------------------------|------------------|------------------------------------------------------------------------|
| `json_path`               | `str`            | Path to the configuration JSON file                                    |
| `source_table_name`       | `str`            | The name of the table that will have the validated data                |
| `target_table_name`       | `str`            | The name of the table where the validation results will be saved       |
| `process_type`            | `str`            | Type of the process, typically "batch" or "stream"                     |
| `source_config_name`      | `str`            | The name of the source configuration to use                            |
| `checkpoint`              | `str`            | Path to the checkpoint directory for Spark                             |
| `metric_set_name`         | `Optional[str]`  | Metrics set name (optional)                                            |
| `custom_expectation_name` | `Optional[str]`  | Custom expectation name (optional)                                     |

### Detailed Example

Here is an example of how to use Sentinel in a typical data validation scenario.

1. **Create the Configuration File**:

   ```json
   {
     "greatExpectations": [
       {
         "expectationType": "expect_column_to_exist",
         "kwargs": {
           "column": "name"
         }
       },
       {
         "expectationType": "expect_column_values_to_not_be_null",
         "kwargs": {
           "column": "age"
         }
       }
     ],
     "customExpectations": [
       {
         "name": "validation1",
         "expectations": [
           {
             "name": "total_custom",
             "sql": "SELECT CASE WHEN COUNT(*) = 2 THEN 1 ELSE 0 END as validation_result from test_db.source_table where age=45"
           }
         ]
       }
     ],
     "metrics_config": [
       {
         "id": "metric_set_1",
         "name": "test_metrics",
         "group_by": "status",
         "metrics": {
           "accuracy": [
             {
               "column_name": "accuracy_name",
               "condition": "name LIKE '%@%.%'"
             },
             {
               "column_name": "accuracy_age",
               "condition": "age BETWEEN 0 AND 120"
             }
           ],
           "completeness": [
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
             "error_message": "Age accuracy is below threshold"
           }
         ]
       }
     ]
   }
   ```

2. **Run the Validation**:

   ```bash
   poetry run sentinel --jsonpath config.json --source-table-name test_db.source_table --target-table-name test_db.result_table --metric-set-name test_metrics --custom-expectation-group-name validation1
   ```

3. **Review the Results**:

   Check the target table (`test_db.result_table`) for detailed results of the validation process.

### Metrics Calculation and Validation Rules

The `metrics_config` section in the configuration JSON allows you to define custom metrics that will be calculated during the validation process. Each metric set can have multiple metrics and validation rules.

- **Metrics**: Defined as conditions on the data. For example, `accuracy_age` checks if the age is between 0 and 120.
- **Validation Rules**: Conditions that must be met for the data to be considered valid. For example, `age_range_check` ensures that the `accuracy_age` metric is above 0.95.

### Example of Metrics Results

Before exploding the `metrics` column, the results are saved in a Delta table as follows:

#### Metrics Table (Before Exploding)

```plaintext
+----------+-------------------+----------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------+
|table_name|execution_date     |validation_id   |metric_set_id|metric_set_name|metrics                                                                                                                                |
+----------+-------------------+----------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------+
|test_table|2024-06-09 15:09:55|validation_12345|1            |test_metrics   |{"accuracy_name":0.85,"accuracy_age":0.98,"completeness_name":1.0,"age_range_check_validation":1}|
+----------+-------------------+----------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------+
```

### Extract and Display Metrics

To extract and display the metrics, you can use the following function:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def extract_and_display_metrics(spark: SparkSession, target_table_name: str):
    metrics_df = spark.table(target_table_name)
    metrics_df = metrics_df.withColumn("metrics_exploded", F.explode(F.map_entries(F.from_json(F.col("metrics"), "map<string, double>"))))
    exploded_df = metrics_df.select(
        "table_name",
        "execution_date",
        "validation_id",
        "metric_set_name",
        F.col("metrics_exploded.key").alias("metric_name"),
        F.col("metrics_exploded.value").alias("metric_value")
    )
    exploded_df.show(10, truncate=False)
   

 return exploded_df

# Example usage:
# extract_and_display_metrics(spark, 'test_db.result_table_metrics')
```

#### Metrics Table (After Exploding)

```plaintext
+----------+-------------------+----------------+---------------+--------------------+--------------------+
|table_name|execution_date     |validation_id   |metric_set_name|metric_name         |metric_value        |
+----------+-------------------+----------------+---------------+--------------------+--------------------+
|test_table|2024-06-09 15:09:55|validation_12345|test_metrics   |accuracy_name       |0.85                |
|test_table|2024-06-09 15:09:55|validation_12345|test_metrics   |accuracy_age        |0.98                |
|test_table|2024-06-09 15:09:55|validation_12345|test_metrics   |completeness_name   |1.0                 |
|test_table|2024-06-09 15:09:55|validation_12345|test_metrics   |age_range_check     |1.0                 |
+----------+-------------------+----------------+---------------+--------------------+--------------------+
```

### Logging and Error Handling

Sentinel provides detailed logging to help you understand the validation process and quickly identify any issues. In case of an error, the details are logged, and an error message is saved to the target table.

### Example Log Output

```
2024-05-24 10:54:35,847 | INFO | Starting validation                            
2024-05-24 10:54:35,847 | ERROR | An error occurred during execution! Please consult table test_db.result_table for more information.
2024-05-24 10:54:35,848 | ERROR | 'dict' object has no attribute 'great_expectations'
```

### Error Handling

If any expectation fails, Sentinel raises a `ValidationError` and logs the error message. The error message provides details on the cause of the failure and suggests consulting the target table for more information.

## Testing

To ensure the robustness of Sentinel, unit tests are included in the project. You can run the tests using `pytest`:

```bash
poetry run pytest tests/
```

## Contributing

We welcome contributions to Sentinel! Please fork the repository and submit a pull request with your changes. Make sure to include tests for any new features or bug fixes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
