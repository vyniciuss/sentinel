# Sentinel: Data Quality Validation Project

## Overview

Sentinel is a data quality validation project designed to ensure the integrity and accuracy of your data. It uses a combination of Great Expectations for standardized data validations and custom SQL-based expectations to handle unique validation scenarios. This project is particularly useful for organizations that need to maintain high data quality standards across various data pipelines.

## Features

- **Great Expectations Integration**: Leverage the robust validation framework of Great Expectations to perform comprehensive data quality checks.
- **Custom Expectations**: Define and execute custom SQL-based validations tailored to your specific data quality requirements.
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
  "custom_expectations": [
    {
      "name": "total_custom",
      "sql": "SELECT COUNT(*) as total from test_db.source_table where age=45",
      "result_columns": [{"total": 1}, {"total": 2}]
    },
    {
      "name": "total_custom2",
      "sql": "SELECT COUNT(*) as total, name, street from test_db.source_table where age=30",
      "result_columns": [{"total": 1, "name": "Joan", "street": "address"}, {"total": 2, "name": "Joan", "street": "address"}]
    }
  ]
}
```

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
     "custom_expectations": [
       {
         "name": "total_custom",
         "sql": "SELECT COUNT(*) as total from test_db.source_table where age=45",
         "result_columns": [{"total": 1}, {"total": 2}]
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

### Example of Validation Results

#### Custom Expectation Result

```plaintext
| expectation_type | kwargs                                 | success | error_message | observed_value | severity | table_name         | execution_date      | validation_id      | validation_time | batch_id         | datasource_name  | dataset_name | expectation_suite_name |
|------------------|----------------------------------------|---------|---------------|----------------|----------|---------------------|---------------------|--------------------|-----------------|------------------|------------------|--------------|------------------------|
| total_custom     | {"sql": "SELECT COUNT(*) as total ..."} | True    |               | {"total": 2}   | info     | test_db.source_table| 2024-05-25 16:57:34 | validation_20240525 | 0.5             | batch_20240525   | mock_datasource  | mock_dataset | default_suite          |
```

#### Great Expectations Result

```plaintext
| expectation_type                 | kwargs                               | success | error_message | observed_value | severity | table_name         | execution_date      | validation_id      | validation_time | batch_id         | datasource_name  | dataset_name | expectation_suite_name |
|----------------------------------|--------------------------------------|---------|---------------|----------------|----------|---------------------|---------------------|--------------------|-----------------|------------------|------------------|--------------|------------------------|
| expect_column_to_exist           | {"column": "name"}                   | True    |               |                | info     | test_db.source_table| 2024-05-25 16:57:34 | validation_20240525 | 0.3             | batch_20240525   | mock_datasource  | mock_dataset | default_suite          |
| expect_column_values_to_not_be_null | {"column": "age"}                | True    |               |                | info     | test_db.source_table| 2024-05-25 16:57:34 | validation_20240525 | 0.3             | batch_20240525   | mock_datasource  | mock_dataset | default_suite          |
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

---