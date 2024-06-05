### Categories of Data Quality Validations

#### 1. Critical
These are validations that, if they fail, indicate serious issues in the data that need to be corrected immediately. Failures at this level can disrupt business processes or cause significant damage.

**Examples:**
- **Schema Validations**: The structure of the data does not match the expected (missing or additional columns).
- **Primary Key Consistency**: Duplication of primary keys that must be unique.
- **Referential Integrity**: Foreign keys that do not correspond to a primary key in another table.

#### 2. High
Validations that are important but can be addressed within a defined time frame. Issues here can lead to inaccurate results or delays.

**Examples:**
- **Missing Data**: Mandatory fields missing in a significant percentage of records.
- **Invalid Values**: Data that is not within the expected or acceptable values (e.g., negative values in quantity columns).

#### 3. Medium
Validations that affect data quality but are not urgent. Failures here can be corrected in regular maintenance cycles.

**Examples:**
- **Formatting Standards**: Email addresses, phone numbers, or other fields that do not follow the expected format.
- **Data Consistency**: Verification that correlated values between different columns are consistent (e.g., state and city).

#### 4. Low
Validations that have a minor impact on overall data quality and can be corrected as part of a continuous improvement cycle.

**Examples:**
- **Duplicate Checks**: Duplication of non-critical records.
- **Optional Data Validation**: Optional fields containing unexpected data or incorrect formats.

### Types of Validations

For the "type" attribute, here are some suggestions that can be used to categorize the validations:

1. **schema**
   - Verification of the data structure, including the presence of mandatory columns and compliance with the expected schema.

2. **uniqueness**
   - Ensuring that certain fields, such as primary keys, are unique across the dataset.

3. **referential_integrity**
   - Validation of referential integrity, ensuring foreign keys match primary keys in other tables.

4. **null_check**
   - Verification of the presence of null values in fields that should not be null.

5. **format**
   - Validation that data follows a specific format, such as email addresses, phone numbers, dates, etc.

6. **range**
   - Ensuring that numeric or date values are within an acceptable range.

7. **consistency**
   - Verification of consistency between correlated columns (e.g., state and city).

8. **completeness**
   - Assessment of data completeness, ensuring that all mandatory fields are filled.

9. **duplicate_check**
   - Identification of records that should not be duplicated.

10. **business_rule**
    - Validation of specific business rules that the data must adhere to.

11. **pattern**
    - Verification that data matches a specific regex pattern.

12. **custom_sql**
    - Custom validations performed through specific SQL queries.

13. **statistical**
    - Validations based on statistics, such as means, medians, standard deviations, etc.

```json
{
  "validations": [
    {
      "type": "schema",
      "level": "Critical",
      "description": "Ensures all mandatory fields are present."
    },
    {
      "type": "uniqueness",
      "level": "Critical",
      "description": "Ensures primary keys are unique."
    },
    {
      "type": "format",
      "level": "Medium",
      "description": "Ensures email addresses follow a valid format."
    },
    {
      "type": "null_check",
      "level": "High",
      "description": "Ensures important fields do not contain null values."
    },
    {
      "type": "referential_integrity",
      "level": "Critical",
      "description": "Ensures foreign keys match primary keys in related tables."
    }
  ]
}
```
