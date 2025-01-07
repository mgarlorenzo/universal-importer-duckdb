# Universal CSV Importer with DuckDB

## Overview
This project provides a dynamic CSV importer and transformation tool using DuckDB and Python. It supports schema validation, custom validation rules, duplicate removal, and projections into clean data tables or views. The tool is configurable through a `config.yaml` file, enabling dynamic adjustments for different data sources.

---

## Features

- **Dynamic CSV Import**: Process CSV files defined in `config.yaml`.
- **Schema Validation**: Validate CSV data against field types, patterns, and required constraints using Pydantic.
- **Custom Validations**: Support custom rules, like age checks, defined in the configuration.
- **Deduplication**: Automatically remove duplicates based on composite keys.
- **Projections**: Create clean tables or views based on defined queries and aliases.
- **Export to CSV**: Save processed views or tables to the `exported_views` folder.

---

## Prerequisites

- Python 3.8 or higher
- DuckDB installed via Python (`pip install duckdb`)
- Pydantic for schema validation (`pip install pydantic`)
- PyYAML for configuration parsing (`pip install pyyaml`)

---

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/universal-importer.git
   cd universal-importer

2. Install the required dependencies:

    ```bash
    pip install -r requirements.txt

---
## Configuration

### `config.yaml`

Define your entity transformations, validation rules, and projections in the `config.yaml` file. Below is an example configuration:

```yaml
transformations_config:
  employees:
    source: "./input_data/employees.csv"
    settings:
      duplicate_resolution: "last"
      custom_validation_mode: "skip"
      unique_composite:
        - ["employee_id", "company_id"]

    projections:
      - name: personal_data
        type: "table"
        query: |
          SELECT employee_id, company_id, first_name, last_name, email, birthday_on, country FROM employees
      - name: contract_data
        type: "table"
        query: |
          SELECT employee_id, starts_on, ends_on, salary_amount FROM employees

    validations:
      schema:
        fields:
          employee_id: {"type": "int", "required": true}
          company_id: {"type": "int", "required": true}
          first_name: {"type": "str", "required": true}
          birthday_on: {"type": "str", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
      custom:
        rules:
          - field: birthday_on
            validation: "age_gte"
            params:
              min_age: 18
```

### Explanation of Sections

1. **`source`**:
   - Specifies the path to the source CSV file for the entity. This is where the raw data for processing is located.

2. **`settings`**:
   - **`duplicate_resolution`**:
     - Defines how duplicates are resolved during data deduplication.
     - Options:
       - `first`: Keeps the first occurrence of a duplicate.
       - `last`: Keeps the last occurrence of a duplicate.
       - `exclude_all`: Removes all duplicates.
   - **`custom_validation_mode`**:
     - Specifies the behavior when custom validation errors occur.
     - Options:
       - `stop`: Stops processing if validation errors are found.
       - `skip`: Skips rows with validation errors and continues processing.
   - **`unique_composite`**:
     - Defines the composite keys used for deduplication.
     - Example: `["employee_id", "company_id"]` ensures unique rows based on these columns.

3. **`projections`**:
   - Defines the transformations or extractions to perform on the validated data.
   - Each projection includes:
     - **`name`**: Name of the projection (used as the output file name).
     - **`type`**: Type of projection (`table` or `view`).
     - **`query`**: SQL query to define the projection logic.
     - **`aliases`** (optional): Aliases for column names in the projection.

4. **`validations`**:
   - **`schema`**:
     - Defines schema validation rules for each field.
     - Includes:
       - `type`: The expected data type (e.g., `int`, `str`, `float`).
       - `required`: Whether the field is mandatory.
       - `pattern`: Regex pattern to validate the field format.
   - **`custom`**:
     - Defines custom validation rules.
     - Each rule specifies:
       - `field`: The column to validate.
       - `validation`: The type of validation (e.g., `age_gte`).
       - `params`: Additional parameters for validation (e.g., `min_age` for `age_gte`).

---

### Usage

To use the dynamic CSV processing tool, follow these steps:

1. **Prepare the Configuration**:
   - Ensure the `config.yaml` file is configured correctly with the entities, source files, schema validations, custom validations, deduplication settings, and projections.

2. **Run the Script**:
   - Execute the script by specifying the entity you want to process. The script will automatically use the `source` file path specified in the `config.yaml` file.

   ```bash
   python main.py employees --config config.yaml --output_dir output

3. Arguments:
  - **`entit`**: The name of the entity to process (e.g., `employees`, `locations`).
  - **`--config`**: Path to the YAML configuration file. Defaults to `config.yaml`.
  - **`--output_dir`**: Directory where output files (exports and errors) will be saved. Defaults to `output`.

4. Output:

  - The processed data will be stored in the specified `output_dir`.
  - Errors and projections will be exported into structured directories for easy review.

5. Example: To process the `employees` entity using the provided configuration file:
    
   ```bash
   python main.py employees
   ```
   This will:

    - Validate the schema and custom rules for `employees`.
    - Deduplicate data based on the `unique_composite` settings.
    - Apply transformations and create projections.
    - Save output to `output/exports` and errors to `output/errors`.

---

### Output

After processing, the tool generates the following outputs in the specified `output_dir`:

1. **Processed Data**:
   - Validated and deduplicated data is stored in DuckDB tables for further analysis and projection.

2. **Exports**:
   - The projections specified in the configuration are exported to CSV files under the `exports` directory.
   - Path: `output/exports/<projection_name>.csv`

   Example:
   - `output/exports/personal_data.csv`
   - `output/exports/contract_data.csv`

3. **Errors**:
   - Any validation or processing errors are saved in the `errors` directory for review.
   - Path: `output/errors/<entity>_<error_type>_errors.csv`

   Example:
   - `output/errors/employees_schema_validation_errors.csv`
   - `output/errors/employees_custom_birthday_on_errors.csv`
   - `output/errors/employees_duplicates_errors.csv`

4. **Logs**:
   - The script prints detailed logs to the console, summarizing:
     - Total rows processed.
     - Schema validation errors.
     - Rows removed due to deduplication.
     - Custom validation errors.
     - Projections created and exported.

5. **Summary**:
   - A processing summary is displayed at the end of the script execution, including:
     - Total rows processed.
     - Number of valid rows inserted.
     - Number of validation errors (schema and custom).
     - Number of rows removed as duplicates.
     - Projection summary with the row count for each projection.

   Example Summary:
   ```plaintext
   Processing Summary:
   Total rows processed: 12
   Total valid rows inserted into raw table: 10
   Total rows with schema validation errors: 2
   Total rows with custom validation errors: 1
   Total duplicate rows removed: 2

   Projection Summary:
     personal_data (table): 10 rows
     contract_data (table): 10 rows

## Tests

This project includes a comprehensive test suite to validate the functionality of the CSV Loader and View Generator.

### Running the Tests

Use the following command to discover and execute all tests:

  ```bash
  python -m unittest discover -s tests -v
  ```

### Test Coverage
The test suite covers the following areas:

- Configuration Validation: Ensures invalid configurations are caught and proper error messages are displayed.
- Custom Validations: Tests specific validation rules, such as ensuring employee age is greater than or equal to a certain value.
