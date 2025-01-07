import duckdb
import pandas as pd
import yaml
import argparse
from datetime import datetime
from pydantic import BaseModel, Field, ValidationError
from typing import Dict, Any, Type
import os

def load_config(config_file) -> Dict:
    """Load YAML configuration."""
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

def validate_config(config, entity):
    """Validate that the required configurations are present for the given entity."""
    if entity not in config.get("transformations_config", {}):
        raise KeyError(f"Error: Entity '{entity}' not found in the configuration.")

    entity_details = config["transformations_config"][entity]
    required_keys = ["source", "settings", "validations"]
    for key in required_keys:
        if key not in entity_details:
            raise KeyError(f"Missing required configuration '{key}' for entity '{entity}'.")
    
    # Validate mandatory fields in settings
    settings = entity_details.get("settings", {})
    if "duplicate_resolution" not in settings:
        raise KeyError(f"Missing 'duplicate_resolution' in settings for entity '{entity}'.")
    if "custom_validation_mode" not in settings:
        raise KeyError(f"Missing 'custom_validation_mode' in settings for entity '{entity}'.")

    return entity_details

def create_pydantic_model(name: str, schema: Dict[str, Any]) -> Type[BaseModel]:
    """Create a Pydantic model dynamically from schema definition."""
    annotations = {}
    fields = {}

    for field_name, rules in schema.items():
        field_type = eval(rules.get("type", "str"))  # Convert type string to Python type
        field_args = {}

        if rules.get("required", False):
            field_args["default"] = ...
        if "pattern" in rules:
            field_args["pattern"] = rules["pattern"]
        if "min" in rules and field_type in (int, float):
            field_args["ge"] = rules["min"]

        annotations[field_name] = field_type
        fields[field_name] = Field(**field_args)

    return type(name, (BaseModel,), {"__annotations__": annotations, **fields})

def validate_records_with_pydantic(df, model, custom_validations):
    """Validate DataFrame records using Pydantic and custom rules."""
    df = df.fillna({
        'trial_period_ends_on': '',
        'ends_on': '',
        'es_contract_observations': '',
        'pt_contract_type_id': 0,
    })

    valid_records = []
    error_records = []

    for index, record in df.iterrows():
        try:
            validated_record = model(**record.to_dict())
            valid_records.append(validated_record.dict())
        except (ValidationError, ValueError) as e:
            error_message = []
            if isinstance(e, ValidationError):
                error_message = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            else:
                error_message.append(str(e))
            error_records.append({"row": index + 1, "data": record.to_dict(), "errors": error_message})

    print(f"Validated {len(valid_records)} records successfully.")
    print(f"Found {len(error_records)} invalid records.")
    return pd.DataFrame(valid_records), error_records

def validate_format_with_pydantic(df, schema_definition, custom_validations):
    """Validate basic format and schema using Pydantic."""
    DynamicModel = create_pydantic_model("DynamicModel", schema_definition)
    valid_records, error_records = validate_records_with_pydantic(df, DynamicModel, custom_validations)
    return valid_records, error_records

def load_raw_table(conn, table, df):
    """Load raw validated DataFrame into DuckDB."""
    conn.execute(f"DROP TABLE IF EXISTS {table}_raw")
    conn.register(f"{table}_raw", df)
    conn.execute(f"CREATE TABLE {table}_raw AS SELECT * FROM {table}_raw")
    print(f"Loaded raw data into table '{table}_raw'.")

def remove_duplicates_from_stage_table(conn, table, unique_composite, duplicate_resolution="first"):
    """Remove duplicates in DuckDB based on unique composite keys."""
    conn.execute(f"CREATE TABLE {table}_stage AS SELECT * FROM {table}_raw")
    duplicate_rows = []

    print(f"Starting deduplication for table '{table}_stage'. Initial row count:")
    initial_count = conn.execute(f"SELECT COUNT(*) FROM {table}_stage").fetchone()[0]
    print(f"Initial rows in stage table: {initial_count}")

    for composite_key in unique_composite:
        group_columns = ", ".join(composite_key)
        if duplicate_resolution == "exclude_all":
            duplicates = conn.execute(f"""
                SELECT *
                FROM {table}_stage
                WHERE ROW({group_columns}) IN (
                    SELECT ROW({group_columns})
                    FROM {table}_stage
                    GROUP BY {group_columns}
                    HAVING COUNT(*) > 1
                )
            """).fetchdf()

            duplicate_rows.append(duplicates)
            conn.execute(f"""
                DELETE FROM {table}_stage
                WHERE ROW({group_columns}) IN (
                    SELECT ROW({group_columns})
                    FROM {table}_stage
                    GROUP BY {group_columns}
                    HAVING COUNT(*) > 1
                )
            """)
            print(f"Removed all duplicates for composite key: {composite_key}")
        elif duplicate_resolution in ("first", "last"):
            duplicates = conn.execute(f"""
                SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {group_columns} ORDER BY ROWID) AS rn
                    FROM {table}_stage
                ) sub
                WHERE rn > 1
            """).fetchdf()

            duplicate_rows.append(duplicates)
            conn.execute(f"""
                DELETE FROM {table}_stage
                WHERE ROWID IN (
                    SELECT ROWID
                    FROM (
                        SELECT ROWID, ROW_NUMBER() OVER (PARTITION BY {group_columns} ORDER BY ROWID) AS rn
                        FROM {table}_stage
                    ) sub
                    WHERE rn > 1
                )
            """)
            print(f"Removed duplicates for composite key {composite_key} (keeping '{duplicate_resolution}').")

    final_count = conn.execute(f"SELECT COUNT(*) FROM {table}_stage").fetchone()[0]
    print(f"Final rows in stage table after deduplication: {final_count}")

    duplicate_rows = pd.concat(duplicate_rows) if duplicate_rows else pd.DataFrame()
    return duplicate_rows

def execute_custom_validations(conn, table, custom_validations, validation_mode):
    validation_issues = []
    total_invalid_rows = 0

    for field, validation in custom_validations.items():
        validation_type = validation.get("validation")
        if validation_type == "age_gte":
            min_age = validation["params"].get("min_age", 0)
            today = datetime.today().strftime("%Y-%m-%d")

            # Identify rows failing the validation
            invalid_rows = conn.execute(f"""
                SELECT *
                FROM {table}_stage
                WHERE DATE_PART('year', AGE(DATE '{today}', CAST({field} AS DATE))) < {min_age}
            """).fetchdf()
            total_invalid_rows += len(invalid_rows)

            if not invalid_rows.empty:
                validation_issues.append({
                    "field": field,
                    "validation": "age_gte",
                    "invalid_rows": invalid_rows
                })

                if validation_mode == "stop":
                    raise ValueError(f"Custom validation failed for field '{field}' with age_gte.")

                if validation_mode == "skip":
                    conn.execute(f"""
                        DELETE FROM {table}_stage
                        WHERE DATE_PART('year', AGE(DATE '{today}', CAST({field} AS DATE))) < {min_age}
                    """)
                    print(f"Skipped invalid rows for custom validation '{field}' with age_gte (min_age={min_age}).")
        print(f"Running custom validation: {validation['validation']} on field: {field}")

    return validation_issues, total_invalid_rows

def remove_composite_duplicates(df, unique_composite, resolution="first"):
    """Remove duplicate rows based on unique composite constraints."""
    removed_rows = []

    for group in unique_composite:
        if resolution == "exclude_all":
            # Identify all duplicates, including the first occurrence
            duplicates = df[df.duplicated(subset=group, keep=False)]
            removed_rows.append(duplicates)

            # Remove all duplicates, including the first occurrence
            df = df[~df.duplicated(subset=group, keep=False)]
        else:
            # Identify duplicates based on the resolution
            duplicates = df[df.duplicated(subset=group, keep="first" if resolution == "last" else "last")]
            removed_rows.append(duplicates)

            # Drop duplicates, keeping the resolved occurrence
            df = df.drop_duplicates(subset=group, keep=resolution)

    # Combine all removed duplicates into one DataFrame
    removed_rows = pd.concat(removed_rows) if removed_rows else pd.DataFrame()
    return df, removed_rows

def apply_aliases(query, aliases, schema):
    """Apply field aliases to the query and validate against the schema."""
    if not aliases:
        return query

    for original, alias in aliases.items():
        if original not in schema:
            raise ValueError(f"Field '{original}' in aliases is not defined in the schema.")
        query = query.replace(f"{original}", f"{original} AS {alias}")
    return query

def create_views_with_projection(conn, table, projections, schema):
    """Create projection views dynamically for the table with alias validation."""
    base_table = f"{table}_stage"  # Use the stage table as the source
    for proj_details in projections:
        if proj_details.get("type") != "view":
            continue

        view_name = proj_details["name"]
        query = proj_details["query"]
        aliases = proj_details.get("aliases", {})

        # Validate configuration
        if not query:
            print(f"Warning: No query defined for view '{view_name}' in table '{table}'. Skipping.")
            continue

        # Replace table reference in the query
        query = query.replace(f"{table}", base_table)

        # Apply aliases to the query
        try:
            query = apply_aliases(query, aliases, schema)
        except ValueError as e:
            print(f"Error in view '{view_name}' for table '{table}': {e}. Skipping.")
            continue

        # Create or replace the view
        try:
            conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS {query}")
            print(f"Created view '{view_name}' for table '{table}'.")
        except Exception as e:
            print(f"Failed to create view '{view_name}': {e}")

def create_tables_with_projection(conn, table, projections, schema):
    """Create projection tables dynamically with alias validation."""
    base_table = f"{table}_stage"  # Use the stage table as the source
    for proj_details in projections:
        if proj_details.get("type") != "table":
            continue

        table_name = proj_details["name"]
        query = proj_details["query"]
        aliases = proj_details.get("aliases", {})

        # Validate configuration
        if not query:
            print(f"Warning: No query defined for table '{table_name}' in table '{table}'. Skipping.")
            continue

        # Replace table reference in the query
        query = query.replace(f"{table}", base_table)

        # Apply aliases to the query
        try:
            query = apply_aliases(query, aliases, schema)
        except ValueError as e:
            print(f"Error in table '{table_name}' for table '{table}': {e}. Skipping.")
            continue

        # Create the projection table
        try:
            conn.execute(f"CREATE TABLE {table_name} AS {query}")
            print(f"Created projection table '{table_name}' for table '{table}'.")
        except Exception as e:
            print(f"Failed to create table '{table_name}': {e}")

def export_views_to_csv(conn, entity, projections, output_dir):
    """
    Export views or projection tables to CSV files.

    :param conn: DuckDB connection
    :param entity: The entity name (used for identifying projections)
    :param projections: List of projection definitions (views or tables)
    :param output_dir: Directory to save the exported CSV files
    """
    export_dir = os.path.join(output_dir, "exports")
    os.makedirs(export_dir, exist_ok=True)

    for proj_details in projections:
        projection_name = proj_details["name"]
        projection_type = proj_details["type"]

        # Determine the export source: view or table
        if projection_type not in ["view", "table"]:
            print(f"Skipping unsupported projection type '{projection_type}' for projection '{projection_name}'.")
            continue

        output_path = os.path.join(export_dir, f"{projection_name}.csv")
        try:
            conn.execute(f"COPY (SELECT * FROM {projection_name}) TO '{output_path}' WITH (HEADER, DELIMITER ',')")
            print(f"Exported {projection_type} '{projection_name}' to '{output_path}'.")
        except Exception as e:
            print(f"Failed to export {projection_type} '{projection_name}': {e}")

def get_projection_summary(conn, projections):
    """Generate a summary of rows in each projection."""
    summary = {}
    for projection in projections:
        name = projection["name"]
        projection_type = projection["type"]
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            summary[name] = {
                "type": projection_type,
                "rows": row_count
            }
        except Exception as e:
            print(f"Warning: Could not fetch row count for {name}: {e}")
            summary[name] = {
                "type": projection_type,
                "rows": "Error"
            }
    return summary

def generate_summary(total_rows, valid_rows, schema_errors, custom_issues, duplicate_rows, projections, conn):
    """Generate and print a detailed summary of the process."""
    print("\nProcessing Summary:")
    print(f"Total rows processed: {total_rows}")
    print(f"Total valid rows inserted into raw table: {valid_rows}")
    print(f"Total rows with schema validation errors: {len(schema_errors)}")
    print(f"Total rows with custom validation errors: {custom_issues}")
    print(f"Total duplicate rows removed: {len(duplicate_rows) if duplicate_rows is not None else 0}")

    print("\nProjection Summary:")
    for projection in projections:
        name = projection["name"]
        projection_type = projection["type"]
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  {name} ({projection_type}): {row_count} rows")
        except Exception as e:
            print(f"  {name} ({projection_type}): Error fetching row count ({e})")

def save_errors(data, error_type, entity, output_dir):
    """
    Save errors to separate CSV files based on the type of error.
    
    :param data: DataFrame or list of error records to save.
    :param error_type: Type of error (e.g., "schema_validation", "custom_validation", "duplicates").
    :param entity: The entity name (used in the file name).
    :param output_dir: Directory to save the error files.
    """
    error_dir = os.path.join(output_dir, "errors")
    os.makedirs(error_dir, exist_ok=True)

    if isinstance(data, pd.DataFrame):
        if not data.empty:
            error_path = os.path.join(error_dir, f"{entity}_{error_type}_errors.csv")
            data.to_csv(error_path, index=False)
            print(f"{error_type.capitalize()} errors saved to '{error_path}'.")
        else:
            print(f"No {error_type} errors to save.")
    elif isinstance(data, list) and data:
        # For structured error records like validation errors
        error_data = pd.DataFrame([
            {
                "row": error["row"],
                "errors": "; ".join(error["errors"]),
                **error["data"]
            } for error in data
        ])
        error_path = os.path.join(error_dir, f"{entity}_{error_type}_errors.csv")
        error_data.to_csv(error_path, index=False)
        print(f"{error_type.capitalize()} errors saved to '{error_path}'.")
    else:
        print(f"No {error_type} errors to save.")

def main():
    parser = argparse.ArgumentParser(description="Dynamic CSV Loader and View Generator")
    parser.add_argument("entity", type=str, help="The entity name to process from the YAML configuration.")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to the YAML configuration file.")
    parser.add_argument("--output_dir", type=str, default="output", help="Directory to save output files.")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Validate configuration
    entity_details = validate_config(config, args.entity)

    csv_file = entity_details["source"]
    settings = entity_details.get("settings", {})
    projections = entity_details.get("projections", [])
    validations = entity_details.get("validations", {})
    schema_fields = validations.get("schema", {}).get("fields", {})
    custom_validations = {rule["field"]: rule for rule in validations.get("custom", {}).get("rules", [])}
   
    # Settings
    duplicate_resolution = settings.get("duplicate_resolution", "first")
    custom_validation_mode = settings.get("custom_validation_mode", "stop")
    unique_composite = settings.get("unique_composite", [])
    
    
    conn = duckdb.connect()

    try:
        # Step 1: Validate format (schema validation)
        print("Validating schema...")
        df = pd.read_csv(csv_file)
        valid_records, schema_errors = validate_format_with_pydantic(df, schema_fields, custom_validations)

        if schema_errors:
            save_errors(schema_errors, "schema_validation", args.entity, args.output_dir)
            print(f"Schema validation errors found. {len(schema_errors)} records failed.")
            if custom_validation_mode == "stop":
                return

        # Step 2: Load raw table
        print("Loading raw table...")
        load_raw_table(conn, args.entity, valid_records)

        # Step 3: Deduplicate rows
        print("Removing duplicates...")
        duplicate_rows = remove_duplicates_from_stage_table(
            conn,
            args.entity,
            unique_composite,
            duplicate_resolution=duplicate_resolution
        )
        if not duplicate_rows.empty:
            save_errors(duplicate_rows, "duplicates", args.entity, args.output_dir)

        # Step 4: Apply custom validations on stage table
        print("Executing custom validations...")
        validation_issues, total_invalid_rows = execute_custom_validations(
            conn, 
            args.entity, 
            custom_validations, 
            custom_validation_mode
        )

        # Save custom validation issues
        for issue in validation_issues:
            invalid_rows = issue["invalid_rows"]
            if not invalid_rows.empty:
                save_errors(invalid_rows, f"custom_{issue['field']}", args.entity, args.output_dir)


        # Step 5: Create projections
        print("Creating projections...")
        create_views_with_projection(conn, args.entity, projections, schema_fields)
        create_tables_with_projection(conn, args.entity, projections, schema_fields)

        # Step 6: Export projections
        print("Exporting projections to CSV...")
        export_views_to_csv(conn, args.entity, projections, args.output_dir)

        # Step 7: Generate projection summary
        print("Generating summary...")
        projection_summary = get_projection_summary(conn, projections)

        # Generate and print the detailed summary
        generate_summary(
            total_rows=len(df),
            valid_rows=len(valid_records),
            schema_errors=schema_errors,
            custom_issues=total_invalid_rows,
            duplicate_rows=duplicate_rows,
            projections=projections,
            conn=conn
        )

    except KeyError as ke:
        print(f"Configuration Error: {ke}")
    except ValueError as ve:
        print(f"Validation Error: {ve}")
    except FileNotFoundError as fe:
        print(f"File Error: {fe}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

