import unittest
import pandas as pd
from main import load_config, validate_config,execute_custom_validations
import subprocess
import duckdb
import os


class TestConfigurationValidation(unittest.TestCase):
    def setUp(self):
        self.valid_config = "tests/test_data/valid_config.yaml"
        self.invalid_config = "tests/test_data/invalid_config.yaml"

    def test_valid_config(self):
        config = load_config(self.valid_config)
        entity_details = validate_config(config, "employees")
        self.assertIn("source", entity_details)
        self.assertIn("validations", entity_details)

    def test_missing_entity(self):
        config = load_config(self.valid_config)
        with self.assertRaises(KeyError):
            validate_config(config, "nonexistent_entity")

    def test_invalid_config(self):
        with self.assertRaises(KeyError):
            config = load_config(self.invalid_config)
            validate_config(config, "employees")

class TestCustomValidations(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        self.conn.execute("""
        CREATE TABLE employees_stage (
            employee_id INT,
            birthday_on DATE
        )
        """)
        self.conn.execute("""
        INSERT INTO employees_stage VALUES
            (1, '2000-01-01'),
            (2, '2010-01-01'),
            (3, '1995-05-15')
        """)
        self.custom_validations = {
            "birthday_on": {"validation": "age_gte", "params": {"min_age": 18}}
        }

    def tearDown(self):
        self.conn.close()

    def test_age_validation(self):
        # Expected: One row invalid due to age_gte validation
        with self.assertRaises(ValueError) as context:
            execute_custom_validations(
                self.conn, "employees", self.custom_validations, "stop"
            )
        self.assertIn("Custom validation failed for field 'birthday_on'", str(context.exception))