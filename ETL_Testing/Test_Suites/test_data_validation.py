import pandas as pd
import logging
import pytest

from ETL_Testing.Data_Scripts.Record_Count import Oracle_RecordCount
from ETL_Testing.Data_Scripts.Record_Count import Snowflake_RecordCount
from ETL_Testing.Data_Scripts.Null_Validation import Oracle_NullValidation
from ETL_Testing.Data_Scripts.Duplicate import Oracle_DuplicateValidation
from ETL_Testing.Data_Scripts.Data_Validation import Oracle_MinusValidation
from ETL_Testing.Data_Scripts.Schema_Validation import validate_table_structure

# File path for table names Excel sheet
file_path = 'C:/Users/KCM/Desktop/TableNames.xlsx'

# Read table names from an Excel file
def read_table_names_from_excel(file_path):
    # Load the Excel file
    df = pd.read_excel(file_path)

    # Strip any extra spaces in column names
    df.columns = df.columns.str.strip()

    # Print the columns for debugging
    print("Column names in the Excel file:", df.columns)  # Print column names

    # Extract table names for Snowflake and Oracle
    src_sf_table_names_list = df['src_sf_table_names'].tolist()
    tgt_sf_table_names_list = df['tgt_sf_table_names'].tolist()
    src_Or_table_names_list = df['src_Or_table_names'].tolist()
    tgt_Or_table_names_list = df['tgt_Or_table_names'].tolist()

    return src_sf_table_names_list, tgt_sf_table_names_list, src_Or_table_names_list, tgt_Or_table_names_list

# List of table names fetched from Excel
src_sf_table_names_list, tgt_sf_table_names_list, src_Or_table_names_list, tgt_Or_table_names_list = read_table_names_from_excel(file_path)


# Test case for Oracle record count for multiple tables
@pytest.mark.parametrize("src_table, tgt_table", zip(src_Or_table_names_list, tgt_Or_table_names_list))
def test_Oracle_RecordCount(src_table, tgt_table):
    print(f"\n{'=' * 80}")
    print(f"Running Oracle Record Count Validation for {src_table} and {tgt_table}")
    print(f"{'=' * 80}")
    # Fetch the record counts for each table
    src_count, tgt_count = Oracle_RecordCount(src_table, tgt_table)

    # Check if record counts are fetched, if not, fail the test
    if src_count is None or tgt_count is None:
        pytest.fail(f"Failed to get record counts for {src_table} and {tgt_table}")

    # Assert that source and target record counts match
    assert src_count == tgt_count, f"Source: {src_count}, Target: {tgt_count}. Counts don't match for {src_table} and {tgt_table}."

    print(f"\n{'=' * 80}")
    print(f"Oracle Record Count Validation Complete for {src_table} and {tgt_table}")
    print(f"{'=' * 80}\n")

# Test case for Snowflake record count for multiple tables
@pytest.mark.parametrize("src_table, tgt_table", zip(src_sf_table_names_list, tgt_sf_table_names_list))
def test_Snowflake_RecordCount(src_table, tgt_table):
    print(f"\n{'=' * 80}")
    print(f"Running Snowflake Record Count Validation for {src_table} and {tgt_table}")
    print(f"{'=' * 80}")
    # Fetch the record counts for each table
    src_count, tgt_count = Snowflake_RecordCount(src_table, tgt_table)

    # Check if record counts are fetched, if not, fail the test
    if src_count is None or tgt_count is None:
        pytest.fail(f"Failed to get record counts for {src_table} and {tgt_table}")

    # Assert that source and target record counts match
    assert src_count == tgt_count, f"Source: {src_count}, Target: {tgt_count}. Counts don't match for {src_table} and {tgt_table}."

    print(f"\n{'=' * 80}")
    print(f"Snowflake Record Count Validation Complete for {src_table} and {tgt_table}")
    print(f"{'=' * 80}\n")


def test_oracle_null_validation():
    """
    Integration test for Oracle_NullValidation using a real Oracle database connection.
    Assumes that the Oracle database is running and accessible.
    """
    # Run the Oracle_NullValidation function to check the NULL counts
    print(f"\n{'=' * 80}")
    print(f"Running Oracle Null Validation")
    print(f"{'=' * 80}")
    Oracle_NullValidation()


def test_Oracle_DuplicateValidation():
    """
    Integration test for Oracle_DuplicateValidation using a real Oracle database connection.
    Assumes that the Oracle database is running and accessible.
    """
    # Run the Oracle_DuplicateValidation function to check the Duplicates counts
    print(f"\n{'=' * 80}")
    print(f"Running Oracle Duplicates")
    print(f"{'=' * 80}")
    Oracle_DuplicateValidation()

def test_Oracle_MinusValidation():
    """
    Integration test for Oracle_MinusValidation using a real Oracle database connection.
    Assumes that the Oracle database is running and accessible.
    """
    # Run the Oracle_MinusValidation function to check the Duplicates counts
    print(f"\n{'=' * 80}")
    print(f"Running Oracle Data Validation using Minus Query")
    print(f"{'=' * 80}")
    Oracle_MinusValidation()


def test_SchemaValidation():
    """
    Integration test for test_SchemaValidation using a real Oracle database connection.
    Assumes that the Oracle database is running and accessible.
    """
    # Run the test_SchemaValidation function to check the Duplicates counts

    print(f"\n{'=' * 80}")
    print(f"Running Snowflake Table Sturcture Validation ")
    print(f"{'=' * 80}")

    # Example expected structure (define based on your schema)
    expected_structure = {
        'EMPID': {'data_type': 'NUMBER', 'is_nullable': 'YES'},
        'ENAME': {'data_type': 'TEXT', 'is_nullable': 'YES'},
        'SALARY': {'data_type': 'NUMBER', 'is_nullable': 'YES'}
    }

    # Validate the structure of a table
    table_name = 'EMP'
    is_valid, errors = validate_table_structure(table_name, expected_structure)

    # Output the results
    if is_valid:
        print(f"Table structure for '{table_name}' is valid.")
    else:
        print(f"Table structure for '{table_name}' is invalid:")
        for error in errors:
            print(f"- {error}")











