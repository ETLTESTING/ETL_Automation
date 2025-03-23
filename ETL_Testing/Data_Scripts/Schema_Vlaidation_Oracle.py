import cx_Oracle

from ETL_Testing.Common.Database_Connection import get_OracleConnect


# Function to fetch table structure
def fetch_table_structure(table_name):

    # Establish Oracle connection
    conn_Or = get_OracleConnect()

    if conn_Or is None:
        print("Failed to connect to the Oracle database.")
        return None

    # Create cursor object
    curs = conn_Or.cursor()

    USER = 'TARGET'
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = '{table_name.upper()}'
    AND OWNER = '{USER.upper()}'
    ORDER BY COLUMN_ID;
    # """
    curs.execute(query)
    table_structure = curs.fetchall()
    curs.close()

    # Format the result into a dictionary
    structure = {}
    for column in table_structure:
        column_name, data_type, nullable = column
        structure[column_name] = {'data_type': data_type, 'is_nullable': nullable}

    return structure


# Function to validate table structure
def validate_table_structure(table_name, expected_structure):
    actual_structure = fetch_table_structure(table_name)

    is_valid = True
    validation_errors = []

    for column_name, expected_details in expected_structure.items():
        if column_name not in actual_structure:
            is_valid = False
            validation_errors.append(f"Missing column: {column_name}")
        else:
            actual_details = actual_structure[column_name]
            if actual_details['data_type'] != expected_details['data_type']:
                is_valid = False
                validation_errors.append(
                    f"Column '{column_name}' has wrong data type: expected {expected_details['data_type']}, found {actual_details['data_type']}")
            if actual_details['is_nullable'] != expected_details['is_nullable']:
                is_valid = False
                validation_errors.append(
                    f"Column '{column_name}' has wrong nullability: expected {expected_details['is_nullable']}, found {actual_details['is_nullable']}")

    # Check for any extra columns in the actual structure
    for column_name in actual_structure:
        if column_name not in expected_structure:
            is_valid = False
            validation_errors.append(f"Extra column: {column_name}")

    return is_valid, validation_errors


# Example expected structure (you can define this or read it from an external source)
expected_structure = {
    'EMP_ID': {'data_type': 'NUMBER', 'is_nullable': 'Y'},
    'LAST_NAME': {'data_type': 'VARCHAR2', 'is_nullable': 'Y'},
    'FIRST_NAME': {'data_type': 'TIMESTAMP', 'is_nullable': 'Y'}
}

# Table name to validate
table_name = 'TGT_EMP'

# Validate the structure of the table
is_valid, errors = validate_table_structure(table_name, expected_structure)

# Output the results
if is_valid:
    print(f"Table structure for '{table_name}' is valid.")
else:
    print(f"Table structure for '{table_name}' is invalid:")
    for error in errors:
        print(f"- {error}")


