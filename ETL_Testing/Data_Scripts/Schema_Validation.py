import logging


from ETL_Testing.Common.Database_Connection import get_snowflakeconnection
from ETL_Testing.Common.config_sf import sf_schema


# Function to fetch table structure
def fetch_table_structure(table_name):

    # Establish Snowflake connection
    conn_sf = get_snowflakeconnection()

    if conn_sf is None:
        logging.error("Failed to connect to the database.")
        return None

    # Create cursor object
    curs = conn_sf.cursor()

    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{sf_schema}'
    ORDER BY ORDINAL_POSITION;
    """

    curs.execute(query)
    table_structure = curs.fetchall()
    curs.close()

    # Format the result into a dictionary
    structure = {}
    for column in table_structure:
        column_name, data_type, is_nullable = column
        structure[column_name] = {'data_type': data_type, 'is_nullable': is_nullable}

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





