import logging

from ETL_Testing.Common.Database_Connection import get_OracleConnect

def Oracle_DuplicateValidation():
    curs = None
    conn_Or = None
    # Define the columns for each table in a dictionary (same as the original structure)
    tables_columns = {
        'TARGET.DIM_CUSTOMER': ['DW_CUST_ID', 'CUST_NAME', 'AGE', 'GENDER', 'LOCATION', 'STATE', 'ZIP', 'STATUS', 'START_DATE', 'END_DATE'],
        'TARGET.DIM_STUDENT': ['SID', 'SNAME', 'ADDRESS', 'PHONE'],
        'TARGET.TGT_EMP': ['EMP_ID', 'LAST_NAME', 'FIRST_NAME', 'SALARRY', 'COMM']
    }

    try:
        # Establish Oracle connection
        conn_Or = get_OracleConnect()

        if conn_Or is None:
            logging.error("Failed to connect to the Oracle database.")
            return None

        # Create cursor object
        curs = conn_Or.cursor()

        logging.info("Database connection established.")

        for table, columns in tables_columns.items():
            # Print or log the current table being checked
            print(f"Checking for duplicate records in table: {table}")

            # Build the GROUP BY clause based on the columns to check for duplicates
            group_by_columns = ", ".join(columns)
            query = f'''
                SELECT {group_by_columns}, COUNT(*) AS duplicate_count
                FROM {table}
                GROUP BY {group_by_columns}
                HAVING COUNT(*) > 1
            '''

            # Execute the query to find duplicate rows
            curs.execute(query)

            # Fetch the result
            duplicates = curs.fetchall()

            # Handle and format the result
            if duplicates:
                # Print duplicate records for the table
                print(f"Duplicate records found in {table}:")
                for duplicate in duplicates:
                    # Print each duplicate row and the count of its occurrences
                    print(f"Duplicate Row: {duplicate[:-1]} - Duplicate Count: {duplicate[-1]}")
            else:
                print(f"No duplicate records found in {table}.")

    except Exception as e:
        logging.error(f"Error occurred during Oracle duplicate record validation: {e}")
    finally:
        # Ensure cursor and connection are closed properly
        if curs:
            curs.close()
        if conn_Or:
            conn_Or.close()

