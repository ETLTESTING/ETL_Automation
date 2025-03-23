import logging

# Assuming get_OracleConnect() is already defined elsewhere

from ETL_Testing.Common.Database_Connection import get_OracleConnect

def Oracle_MinusValidation():
    curs = None
    conn_Or = None
    # Define the columns for each table that we are comparing using MINUS
    tables_columns = {
        'source.src_customer': ['CUST_NAME', 'AGE', 'GENDER', 'LOCATION', 'STATE', 'ZIP'],
        'target.dim_customer': ['CUST_NAME', 'AGE', 'GENDER', 'LOCATION', 'STATE', 'ZIP']
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

        # Perform MINUS query for each table
        source_table = 'source.src_customer'
        target_table = 'target.dim_customer'

        print(f"Running MINUS query to find records in {source_table} not in {target_table}.")

        # Build the MINUS query to find differences between the source and target
        columns = tables_columns[source_table]
        select_columns = ", ".join(columns)

        # Construct the MINUS query
        query = f'''
            SELECT {select_columns}
            FROM {source_table}
            MINUS
            SELECT {select_columns}
            FROM {target_table}
        '''

        # Execute the query
        curs.execute(query)

        # Fetch the result
        diff_records = curs.fetchall()

        # Handle and format the result
        if diff_records:
            print(f"Records found in {source_table} but not in {target_table}:")
            for record in diff_records:
                print(f"Record: {record}")
        else:
            print(f"No records found in {source_table} that are not in {target_table}.")

    except Exception as e:
        logging.error(f"Error occurred during Oracle MINUS validation: {e}")
    finally:
        # Ensure cursor and connection are closed properly
        if curs:
            curs.close()
        if conn_Or:
            conn_Or.close()


