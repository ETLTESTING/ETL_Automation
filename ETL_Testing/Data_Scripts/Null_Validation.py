import logging


from ETL_Testing.Common.Database_Connection import get_OracleConnect

# Example of how to set up basic logging for your script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Assuming get_OracleConnect() is already defined elsewhere
def Oracle_NullValidation():
    curs = None
    conn_Or = None
    # Define the columns for each table in a dictionary
    tables_columns = {
        'TARGET.DIM_CUSTOMER': ['DW_CUST_ID', 'CUST_NAME', 'AGE', 'GENDER', 'LOCATION', 'STATE', 'ZIP', 'STATUS',
                                'START_DATE', 'END_DATE'],
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
            logging.info(f"Checking NULL counts for table: {table}")

            # Build the COUNT(CASE WHEN ... IS NULL) for each column dynamically
            count_columns_sql = ", ".join(
                [f"COUNT(CASE WHEN {col} IS NULL THEN 1 END) AS {col}_null_count" for col in columns])

            # Fetch record count from the current table
            query = f'''
                SELECT {count_columns_sql}
                FROM {table}
            '''
            curs.execute(query)

            # Fetch the result
            tgt_count = curs.fetchall()

            # Handle and format the result
            if tgt_count:
                # Assuming the query returns only one row, let's extract the counts
                result = tgt_count[0]
                #logging.info(f"NULL counts for {table}:")
                print(f"NULL counts for {table}:")
                for idx, col_name in enumerate(columns):
                    #logging.info(f'{col_name} NULL Count: {result[idx]}')
                    print(f'{col_name} NULL Count: {result[idx]}')
            else:
                logging.warning(f"No data returned from the query for {table}.")

    except Exception as e:
        logging.error(f"Error occurred during Oracle record count validation: {e}")
    finally:
        # Ensure cursor and connection are closed properly
        if curs:
            curs.close()
        if conn_Or:
            conn_Or.close()


# # Call the function
# Oracle_NullValidation()
