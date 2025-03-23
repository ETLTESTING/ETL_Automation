import pandas as pd
import logging
import pytest


from ETL_Testing.Common.Database_Connection import get_snowflakeconnection
from ETL_Testing.Common.Database_Connection import get_OracleConnect

# Snowflake Record Count function
def Snowflake_RecordCount(src_sf_table_names, tgt_sf_table_names):
    #print(f"\nSnowflake Record Count Validation for {src_sf_table_names} and {tgt_sf_table_names} *******")
    try:
        # Establish Snowflake connection
        conn_sf = get_snowflakeconnection()

        if conn_sf is None:
            logging.error("Failed to connect to the database.")
            return None

        # Create cursor object
        curs = conn_sf.cursor()

        # Fetch record count from source and target tables
        curs.execute(f"SELECT count(*) FROM {src_sf_table_names}")
        sf_src_count = curs.fetchone()[0]
        curs.execute(f"SELECT count(*) FROM {tgt_sf_table_names}")
        sf_tgt_count = curs.fetchone()[0]
        print(f"Source Record: {sf_src_count} Target Record Count: {sf_tgt_count}")

        return sf_src_count, sf_tgt_count

    except Exception as e:
        logging.error(f"Error occurred during record count validation: {e}")
    finally:
        # Close cursor and connection
        if curs:
            curs.close()
        if conn_sf:
            conn_sf.close()


# Oracle Record Count function
def Oracle_RecordCount(src_Or_table_names, tgt_Or_table_names):
    #print(f"\nOracle Record Count Validation for {src_Or_table_names} and {tgt_Or_table_names} *******")
    curs = None
    conn_Or = None
    try:
        # Establish Oracle connection
        conn_Or = get_OracleConnect()

        if conn_Or is None:
            print("Failed to connect to the Oracle database.")
            return None

        # Create cursor object
        curs = conn_Or.cursor()

        # Fetch record count from source and target tables
        curs.execute(f"SELECT count(*) FROM {src_Or_table_names}")
        src_count = curs.fetchone()[0]
        curs.execute(f"SELECT count(*) FROM {tgt_Or_table_names} where status = 'ACTIVE'")
        tgt_count = curs.fetchone()[0]
        print(f"Source Record: {src_count} Target Record Count: {tgt_count}")

        return src_count, tgt_count

    except Exception as e:
        logging.error(f"Error occurred during Oracle record count validation: {e}")
    finally:
        # Ensure cursor and connection are closed properly
        if curs:
            curs.close()
        if conn_Or:
            conn_Or.close()


