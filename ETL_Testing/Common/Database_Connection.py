import logging
import snowflake.connector
import cx_Oracle
from Common.config_sf import sf_username, sf_password, sf_account, sf_warehouse, sf_database, sf_schema
from Common.config_sf import Or_username, Or_password, Or_hostname, port, sid

# Set up logging configuration
logging.basicConfig(level=logging.INFO, filename='../../ETL_Testing/Reports/snowflake_log.txt', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')


def get_snowflakeconnection():
    """ Establishes a connection to the Snowflake database. """
    try:
        # Set up connection parameters for Snowflake
        conn_params = {
            "user": sf_username,
            "password": sf_password,
            "account": sf_account,
            "warehouse": sf_warehouse,
            "database": sf_database,
            "schema": sf_schema,
        }
        # Create a connection object
        sf_conn = snowflake.connector.connect(**conn_params)
        logging.info("Successfully connected to Snowflake.")
        return sf_conn
    except Exception as e:
        logging.error(f"Snowflake Database Connection is Failed : {e}")
        return None

def get_OracleConnect():
    """Establishes a connection to the Oracle database."""
    try:
        # Build the Oracle DSN string
        dsn = cx_Oracle.makedsn(Or_hostname, port, sid)
        # Create the Oracle connection
        Or_conn = cx_Oracle.connect(user=Or_username, password=Or_password, dsn=dsn)
        logging.info("Successfully connected to Oracle.")
        return Or_conn
    except Exception as e:
        logging.error(f"Oracle Database Connection is Failed: {e}")
        return None

def close_connection(conn, db_type="Unknown"):
    """ Closes a given database connection.
    Args:
        conn (object): The database connection object to be closed.
        db_type (str): The type of database ('Snowflake' or 'Oracle') for logging purposes.
    """
    try:
        if conn:
            conn.close()
            logging.info(f"Successfully closed the {db_type} connection.")
        else:
            logging.warning(f"{db_type} connection is None, skipping close.")
    except Exception as e:
        logging.error(f"Error occurred while closing {db_type} connection: {e}")



get_snowflakeconnection()
get_OracleConnect()


# After finishing operations, close the connection
# close_connection(get_snowflakeconnection(), "Snowflake")
# close_connection(get_OracleConnect(), "Oracle")