"""This module are the general functions for using Hana and ODBC connector"""

import os
import math
import urllib
import warnings
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, exc

MILLNAMES = ['', 'k', 'mn', 'bn', 'tn']

def load_env():
    """ Load .env file into namespace
    """
    try:
        load_dotenv(find_dotenv(), verbose=True)
    except UserWarning as user_e:
        print(f'Cannot find file. \n{user_e}')
    except Exception as general_e:
        print(general_e)

def get_hana_connection_details(user: str, pwd: str, host: str, port: str):
    """
    Get Hana database credentials from .env file.

    Args:
        user (str): .env file Hana user key
        pwd: .env file Hana password key
        host: .env file Hana database hostname key
        port: .env file Hana database port key

    Returns:
        user_value, pwd_value, host_value, port_value (str)
    """
    user_value = os.getenv(user)
    pwd_value = os.getenv(pwd)
    host_value = os.getenv(host)
    port_value = os.getenv(port)

    return user_value, pwd_value, host_value, port_value

def create_hana_engine(user: str, pwd: str, host: str, port: str,
                       use_env: bool = True):
    """
    Description:
        Creating a Hana engine connecting to the given database.
        This function accepts both using the env_path to authenticate given the correct keys,
        or provide values as function arguments.
    Parameters:
        user (str): if env_path is given, use the key defined;
        host (str): if use_env, use the key defined; otherwise, use the
        hostname given.
        port (str): if use_env, use the key defined; otherwise, user the server
        port given.
        use_env (bool): if not None, use the .env values to create the engine
    """

    if use_env is not None:
        user, pwd, host, port = get_hana_connection_details(user, pwd, host,
                                                            port)

    connection_string = 'hana+pyhdb://{}:{}@{}:{}/'.format(user, pwd, host, port)

    if None in (user, pwd, host, port):
        print('Connection detail loaded incorrectly, please check .env is '
              'loaded.')
    else:
        print('Connecting to: {}...'.format(host))

        try:
            engine = create_engine(connection_string)
            engine.connect()
        except ValueError as val_e:
            print('{}: likely port to be incorrect.'.format(val_e))
        except Exception as general_e:
            print(general_e)
        else:
            print('Connection successful. Engine create as: {}'.format(engine))
            return engine

def get_sqlserver_connection_details(server: str) -> str:
    """
    Get SQL Server server name
    Args:
         server (str): The SQL Server key stored in the .env file

    Returns:
        server (str): The SQL Server name
    """
    load_env()
    server = os.getenv(server)
    if server is None:
        warnings.warn('Server is not defined, check server .env key.')
    return server

def create_sqlserver_engine(server: str, database: str, use_env: bool = True, \
                            driver='{ODBC Driver 17 for SQL Server}'):
    """
    Description:
        Creating a SQL Server engine given server and database.
        This function requires a trusted connection with Kerberos as well as a valid .env file.
        The default is ODBC Driver for SQL Server.
    Parameters:
        server (str): server key in the env file
        database (str): database name
        driver (str): All available Microsoft ODBC drivers
    Returns:
        engine (sqlalchemy.engine.base.Engine):
    """
    if use_env:
        server = get_sqlserver_connection_details(server)

    params = urllib.parse.quote_plus(f'DRIVER={driver};'
                                     f'SERVER={server};'
                                     f'DATABASE={database};'
                                     f'Trusted_Connection=yes;')
    connection_string = f'mssql+pyodbc:///?odbc_connect={params}'

    try:
        engine = create_engine(connection_string)
        engine.connect()
    except exc.DBAPIError as e:
        print(f'Possible authentication error, '
              f'please enable your kerberos with kinit and try again \n {e}.')
    except Exception as general_e:
        print(general_e)

    return engine

def millify(num):
    """
    Convert a long number to a human readable form, such as 1e6 as 1 mn.

    Parameters:
        n (int/float/str): A long number such as 12345678
    Return:
        mn_name (str): A human readable number in form of a string, i.e. 12.3 mn
    """
    num = float(num)
    millidx = max(0, min(len(MILLNAMES)-1,
                         int(math.floor(
                             0 if num == 0 else math.log10(abs(num))/3))))
    mn_name = '{:.0f}{}'.format(num / 10**(3 * millidx), MILLNAMES[millidx])

    return mn_name


def get_sql_queries(sql_file_path):
    """ Return sql queries in string format from a sql file
    Args:
        sql_file_path (str): path/to/sql/file.sql
    Returns:
        sql_commands (str)
    """
    file_ = open(sql_file_path, 'r')
    sql_file = file_.read()
    sql_commands = sql_file.split(';')
    file_.close()
    return sql_commands

def execute_query(engine, query, message, retry=0):
    """
    Description:
        Execute query given the engine, allowing maximum of 3 retries if
        database connection is lost.
    Parameters:
        engine (sqlalchemy.engine.base.Engine)
        query (str): The query to be executed.
        message (str): Message to print out if the transaction is successful.
    """

    if retry < 3:
        print(retry)
        try:
            con = engine.connect()
            con.execute(query)
            print(message)
            con.close()
        except Exception as e:
            print(f'Unexpected error occurred. \n {e}')
            retry += 1
            if 'BrokenPipeError' in e.args[0]:
                print(f'BrokenPipeError: {e}. \nRetrying number {retry}.')
                execute_query(query, message, retry)
            else:
                print({e})


if __name__ == "__main__":
    pass
