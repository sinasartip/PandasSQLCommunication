
import sqlalchemy
from sqlalchemy.engine.base import Engine, Connection
import pandas as pd
from IPython.display import display
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def get_secret_from_keyvault(keyvault_name:str, secret_name:str)->str:
    """get secret from keyvaults in logged in subscription

    Args:
        keyvault_name (str): name of resourece keyvault to access
        secret_name (str): secret name to retrieve

    Returns:
        str: secret value as a string
    """
    KVUri = f"https://{keyvault_name}.vault.azure.net"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)

    retrieved_secret = client.get_secret(secret_name)

    return retrieved_secret.value


def create_connection(user:str, password:str, server:str, database:str)->tuple[Engine, Connection]:
        """create and test connection to database
        sample connection example
        ```
        user = 'sqladm'
        password = get_secret_from_keyvault('moneyhubkeyvault', 'sqladm')
        server = 'moneyhubsqlserver.database.windows.net'
        database = 'moneyhubsqldev'
        engine, connection = create_connection(user,password,server,database)
        ```

        Args:
                user (str): username for server
                password (str): password for server
                server (str): server address
                database (str): database to connect to

        Returns:
                tuple[Engine, Connection]: SQL Alchemy engine, SQL Alchemy connection
        """
        connection_string = f'mssql+pyodbc://{user}:{password}@{server}/{database}?driver=SQL Server'
        engine = sqlalchemy.create_engine(connection_string)
        connection = engine.connect()

        sql = """
                SELECT *
                FROM
                INFORMATION_SCHEMA.TABLES;
                """
        display(pd.read_sql(sql,connection))

        return engine, connection


