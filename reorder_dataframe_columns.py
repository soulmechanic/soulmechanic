import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import json
import requests
import io
  
def reorder_dataframe_columns(snowflake_connection, table_name, schema, df):
    """ Reorder columns in a Pandas DataFrame based on the order of columns in a Snowflake table.

        :param snowflake_connection: A valid Snowflake connection object
        :param table_name: The name of the Snowflake table to use as reference
        :param schema: The name of the Snowflake schema to use as refrence
        :param df: The Pandas DataFrame to reorder columns
        :return: A new Pandas DataFrame with reordered columns
        """
    try:
        # Get the column names from the Snowflake table
        result = snowflake_connection.cursor().execute(f"DESCRIBE TABLE {schema}.{table_name}")
        columns = [row[0] for row in result]
        df.columns =  [x.upper() for x in df.columns]

        # Check if any columns are missing in the DataFrame
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"The following columns are missing in the DataFrame: {missing_columns}")

        # Reorder the columns in the DataFrame
        df = df[columns]

        return df
    except Exception as e:
        raise ValueError(f"An error occurred while reordering columns: {e}")
