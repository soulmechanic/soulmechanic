import pandas as pd
import numpy as np

def date_variance(df, date1, date2):
    try:
        df[date1] = pd.to_datetime(df[date1])
        df[date2] = pd.to_datetime(df[date2])
        df['difference'] = (df[date2] - df[date1]).dt.days
#         variance = np.var(df['difference'])
        return df['difference']
    except KeyError:
        print("One or both of the specified date columns do not exist in the DataFrame.")
    except TypeError:
        print("One or both of the specified date columns do not contain valid date values.")
        
        
def move_column(df, column_to_move, column_to_move_after):
    """
    This function takes in a DataFrame `df`, the name of the column you want to move `column_to_move`, 
    and the name of the column you want to move it after `column_to_move_after`. 
    It returns a new DataFrame with the specified column moved to the desired position. 
    If one or both of the specified columns do not exist in the DataFrame, 
    it will raise a KeyError and return the original DataFrame.
    """
    cols = df.columns.tolist()
    if column_to_move not in cols or column_to_move_after not in cols:
        raise KeyError("One or both of the specified columns do not exist in the DataFrame.")
    cols.insert(cols.index(column_to_move_after) + 1, cols.pop(cols.index(column_to_move)))
    df = df[cols]
    return df



def format_date_columns(df, date_columns=None, date_format, errors='raise', find_date_columns=False):
    """
    Formats the date columns in a DataFrame to a specific string format.

    :param df: The DataFrame containing the date columns to format.
    :type df: pandas.DataFrame
    :param date_columns: A list of column names to format. If find_date_columns is set to True, this parameter is ignored.
    :type date_columns: list, optional
    :param date_format: The desired date format string.
    :type date_format: str
    :param errors: Determines how errors should be handled. 'raise' will raise an exception, 'coerce' will set invalid values to NaT, and 'ignore' will skip invalid values.
    :type errors: str, optional
    :param find_date_columns: If set to True, the function will automatically find all columns in the DataFrame that contain dates and attempt to format them. If set to False, the function will only format the columns specified in the date_columns parameter.
    :type find_date_columns: bool, optional
    :return: The DataFrame with formatted date columns.
    :rtype: pandas.DataFrame
    """
    if find_date_columns:
        date_columns = df.select_dtypes(include=['datetime64']).columns
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col]).dt.strftime(date_format)
        except Exception as e:
            if errors == 'raise':
                raise e
            elif errors == 'coerce':
                df[col] = pd.NaT
            elif errors == 'ignore':
                continue
    return df


def override_values(df1, df2, key_column):
    """
    Overrides values on one dataframe using values from another dataframe based on the key column.

    :param df1: The first dataframe
    :type df1: pandas.DataFrame
    :param df2: The second dataframe
    :type df2: pandas.DataFrame
    :param key_column: The common column between the two dataframes
    :type key_column: str
    :return: The updated first dataframe
    :rtype: pandas.DataFrame
    """
    try:
        columns = df1.columns
        df1.set_index(key_column, inplace=True)
        df2.set_index(key_column, inplace=True)
        df1.update(df2)
        df1.reset_index(inplace=True)
        return df1[columns]
    except KeyError:
        print(f"The key column '{key_column}' was not found in one or both dataframes.")
        return None

