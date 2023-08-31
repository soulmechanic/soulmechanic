import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
#from dateutil.parser import parse
#from flatsplode import flatsplode

#general dataframe prep for
#place any transforms to be applied to every cell in dataframe here
#use type checking to apply transforms to cells of particular data type
#TODO - can we make this recursive? to format within nested objects

def prep_for_JSON(value):
    try:
        if isinstance(value, date):
            #print(type(value))
            if not pd.isnull(value):
                return value.strftime('%Y/%m/%d')
            else: #empty dates are still date instances, will be set to none here
                pass 
        else:
            return value

    except:
        return value
    
    
def format_date_columns(df, date_format, date_columns=None, errors='raise', find_date_columns=False):
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
        datetime_types = ["datetime", "datetime64", "datetime64[ns]", "datetimetz","datetime64[ns, UTC]"]
        date_columns = df.select_dtypes(include=datetime_types).columns
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



    
# function to clean speacial char from json
def Replace_Special_Char_for_JSON(input_df):
    input_df = input_df.replace(to_replace=chr(92)+chr(91), value=chr(40),regex=True) #[ -> (
    input_df = input_df.replace(to_replace=chr(92)+chr(93), value=chr(41),regex=True) #] -> )
    input_df = input_df.replace(to_replace=chr(92)+chr(92), value=chr(92)+chr(92),regex=True) #\ -> \\
    input_df = input_df.replace(to_replace=chr(92)+chr(9), value=' ',regex=True) # \t -> ' ' 
    return input_df



# function to Explode json to dataframe

def xplode_json(value):
    data = json.loads(value)
    df = pd.DataFrame(list(flatsplode(data)))#.fillna("NA")
    return df
    
    

