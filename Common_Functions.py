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

