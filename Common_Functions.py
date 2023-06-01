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
