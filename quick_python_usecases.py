#### Groupby with condition

ms_df = ms_df.groupby(['Next_MS','Code'])['MS_Date'].min()

## fileter Dataframe with condtion
ms_df = ms_df[ms_df['MS_Date'] > curr_dt]