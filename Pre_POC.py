# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from CS_FIH_DataTransformFunctions import date_variance, snowflake_datetime_fix

# Read recipe SOCA inputs
P2P_MAIN_SOCA_df = dataiku.Dataset("P2P_MAIN_SOCA").get_dataframe()

P2P_MAIN_SOCA_HIST_INPUT_df = dataiku.Dataset("P2P_MAIN_SOCA_HIST_INPUT").get_dataframe()

P2P_MAIN_SOCA_OVERRIDE_SLIPSTREAM_df = dataiku.Dataset("P2P_MAIN_SOCA_OVERRIDE_SLIPSTREAM").get_dataframe()

P2P_MAIN_SOCA_XL_SLIPSTREAM_df = dataiku.Dataset("P2P_MAIN_SOCA_XL_SLIPSTREAM").get_dataframe()

# Read recipe POC inputs
P2P_MAIN_POC_df = dataiku.Dataset("P2P_MAIN_POC").get_dataframe()

P2P_MAIN_POC_HIST_INPUT_df = dataiku.Dataset("P2P_MAIN_POC_HIST_INPUT").get_dataframe()

P2P_MAIN_POC_OVERRIDE_SLIPSTREAM_df = dataiku.Dataset("P2P_MAIN_POC_OVERRIDE_SLIPSTREAM").get_dataframe()

P2P_MAIN_POC_XL_SLIPSTREAM_df = dataiku.Dataset("P2P_MAIN_POC_XL_SLIPSTREAM").get_dataframe()

# union data from slipstream excel file and data from sql query
UNION_P2P_MAIN_SOCA_df = P2P_MAIN_SOCA_df.append(P2P_MAIN_SOCA_XL_SLIPSTREAM_df, ignore_index=True)
UNION_P2P_MAIN_POC_df = P2P_MAIN_POC_df.append(P2P_MAIN_POC_XL_SLIPSTREAM_df, ignore_index=True)

# Fixing the timestamp
UNION_P2P_MAIN_SOCA_df = snowflake_datetime_fix(UNION_P2P_MAIN_SOCA_df)
UNION_P2P_MAIN_POC_df = snowflake_datetime_fix(UNION_P2P_MAIN_POC_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# function to override data from overide table wth the data in main table.
def override_values(df1, df2, key_column):
    """
    Overrides values on one dataframe using values from another dataframe based on the key column.

    :param df1: The first dataframe
    :param df2: The second dataframe
    :param key_column: The common column between the two dataframes
    :return: The updated first dataframe
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


P2P_MAIN_SOCA_df = override_values(UNION_P2P_MAIN_SOCA_df, P2P_MAIN_SOCA_OVERRIDE_SLIPSTREAM_df,[ 'PFIZER_CODE','P2P_BASELINE'])
P2P_MAIN_POC_df = override_values(UNION_P2P_MAIN_POC_df, P2P_MAIN_POC_OVERRIDE_SLIPSTREAM_df, ['PFIZER_CODE','P2P_BASELINE'])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### SOCA Data Processing

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# picking the max p_date from monthly hist table
max_p_date = max(P2P_MAIN_SOCA_HIST_INPUT_df['P_DATE'])

F_P2P_MAIN_SOCA_HIST_INPUT_df = P2P_MAIN_SOCA_HIST_INPUT_df[P2P_MAIN_SOCA_HIST_INPUT_df['P_DATE']==max_p_date].drop_duplicates(subset=['PFIZER_CODE'],keep='first')

P2P_MAIN_SOCA_df['PREVIOUS_LE'] = P2P_MAIN_SOCA_df['PFIZER_CODE'].map(F_P2P_MAIN_SOCA_HIST_INPUT_df.set_index('PFIZER_CODE')['SOCA_CURRENT_LE'])

# calculating difference between current LE to PREVIOUS LE
P2P_MAIN_SOCA_df = date_variance(P2P_MAIN_SOCA_df,'SOCA_VARIANCE_DAYS','SOCA_INITIAL_TARGET_AT_THE_TIME_OF_FIH','SOCA_CURRENT_LE',days=True)

P2P_MAIN_SOCA_df = date_variance(P2P_MAIN_SOCA_df,'SOCA_VARIANCE_MONTHS','SOCA_INITIAL_TARGET_AT_THE_TIME_OF_FIH','SOCA_CURRENT_LE',days=False)

P2P_MAIN_SOCA_df = date_variance(P2P_MAIN_SOCA_df,'VARIANCE_IN_CURRENT_SOCA_TO_PREVIOUS_SOCA','SOCA_CURRENT_LE','PREVIOUS_LE',days=False)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### POC Data Processing

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# picking the max p_date from monthly hist table
max_p_date = max(P2P_MAIN_POC_HIST_INPUT_df['P_DATE'])

F_P2P_MAIN_POC_HIST_INPUT_df = P2P_MAIN_POC_HIST_INPUT_df[P2P_MAIN_POC_HIST_INPUT_df['P_DATE']==max_p_date].drop_duplicates(subset=['PFIZER_CODE'],keep='first')

P2P_MAIN_POC_df['PREVIOUS_LE'] = P2P_MAIN_POC_df['PFIZER_CODE'].map(F_P2P_MAIN_POC_HIST_INPUT_df.set_index('PFIZER_CODE')['POC_CURRENT_LE'])

# calculating difference between current LE to PREVIOUS LE
P2P_MAIN_POC_df = date_variance(P2P_MAIN_POC_df,'POC_VARIANCE_DAYS','POC_INITIAL_TARGET_AT_THE_TIME_OF_PSS','POC_CURRENT_LE',days=True)

P2P_MAIN_POC_df = date_variance(P2P_MAIN_POC_df,'POC_VARIANCE_MONTHS','POC_INITIAL_TARGET_AT_THE_TIME_OF_PSS','POC_CURRENT_LE',days=False)

P2P_MAIN_POC_df = date_variance(P2P_MAIN_POC_df,'VARIANCE_IN_CURRENT_POC_TO_PREVIOUS_POC','POC_CURRENT_LE','PREVIOUS_LE', days=False)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Combining POC and SOCA Data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# combining POC and SOCA data
common_cols = ['PFIZER_CODE', 'PROJECT_NAME', 'CANDIDATE_BUSINESS_CATEGORY', 'CANDIDATE_PORTFOLIO_PRIORITY',
              'COHORT', 'COHORT_CHANGE', 'P2P_BASELINE','BASELINE_DATE','VARIANCE_MONTHS',
              'VARIANCE_IN_CURRENT_TO_PREVIOUS']

P2P_MAIN_POC_df = P2P_MAIN_POC_df.rename(columns={'POC_VARIANCE_MONTHS':'VARIANCE_MONTHS',
                                                  'VARIANCE_IN_CURRENT_POC_TO_PREVIOUS_POC':
                                                  'VARIANCE_IN_CURRENT_TO_PREVIOUS'})

P2P_MAIN_SOCA_df = P2P_MAIN_SOCA_df.rename(columns={'SOCA_VARIANCE_MONTHS':'VARIANCE_MONTHS',
                                                   'VARIANCE_IN_CURRENT_SOCA_TO_PREVIOUS_SOCA':
                                                   'VARIANCE_IN_CURRENT_TO_PREVIOUS'})

P2P_MAIN_FINAL_DF = pd.concat([P2P_MAIN_POC_df,P2P_MAIN_SOCA_df],join='inner',ignore_index=True)
P2P_MAIN_FINAL_DF = P2P_MAIN_FINAL_DF[common_cols]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
P2P_MAIN_SOCA_FINAL = dataiku.Dataset("P2P_MAIN_SOCA_FINAL").write_with_schema(P2P_MAIN_SOCA_df)

P2P_MAIN_POC_FINAL = dataiku.Dataset("P2P_MAIN_POC_FINAL").write_with_schema(P2P_MAIN_POC_df)

P2P_MAIN_FINAL = dataiku.Dataset("P2P_MAIN_FINAL").write_with_schema(P2P_MAIN_FINAL_DF)
