# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime
import re

# these function are being imported from project libraray
from CS_FIH_DataTransformFunctions import (GetDiffBwColumns, overwrite_data, overwrite_data_empty, Add_FIH_FIP,
                                           Add_BM_Target,ChangeDateFormat, force_proj_in_out, last_next_milestone,
                                          snowflake_datetime_fix, date_variance, move_column)

# Read recipe inputs
CS_FIH_BASE_DATA = dataiku.Dataset("CS_FIH_BASE_DATA")
CS_FIH_BASE_DATA_DF = CS_FIH_BASE_DATA.get_dataframe()

BM_TARGETS = dataiku.Dataset("BM_TARGETS")
BM_TARGETS_DF = BM_TARGETS.get_dataframe()


OVERWRITE_CIS_FIH = dataiku.Dataset("OVERWRITE_CIS_FIH")
OVERWRITE_CIS_FIH_DF = OVERWRITE_CIS_FIH.get_dataframe()

FORCE_PROJ_IN_OUT = dataiku.Dataset("FORCE_PROJ_IN_OUT")
FORCE_PROJ_IN_OUT_DF = FORCE_PROJ_IN_OUT.get_dataframe()


OVERWRITE_CALCULATED_COLUMNS = dataiku.Dataset("OVERWRITE_CALCULATED_COLUMNS")
OVERWRITE_CALCULATED_COLUMNS_DF = OVERWRITE_CALCULATED_COLUMNS.get_dataframe()

CYC_CSTOFIH_MONTHLY_HIST_INPUT = dataiku.Dataset("CYC_CSTOFIH_MONTHLY_HIST_INPUT")
CYC_CSTOFIH_MONTHLY_HIST_INPUT_DF = CYC_CSTOFIH_MONTHLY_HIST_INPUT.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Data Transformations
# ### All the related Custom Python functions are placed in libraries.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To force in or force out the project based on a static table "FORCE_PROJ_IN_OUT" # made change 8/18/2023 Avinash
# this function also filter project status mentioned in the proj_status_conditions list 
proj_status_conditions = ['Ongoing', 'Awaiting Dev Decision']
CS_FIH_BASE_DATA_DF = force_proj_in_out(CS_FIH_BASE_DATA_DF, FORCE_PROJ_IN_OUT_DF,proj_status_conditions)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    # Replace with Vx in modality column if condtion met in column "Business_Category"
    CS_FIH_BASE_DATA_DF.loc[CS_FIH_BASE_DATA_DF['Business_Category'] == 'Vaccines', 'Modality'] = 'Vx'
except Exception as e:
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To Overwite certain values manually defiend in a static editable table "OVERWRITE_CIS_FIH"
CS_FIH_COLUMNS = CS_FIH_BASE_DATA_DF.columns.tolist()
CS_FIH_COLUMNS.remove('Pfizer_Code')

#
for col in CS_FIH_COLUMNS:
    CS_FIH_BASE_DATA_DF[col] = CS_FIH_BASE_DATA_DF.apply(lambda x:overwrite_data(OVERWRITE_CIS_FIH_DF, x['Pfizer_Code'],col,x[col]),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To add FIH_FIP Column to data frame based on the conditions.
CS_FIH_BASE_DATA_DF['FIP_FIH'] = CS_FIH_BASE_DATA_DF.apply(lambda x:Add_FIH_FIP(x['Research_Unit'],x['Modality'],
                                                                               "ORD",
                                                                               "Boulder","GTx"),axis=1)
# CS_FIH_BASE_DATA_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To Add BM_2_3_TARGET Column based on the lookup static table "BM_TARGETS"
CS_FIH_BASE_DATA_DF['BM_2_3_TARGET'] =  CS_FIH_BASE_DATA_DF.apply(lambda x:Add_BM_Target(BM_TARGETS_DF, x['Modality'],
                                                                                     x['FIP_FIH']),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To add "Last_Milestone" with nearest date before current date and "Next_Milestone" which has nearest date after current date
col_list = ['8900_CS', '4110_Reg_Tox_Start', '7565_NCO','9080_IND_Submitted','9061_FIH_v2']
CS_FIH_BASE_DATA_DF['Last_Milestone'] = last_next_milestone(CS_FIH_BASE_DATA_DF, col_list, 'Last_Milestone')
CS_FIH_BASE_DATA_DF['Next_Milestone'] = last_next_milestone(CS_FIH_BASE_DATA_DF, col_list, 'Next_Milestone')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# to calculate number of months differnece between specific milestones
CS_FIH_BASE_DATA_DF['RegTox_NCO'] =  CS_FIH_BASE_DATA_DF.apply(lambda x:GetDiffBwColumns(x['7565_NCO'],
                                                                                     x['4110_Reg_Tox_Start'],
                                                                                      'date_diff'),axis=1)

CS_FIH_BASE_DATA_DF['NCO_IND'] =  CS_FIH_BASE_DATA_DF.apply(lambda x:GetDiffBwColumns(x['9065_IND_Approval'],
                                                                                  x['7565_NCO'],
                                                                                   'date_diff'),axis=1)

CS_FIH_BASE_DATA_DF['CS_RegTox'] =  CS_FIH_BASE_DATA_DF.apply(lambda x:GetDiffBwColumns(x['4110_Reg_Tox_Start'],
                                                                                    x['8900_CS'],
                                                                                     'date_diff'),axis=1)

CS_FIH_BASE_DATA_DF['IND_FIH_FIP'] =  CS_FIH_BASE_DATA_DF.apply(lambda x:GetDiffBwColumns(x['9061_FIH_v2'],
                                                                                      x['9065_IND_Approval'],
                                                                                       'date_diff'),axis=1)

# '9060_Phase_I_Start' old calculation value

CS_FIH_BASE_DATA_DF['CS_FIH_FIP_Cycle_Time_months'] = CS_FIH_BASE_DATA_DF.apply(lambda x:GetDiffBwColumns(x['9061_FIH_v2'],
                                                                                      x['8900_CS'],
                                                                                       'date_diff'),axis=1)

# '9060_Phase_I_Start' old calculation value

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To Calculate Cycle Time Variance between 'CS_FIH_FIP_Cycle_Time_months' and 'BM_2_3_TARGET' columns
CS_FIH_BASE_DATA_DF['Cycle_Time_Variance'] =  CS_FIH_BASE_DATA_DF.apply(lambda x:GetDiffBwColumns(x['CS_FIH_FIP_Cycle_Time_months'],
                                                                                       x['BM_2_3_TARGET'], 'int_diff'),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To combine columns "9061_FIH_v2" and "4020_FIP" based on logic fill empty rows of 9061_FIH_v2 with values in 4020_FIP
try:
    CS_FIH_BASE_DATA_DF['FIH_FIP_Forecast'] = CS_FIH_BASE_DATA_DF['9061_FIH_v2'].fillna(CS_FIH_BASE_DATA_DF['4020_FIP'])
except Exception as e:
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# replace value with specified values
try:
    CS_FIH_BASE_DATA_DF['Modality'] = CS_FIH_BASE_DATA_DF['Modality'].replace(['Small Molecule','Biologic'],['SM','LM'])
    CS_FIH_BASE_DATA_DF['Next_Milestone'] = CS_FIH_BASE_DATA_DF['Next_Milestone'].replace([' FIH v'], ['FIH/FIP'])
    CS_FIH_BASE_DATA_DF['Last_Milestone'] = CS_FIH_BASE_DATA_DF['Last_Milestone'].replace([' FIH v'], ['FIH/FIP'])
    CS_FIH_BASE_DATA_DF['Business_Category'] = CS_FIH_BASE_DATA_DF['Business_Category'].replace(['Hospital'],['HBU'])
except Exception as e:
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# rearranging the columns according to original order in the data set produced by previous manual process.
CS_FIH_BASE_DATA_DF = CS_FIH_BASE_DATA_DF[['Pfizer_Code','Project_Name','CurrentDate','Compound_Number','Business_Category',
                                          'Research_Unit','Modality','Project_Status','Project_Phase','Plan_Owner',
                                          'Project_Manager','FIP_FIH','BM_2_3_TARGET','CS_FIH_FIP_Cycle_Time_months',
                                          'Cycle_Time_Variance','8900_CS','5025_Compound_to_Reg_Tox_Start','4110_Reg_Tox_Start',
                                          '3005_CTD_Tables','7565_NCO','9080_IND_Submitted','9065_IND_Approval',
                                          '9060_Phase_I_Start','9061_FIH_v2','4020_FIP','Issue_Description','Mitigation_Plan',
                                          'Issue_Status', 'RegTox_NCO', 'NCO_IND',
                                           'CS_RegTox', 'IND_FIH_FIP','FIH_FIP_Forecast','Last_Milestone', 'Next_Milestone','Candidate_Portfolio_Priority']]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# date_cols = ['CurrentDate', '8900_CS','5025_Compound_to_Reg_Tox_Start','4110_Reg_Tox_Start','3005_CTD_Tables','7565_NCO',
#              '9080_IND_Submitted','9065_IND_Approval','9060_Phase_I_Start','9061_FIH_v2','4020_FIP','FIH_FIP_Forecast']
# CS_FIH_BASE_DATA_DF# = ChangeDateFormat(CS_FIH_BASE_DATA_DF,date_cols,'%m/%d/%Y')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def override_all(override_df,appended_target_df, key_col):
    ## Modify in place using non-NA values from another DataFrame
    try:
        # renaming the override df columns based on values given in source and target columns from Rules Dataframe
#         Renamed_OverrideAll_df = override_df.rename(columns={source_col:target_col})

        # changing the index column to key_col which is 'CANDIDATE_CODE'
        T_DF = appended_target_df[[key_col,target_col]].set_index(key_col)

        # using update function it is going to override all the values of target dataframe in place using non-NA values from override dataframe
        T_DF.update(override_df[[key_col,target_col]].set_index(key_col))
        return T_DF.reset_index()
    except Exception as e:
        return 'error while overidding Target Dataframe' + str(e)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To Overwite certain values manually defiend in a static editable table "OVERWRITE_CIS_FIH"
CS_FIH_COLUMNS = ['FIP_FIH']


#
for col in CS_FIH_COLUMNS:
    CS_FIH_BASE_DATA_DF[col] = CS_FIH_BASE_DATA_DF.apply(lambda x:overwrite_data(OVERWRITE_CIS_FIH_DF, x['Pfizer_Code'],col,x[col]),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To Overwite certain values manually defiend in a static editable table "OVERWRITE_CALCULATED_COLUMNS_DF"
CS_FIH_COLUMNS = CS_FIH_BASE_DATA_DF.columns.tolist()
CS_FIH_COLUMNS.remove('Pfizer_Code')

#
for col in CS_FIH_COLUMNS:
    CS_FIH_BASE_DATA_DF[col] = (CS_FIH_BASE_DATA_DF.apply(lambda x:overwrite_data_empty(OVERWRITE_CALCULATED_COLUMNS_DF,
                                                                                  x['Pfizer_Code'],col,x[col]),axis=1))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### FIH_FIP_VARIANCE calculation

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# picking the max p_date from monthly hist table
max_p_date = max(CYC_CSTOFIH_MONTHLY_HIST_INPUT_DF['P_DATE'])

# converting all column names to upper as to match snowflake table names
CS_FIH_BASE_DATA_DF.columns =  [x.upper() for x in CS_FIH_BASE_DATA_DF.columns]

# filtering the partitioned monthly hist table on max partitioned date
F_CYC_CSTOFIH_MONTHLY_HIST_INPUT_DF = (CYC_CSTOFIH_MONTHLY_HIST_INPUT_DF[['PFIZER_CODE','FIH_FIP_FORECAST']]
                                       [CYC_CSTOFIH_MONTHLY_HIST_INPUT_DF['P_DATE']==max_p_date]
                                       .drop_duplicates().reset_index(drop=True))
# merging the filtered data to main CS_FIH data
CS_FIH_BASE_DATA_DF = (CS_FIH_BASE_DATA_DF.merge(F_CYC_CSTOFIH_MONTHLY_HIST_INPUT_DF,on='PFIZER_CODE',how='left',
                                                 suffixes=('','_PREVIOUS')))


# temporarly fixing the dates, will remove once finalized
CS_FIH_BASE_DATA_DF = snowflake_datetime_fix(CS_FIH_BASE_DATA_DF)

# calculating difference between current FIH_FIP_FORECAST to FIH_FIP_FORECAST_PREVIOUS
date_variance(CS_FIH_BASE_DATA_DF,'FIH_FIP_FORECAST_DIFF_DAYS','FIH_FIP_FORECAST_PREVIOUS','FIH_FIP_FORECAST')
date_variance(CS_FIH_BASE_DATA_DF,'FIH_FIP_FORECAST_DIFF_MONTHS','FIH_FIP_FORECAST_PREVIOUS','FIH_FIP_FORECAST',days=False)

# date_variance(CS_FIH_BASE_DATA_DF,'FIH_FIP_FORECAST_DIFF_DAYS','FIH_FIP_FORECAST','FIH_FIP_FORECAST_PREVIOUS') Old calculation
# date_variance(CS_FIH_BASE_DATA_DF,'FIH_FIP_FORECAST_DIFF_MONTHS','FIH_FIP_FORECAST','FIH_FIP_FORECAST_PREVIOUS',days=False) old calculation

# moving the calculated columns after FIH_FIP_FORECAST column
CS_FIH_BASE_DATA_DF = move_column(CS_FIH_BASE_DATA_DF, 'FIH_FIP_FORECAST_DIFF_MONTHS', 'FIH_FIP_FORECAST')
CS_FIH_BASE_DATA_DF = move_column(CS_FIH_BASE_DATA_DF, 'FIH_FIP_FORECAST_DIFF_DAYS', 'FIH_FIP_FORECAST')
CS_FIH_BASE_DATA_DF = move_column(CS_FIH_BASE_DATA_DF, 'FIH_FIP_FORECAST_PREVIOUS', 'FIH_FIP_FORECAST')

# CS_FIH_BASE_DATA_OVERIDDEN = dataiku.Dataset("CTM_CYC_CSTOFIH_MAIN_TEST").write_with_schema(CS_FIH_BASE_DATA_DF)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Data Output

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# # Write recipe outputs
CS_FIH_BASE_DATA_OVERIDDEN = dataiku.Dataset("CS_FIH_BASE_DATA_OVERIDDEN")
CS_FIH_BASE_DATA_OVERIDDEN.write_with_schema(CS_FIH_BASE_DATA_DF)
