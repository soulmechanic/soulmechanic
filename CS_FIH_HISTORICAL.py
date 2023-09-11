# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from CS_FIH_DataTransformFunctions import (GetDiffBwColumns, overwrite_data, Add_FIH_FIP, Add_BM_Target, ChangeDateFormat,
                                           force_proj_in_out, overwrite_data_empty)


# Read recipe inputs
bm_TARGETS = dataiku.Dataset("BM_TARGETS")
BM_TARGETS_DF = bm_TARGETS.get_dataframe()

overwrite_CIS_FIH = dataiku.Dataset("OVERWRITE_CIS_FIH")
overwrite_CIS_FIH_DF = overwrite_CIS_FIH.get_dataframe()

OVERWRITE_CALCULATED_COLUMNS = dataiku.Dataset("OVERWRITE_CALCULATED_COLUMNS")
OVERWRITE_CALCULATED_COLUMNS_DF = OVERWRITE_CALCULATED_COLUMNS.get_dataframe()

fih_HISTORICAL_SNAPSHOTS_FINAL = dataiku.Dataset("FIH_HISTORICAL_SNAPSHOTS_FINAL_DATA")
FIH_HIST_SNAP_DF = fih_HISTORICAL_SNAPSHOTS_FINAL.get_dataframe()

FORCE_PROJ_IN_OUT = dataiku.Dataset("FORCE_PROJ_IN_OUT")
FORCE_PROJ_IN_OUT_DF = FORCE_PROJ_IN_OUT.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Data Transformations

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# made change 8/18/2023 Avinash
# this function also filter project status mentioned in the proj_status_conditions list 
proj_status_conditions = ['Ongoing', 'Awaiting Dev Decision']
FIH_HIST_SNAP_DF = force_proj_in_out(FIH_HIST_SNAP_DF, FORCE_PROJ_IN_OUT_DF,proj_status_conditions)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ##### Adding manual overwrites to Dataset

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
fih_HISTORICAL_SNAPSHOTS_COLUMNS = FIH_HIST_SNAP_DF.columns.tolist()
fih_HISTORICAL_SNAPSHOTS_COLUMNS.remove('Pfizer_Code')

#FIXME: inefficient code pattern - try replacing whole row at once
for col in fih_HISTORICAL_SNAPSHOTS_COLUMNS:
    FIH_HIST_SNAP_DF[col] = FIH_HIST_SNAP_DF.apply(lambda x:overwrite_data(overwrite_CIS_FIH_DF,
                                                                           x['Pfizer_Code'],col,x[col]),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    # Replace with Vx in modality column if condtion met in column "Business_Category"
    FIH_HIST_SNAP_DF.loc[FIH_HIST_SNAP_DF['Business_Category'] == 'Vaccines', 'Modality'] = 'Vx'
except Exception as e:
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ##### Adding New Columns based on Logic

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Adding FIP_FIH Column based on the logic if in column 'Research_Unit' has 'ORD' then add 'FIP' or if 'Boulder' 'FIH'
FIH_HIST_SNAP_DF['FIP_FIH'] = FIH_HIST_SNAP_DF.apply(lambda x:Add_FIH_FIP(x['Research_Unit'],x['Modality'],
                                                                          "ORD","Boulder","GTx"),axis=1)


# Adding BM_2_3_TARGET column based on the lookup values in the BM_TARGET_DF table
FIH_HIST_SNAP_DF['BM_2_3_TARGET'] =  FIH_HIST_SNAP_DF.apply(lambda x:Add_BM_Target(BM_TARGETS_DF, x['Modality'],
                                                                                   x['Business_Category']),axis=1)

# Adding CS_FIH_FIP_Cycle_Time_months column based on differance in number of months between 9061_FIH col and 8900_CS
FIH_HIST_SNAP_DF['CS_FIH_FIP_Cycle_Time_months'] = FIH_HIST_SNAP_DF.apply(lambda x:GetDiffBwColumns(x['9061_FIH'],
                                                                                                      x['8900_CS'],
                                                                                                      'date_diff'),axis=1)
# '9060_Phase_I_Start' old calculation value


# Adding Cycle_Time_Variance column based on differance in number of months between CS_FIH_FIP_Cycle_Time_months col
# and BM_2_3_TARGET
FIH_HIST_SNAP_DF['Cycle_Time_Variance'] = FIH_HIST_SNAP_DF.apply(lambda x:GetDiffBwColumns(x['CS_FIH_FIP_Cycle_Time_months'],
                                                                                        x['BM_2_3_TARGET'], 'int_diff'),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# FIH_HIST_SNAP_DF[FIH_HIST_SNAP_DF['Pfizer_Code']=='C488a']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ##### Replacing Values inside columns and Renaming Column

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    # Replacing Values inside columns Modality col and Business_category col
    FIH_HIST_SNAP_DF['Modality'] = FIH_HIST_SNAP_DF['Modality'].replace(['Small Molecule','Biologic'],['SM','LM'])
    FIH_HIST_SNAP_DF['Business_Category'] = FIH_HIST_SNAP_DF['Business_Category'].replace(['Hospital'],['HBU'])

    # Renaming column Names
    FIH_HIST_SNAP_DF = FIH_HIST_SNAP_DF.rename({"Ref_SnapDate": "Project_SnapDate", "8900_CS" : "8900_CS_Snapshot",
                                               "9061_FIH": "9061_FIH_Snapshot"}, axis=1)
except Exception as e:
    print('Failed at this point:','-->', e)
    pass

# '9060_Phase_I_Start' old value

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ##### Rearranging columns for FIH_HIST_SNAP_DF dataset

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
FIH_HIST_SNAP_DF = FIH_HIST_SNAP_DF[["Pfizer_Code","Project_Name","Project_SnapDate","CurrentDate","Compound_Number",
                                     "Business_Category","Research_Unit","Modality","Project_Status","Project_Phase",
                                     "Plan_Owner","Project_Manager","FIP_FIH","BM_2_3_TARGET","CS_FIH_FIP_Cycle_Time_months",
                                     "Cycle_Time_Variance","8900_CS_Snapshot","9061_FIH_Snapshot","Candidate_Portfolio_Priority"]]

# '9060_Phase_I_Start' old value

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ##### Changing the Date format to Custom Format '%m/%d/%Y'

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# date_cols = ['Project_SnapDate', 'Current_Date', '8900_CS_Snapshot','9060_Phase_I_Start_Snapshot']
# FIH_HIST_SNAP_DF = ChangeDateFormat(FIH_HIST_SNAP_DF,date_cols,'%m/%d/%Y')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# from datetime import date
# try:
#     # Replacing Values inside columns Project_SnapDate
#     replace_date = date(2020, 8, 31)
#     print(replace_date)
#     FIH_HIST_SNAP_DF['Project_SnapDate'] = FIH_HIST_SNAP_DF['Project_SnapDate'] == replace_date #date(2020, 8, 31)
# #     FIH_HIST_SNAP_DF['Project_SnapDate'] = FIH_HIST_SNAP_DF['Project_SnapDate'].replace(date(2020, 8, 31),date(2020, 9, 1))
# #     FIH_HIST_SNAP_DF['Project_SnapDate'] = replace_date.replace(day=1, month=9)
# except Exception as e:
#     print('Failed at this point:','-->', e)
#     pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    from datetime import datetime, timezone
    old_date = datetime(2020, 8, 31, tzinfo=timezone.utc).isoformat(sep=' ')
    new_date = datetime(2020, 9, 1, tzinfo=timezone.utc).isoformat(sep=' ')
    old_dt = pd.to_datetime(old_date, utc=True)
    new_dt = pd.to_datetime(new_date, utc=True)

    FIH_HIST_SNAP_DF['Project_SnapDate'] = FIH_HIST_SNAP_DF['Project_SnapDate'].apply(lambda x: new_dt
                                                                                           if x==old_dt else x)
except Exception as e:
    print('Failed at this point:','-->', e)
    pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# FIH_HIST_SNAP_DF['Cycle_Time_Variance'] = FIH_HIST_SNAP_DF['Cycle_Time_Variance'].apply(lambda x: ''
#                                                                                        if x =='GTx' or x == 'Vx' else x)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
fih_HISTORICAL_SNAPSHOTS_COLUMNS = ['FIP_FIH']

#FIXME: inefficient code pattern - try replacing whole row at once
for col in fih_HISTORICAL_SNAPSHOTS_COLUMNS:
    FIH_HIST_SNAP_DF[col] = FIH_HIST_SNAP_DF.apply(lambda x:overwrite_data(overwrite_CIS_FIH_DF,
                                                                           x['Pfizer_Code'],col,x[col]),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
FIH_HIST_SNAP_DF.head(100)

fih_HISTORICAL_SNAPSHOTS_COLUMNS = FIH_HIST_SNAP_DF.columns.tolist()
fih_HISTORICAL_SNAPSHOTS_COLUMNS.remove('Pfizer_Code')

#FIXME: inefficient code pattern - try replacing whole row at once
for col in fih_HISTORICAL_SNAPSHOTS_COLUMNS:
    FIH_HIST_SNAP_DF[col] = FIH_HIST_SNAP_DF.apply(lambda x:overwrite_data_empty(OVERWRITE_CALCULATED_COLUMNS_DF,
                                                                           x['Pfizer_Code'],col,x[col]),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Output the data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

# Compute a Pandas dataframe to write into FIH_HISTORICAL_SNAPSHOTS_FINAL_OVERRIDDEN
fih_HISTORICAL_SNAPSHOTS_FINAL_OVERRIDDEN_df = FIH_HIST_SNAP_DF


# Write recipe outputs
fih_HISTORICAL_SNAPSHOTS_FINAL_OVERRIDDEN = dataiku.Dataset("FIH_HISTORICAL_SNAPSHOTS_FINAL_OVERRIDDEN")
fih_HISTORICAL_SNAPSHOTS_FINAL_OVERRIDDEN.write_with_schema(fih_HISTORICAL_SNAPSHOTS_FINAL_OVERRIDDEN_df)
