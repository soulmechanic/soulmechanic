# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Reading the Data and Importing all the required libraries

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import functools
from datetime import datetime, timezone, date
from dateutil import parser
from P2P_DataTransformFunctions import (GetDiffBwColumns, ChangeDateFormat, force_proj_in_out, var_cal, overwrite_data_p2p)
pd.set_option('mode.chained_assignment', None)

# Read recipe inputs
p2P_Historical = dataiku.Dataset("P2P_Historical_15212020")
p2P_Historical_df = p2P_Historical.get_dataframe()


P2P_MAIN_PROD = dataiku.Dataset("P2P_MAIN_PROD_QUERY")
P2P_MAIN_PROD_df = P2P_MAIN_PROD.get_dataframe()

PROJ_FORCE_IN_OUT = dataiku.Dataset("PROJ_FORCE_IN_OUT")
PROJ_FORCE_IN_OUT_df = PROJ_FORCE_IN_OUT.get_dataframe()

OVERWRITE_P2P = dataiku.Dataset("P2P_OVERWRITE_TABLE")
OVERWRITE_P2P_DF = OVERWRITE_P2P.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# function to convert date columns to required format
def StrDateToISOFormat(DF, list_date_cols):
    try:
        for col in list_date_cols:
            DF[col] = pd.to_datetime(DF[col], utc=True)#.dt.strftime('%m-%d-%Y')
        return DF
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# function to drop duplicate columns created while changing the string date format to iso date format in p2p historical excel file.
def remove_dup_cols(df):
    # remove suffix
    df.columns = df.columns.str.rstrip("_iso")
    df.columns = df.columns.str.replace(' ', '')
    dup_cols =df.columns[df.columns.duplicated()]
    cols=pd.Series(df.columns)
    for dup in df.columns[df.columns.duplicated(keep=False)]:
        cols[df.columns.get_loc(dup)] = ([dup + '_dup'
                                         if d_idx != 0
                                         else dup
                                         for d_idx in range(df.columns.get_loc(dup).sum())]
                                        )
    df.columns=cols
    df = df.drop(columns=dup_cols)
    df.columns = df.columns.str.rstrip("_dup")
    df = StrDateToISOFormat(df, dup_cols)
    return df
p2P_Historical_df = remove_dup_cols(p2P_Historical_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Generating P2P Combined dataset [ Historical & Main ]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Adding SOurce columns to both P2P Historical and Main Datasets

p2P_Historical_df['Source'] = 'Historical'
P2P_MAIN_PROD_df['Source'] = 'Prod'

#force project in or out based condition

P2P_MAIN_PROD_df = force_proj_in_out(P2P_MAIN_PROD_df, PROJ_FORCE_IN_OUT_df)
# P2P_MAIN_PROD_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Concatinating both P2P Historical and Main Datasets with selected columns
try:
    columns_list=['PFIZER_CODE', 'PROJECT_NAME', 'BUSINESS_CATEGORY', 'PROJECT_MANAGER', 'PROJECT_PLANNER',
                  'COMPOUND_NUMBER','CANDIDATE_PORTFOLIO_PRIORITY','CANDIDATE_TYPE', 'MODALITY', 'GOAL', 'GOAL_STATUS', 'PROJECT_STATUS', 'PROJECT_PHASE',
                  'BASELINE_EVENT',
                  'BASELINE_DATE', 'NDA_PPS_TO_SUB_START', 'NDA_PPS_TO_SUB_FINISH', 'NDA_PPS_TO_SUB_DURATION_PLW',
                  'NDA_PPS_TO_SUB_VARIANCE_RC1', 'NDA_PPS_TO_SUB_VARIANCE_RC2', 'MAA_PPS_TO_SUB_START',
                  'MAA_PPS_TO_SUB_FINISH', 'MAA_PPS_TO_SUB_DURATION_PLW', 'MAA_PPS_TO_SUB_VARIANCE_RC1',
                  'MAA_PPS_TO_SUB_VARIANCE_RC2', 'EP2', 'EP2_PC', 'DP3', 'DP3_PC', 'PPS', 'PPS_PC', 'PPS_VAR_RC1',
                  'PPS_VAR_RC2', 'PHASE_3_START', 'PHASE_3_START_PC', 'LSLV_NDA_SUBMISSION', 'LSLV_NDA_SUBMISSION_PC',
                  'LSLV_NDA_SUB_VARIANCE_RC1', 'LSLV_NDA_SUB_VARIANCE_RC2', 'NDA_SUBMISSION', 'NDA_SUBMISSION_PC',
                  'NDA_STUDY_ALIGN', 'NDA_SUB_VARIANCE_RC1', 'NDA_SUB_VARIANCE_RC2', 'LSLV_MAA_SUBMISSION',
                  'LSLV_MAA_SUBMISSION_PC', 'LSLV_MAA_SUB_VARIANCE_RC1', 'LSLV_MAA_SUB_VARIANCE_RC2', 'MAA_SUBMISSION',
                  'MAA_SUBMISSION_PC', 'MAA_STUDY_ALIGN', 'MAA_SUB_VARIANCE_RC1', 'MAA_SUB_VARIANCE_RC2','NDA_APPROVAL',
                  'NDA_APPROVAL_PC','MAA_APPROVAL','MAA_APPROVAL_PC','Source']

    p2P_Historical_df = p2P_Historical_df[columns_list]

    P2P_MAIN_PROD_df = P2P_MAIN_PROD_df[columns_list]

    P2P_H_P_UNION_DF = (pd.concat([p2P_Historical_df,P2P_MAIN_PROD_df])
                          .reset_index(drop=True))
    P2P_DF = (P2P_H_P_UNION_DF.sort_values(by=['PFIZER_CODE','BASELINE_DATE'])
                              .drop_duplicates(subset=['PFIZER_CODE','BASELINE_EVENT'], keep='first')
                              .reset_index(drop=True))
except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### P2P Data Transformations

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To Overwite certain values manually defiend in a static editable table "OVERWRITE_CIS_FIH"
P2P_COLUMNS = P2P_DF.columns.tolist()
remove_key_cols = {'PFIZER_CODE','BASELINE_EVENT'}
P2P_COLUMNS = [ele for ele in P2P_COLUMNS if ele not in remove_key_cols]

for col in P2P_COLUMNS:
    P2P_DF[col] = P2P_DF.apply(lambda x:overwrite_data_p2p(OVERWRITE_P2P_DF, x['PFIZER_CODE'],
                                                           x['BASELINE_EVENT'],col,x[col]),axis=1)

# P2P_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# logic implemented 04/05/2022
# fill the blank DP3 column with the DP3 date from the DP3 Baseline Event for other baseline events
# blank DP3 fields should only be filled in if they are from Baseline Events taken after the DP3 Baseline Event

def fill_dp3(df):

    codes = list(set(df['PFIZER_CODE'].sort_values()))
    f_dfs = []

    for code in codes:
        f_df = df[df['PFIZER_CODE']==code]

        #allow only if baseline even has dp3 event
        # if f_df['BASELINE_EVENT'].str.contains('DP3', case=True, regex=True,na=False).any():
        # added this line of code as there were more then one DP3 event per code
        if f_df['BASELINE_EVENT'].isin(['DP3']).any():
            dp3_date = f_df.set_index('BASELINE_EVENT').at['DP3','DP3']
            f_df['DP3'] = f_df['DP3'].fillna(dp3_date)
            f_dfs.append(f_df)

    DF = pd.concat(f_dfs, ignore_index=True)

    # concat missing pfizer codes, which did not have dp3 in baseline event
    missing_codes = list(sorted(set(df['PFIZER_CODE']) - set(DF['PFIZER_CODE'])))
    DF = pd.concat([DF,df[df['PFIZER_CODE'].isin(missing_codes)]], ignore_index=True)
    DF = StrDateToISOFormat(DF, ['DP3'])

    return DF

P2P_DF = fill_dp3(P2P_DF)
# P2P_DF#.dtypes

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# replace empty values for NDA_SUBMISSION and LSLV_NDA_SUBMISSION with MAA_SUBMISSION and LSLV_MAA_SUBMISSION respectively
P2P_DF['NDA_SUBMISSION'] = P2P_DF['NDA_SUBMISSION'].fillna(P2P_DF['MAA_SUBMISSION'])
P2P_DF['LSLV_NDA_SUBMISSION'] = P2P_DF['LSLV_NDA_SUBMISSION'].fillna(P2P_DF['LSLV_MAA_SUBMISSION'])
P2P_DF['PHASE_3_START'] = P2P_DF['PHASE_3_START'].fillna(P2P_DF['PPS'])
P2P_DF['NDA_APPROVAL'] = P2P_DF['NDA_APPROVAL'].fillna(P2P_DF['MAA_APPROVAL'])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Calculate Difference between two date columns using the function GetDiffBwColumns, which is located in libraries
P2P_DF['FSD_TO_LSLV_CT_MNTS'] = P2P_DF.apply(lambda x:GetDiffBwColumns(x['LSLV_NDA_SUBMISSION'],
                                                                                      x['PHASE_3_START'],
                                                                                       'date_diff'),axis=1)

P2P_DF['LSLV_TO_SUB_CT_MNTS'] = P2P_DF.apply(lambda x:GetDiffBwColumns(x['NDA_SUBMISSION'],
                                                                                      x['LSLV_NDA_SUBMISSION'],
                                                                                       'date_diff'),axis=1)

P2P_DF['PSTART_TO_SUB_CT_MNTS'] = P2P_DF.apply(lambda x:GetDiffBwColumns(x['NDA_SUBMISSION'],
                                                                                      x['PHASE_3_START'],
                                                                                       'date_diff'),axis=1)
P2P_DF['PSTART_TO_APP_CT_MNTS'] = P2P_DF.apply(lambda x:GetDiffBwColumns(x['NDA_APPROVAL'],
                                                                                      x['PHASE_3_START'],
                                                                                       'date_diff'),axis=1)

P2P_DF['SUB_TO_APP_CT_MNTS'] = P2P_DF.apply(lambda x:GetDiffBwColumns(x['NDA_APPROVAL'],
                                                                                      x['NDA_SUBMISSION'],
                                                                                       'date_diff'),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to calculate the variance by DP3  with condition
# if baseline event is DP3 or baseline event happens before DP3 event the calculation is empty

def var_cal(main_df,var_column,column_name,b_event):
    try:
        codes = list(main_df['PFIZER_CODE'].unique())
        DF_list=[]
        for code in codes:
            df = main_df[['BASELINE_EVENT','BASELINE_DATE','PFIZER_CODE',var_column]][main_df['PFIZER_CODE'] == code]
            if df['BASELINE_EVENT'].isin(['DP3']).any():
                df = df.set_index('BASELINE_EVENT')
                dp3_date = df.loc['DP3','BASELINE_DATE']
                df[var_column] = df[var_column].where(df['BASELINE_DATE']>=dp3_date, np.nan)
                df[column_name] = pd.to_numeric(df[var_column] - df.loc[b_event,var_column])
                df=df.reset_index()
                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)
        final_df=pd.concat(DF_list, ignore_index=True)
        final_df=final_df
        final_df[column_name] = final_df[column_name].where(final_df['BASELINE_EVENT']!='DP3', np.nan)
        main_df = pd.merge(main_df,final_df, on=['PFIZER_CODE','BASELINE_EVENT'], how='left')
        return main_df[column_name]
    except Exception as e:
        print('Unable to find', '->', e)


P2P_DF['VAR_PSTART_TO_SUB_BTW_BL'] = var_cal(P2P_DF, 'PSTART_TO_SUB_CT_MNTS', 'VAR_PSTART_TO_SUB_BTW_BL','DP3')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
P2P_DF['LSLV_TO_MAA_SUB_CT_MNTS'] = P2P_DF.apply(lambda x:GetDiffBwColumns(x['MAA_SUBMISSION'],
                                                                                      x['LSLV_MAA_SUBMISSION'],
                                                                                       'date_diff'),axis=1)


P2P_DF['PSTART_TO_MAA_SUB_CT_MNTS'] = P2P_DF.apply(lambda x:GetDiffBwColumns(x['MAA_SUBMISSION'],
                                                                                      x['PHASE_3_START'],
                                                                                       'date_diff'),axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    P2P_DF['PPStart_Date_MS_Var_MNTS'] = (P2P_DF.groupby(['PFIZER_CODE'])['PHASE_3_START']
                                          .diff().div(30.42).div(np.timedelta64(1, 'D'))
                                          .fillna('')
                                          )


    P2P_DF['LSLV_NDA_Sub_Date_MS_Var_MNTS'] = (P2P_DF.groupby(['PFIZER_CODE'])['LSLV_NDA_SUBMISSION']
                                          .diff().div(30.42).div(np.timedelta64(1, 'D'))
                                          .fillna('')
                                          )


    P2P_DF['NDA_Sub_Date_MS_Var_MNTS'] = (P2P_DF.groupby(['PFIZER_CODE'])['NDA_SUBMISSION']
                                          .diff().div(30.42).div(np.timedelta64(1, 'D'))
                                          .fillna('')
                                          )


    P2P_DF['LSLV_MAA_Sub_Date_MS_Var_MNTS'] = (P2P_DF.groupby(['PFIZER_CODE'])['LSLV_MAA_SUBMISSION']
                                          .diff().div(30.42).div(np.timedelta64(1, 'D'))
                                          .fillna('')
                                          )

    P2P_DF['MAA_Sub_Date_MS_Var_MNTS'] = (P2P_DF.groupby(['PFIZER_CODE'])['MAA_SUBMISSION']
                                          .diff().div(30.42).div(np.timedelta64(1, 'D'))
                                          .fillna('')
                                          )
except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to calculate the variance between dates by DP3 date with condition
# if baseline event is DP3 or baseline event happens before DP3 event the calculation is empty

def var_date(DF, var_column, column_name):
    try:
        codes = list(DF['PFIZER_CODE'].unique())
        DF_list=[]
        for code in codes:
            df = DF[['BASELINE_EVENT','BASELINE_DATE','PFIZER_CODE',var_column]][DF['PFIZER_CODE'] == code]
            if df['BASELINE_EVENT'].isin(['DP3']).any():
                df = df.set_index('BASELINE_EVENT')
                dp3_date = df.loc['DP3','BASELINE_DATE']
                df[var_column] = df[var_column].where(df['BASELINE_DATE']>=dp3_date, np.nan)
                df[column_name] = pd.to_numeric(((df[var_column] -
                                                  df.loc['DP3',var_column])/30.42) / (np.timedelta64(1, 'D')),errors='coerce')
                df=df.reset_index()
                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)
            else:
                df[column_name] = np.nan
                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)
        final_df=pd.concat(DF_list, ignore_index=True)
        final_df=final_df
        final_df[column_name] = final_df[column_name].where(final_df['BASELINE_EVENT']!='DP3', np.nan)
        DF = pd.merge(DF,final_df, on=['PFIZER_CODE','BASELINE_EVENT'], how='left')
        return DF[column_name]
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
P2P_DF['PPStart_Date_MS_Var_DP3'] = var_date(P2P_DF, 'PHASE_3_START', 'PPStart_Date_MS_Var_DP3')

P2P_DF['LSLV_NDA_Sub_Date_MS_Var_DP3'] = var_date(P2P_DF, 'LSLV_NDA_SUBMISSION', 'LSLV_NDA_Sub_Date_MS_Var_DP3')

P2P_DF['NDA_Sub_Date_MS_Var_DP3'] = var_date(P2P_DF, 'NDA_SUBMISSION', 'NDA_Sub_Date_MS_Var_DP3')

P2P_DF['LSLV_MAA_Sub_Date_MS_Var_DP3'] = var_date(P2P_DF, 'LSLV_MAA_SUBMISSION', 'LSLV_MAA_Sub_Date_MS_Var_DP3')

P2P_DF['MAA_Sub_Date_MS_Var_DP3'] = var_date(P2P_DF, 'MAA_SUBMISSION', 'MAA_Sub_Date_MS_Var_DP3')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to calculate the variance between dates by baseline event date with condition
# if baseline event is specified baseline evenet or baseline event happens before specified baseline event the calculation is empty

def Time_Elapsed_Since_BLEvent(DF, BL_Event, var_column, column_name):
    try:
        codes = list(DF['PFIZER_CODE'].unique())
        DF_list=[]
        for code in codes:
            df = DF[['BASELINE_EVENT','PFIZER_CODE',var_column]][DF['PFIZER_CODE'] == code]
            if df['BASELINE_EVENT'].isin([BL_Event]).any():
                df = df.set_index('BASELINE_EVENT')
                BL_date = df.loc[BL_Event,var_column]
                df[var_column] = df[var_column].where(df[var_column]>=BL_date, np.nan)
                df[column_name] = pd.to_numeric(((df[var_column] -
                                                  df.loc[BL_Event,var_column])/30.42) / (np.timedelta64(1, 'D')),errors='coerce')
                df=df.reset_index()
                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)
            else:
                df[column_name] = np.nan
                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)
        final_df=pd.concat(DF_list, ignore_index=True)
        final_df[column_name] = final_df[column_name].where(final_df['BASELINE_EVENT']!=BL_Event, np.nan)
        DF = pd.merge(DF,final_df, on=['PFIZER_CODE','BASELINE_EVENT'], how='left')
        return DF[column_name]
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
P2P_DF['Time_Elapsed_Since_DP3'] = Time_Elapsed_Since_BLEvent(P2P_DF, 'DP3', 'BASELINE_DATE', 'Time_Elapsed_Since_DP3')
P2P_DF['Time_Elapsed_Since_Actual_PPS'] = Time_Elapsed_Since_BLEvent(P2P_DF, 'Actual PPS', 'BASELINE_DATE', 'Time_Elapsed_Since_Actual_PPS')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to calculate the variance between dates by baseline event date with condition
# if baseline event is specified baseline evenet or baseline event happens before specified baseline event the calculation is empty

def Time_Elapsed_Since_BLEvent_Date(DF, BL_Event, var_column, column_name):
    try:
        codes = list(DF['PFIZER_CODE'].unique())
        DF_list=[]
        for code in codes:
            df = DF[['BASELINE_EVENT','BASELINE_DATE', 'PFIZER_CODE',var_column]][DF['PFIZER_CODE'] == code]
            if df['BASELINE_EVENT'].isin([BL_Event]).any():
                df = df.set_index('BASELINE_EVENT')
                BL_date = df.loc[BL_Event,'BASELINE_DATE']
                df['BASELINE_DATE'] = df['BASELINE_DATE'].where(df['BASELINE_DATE']>=BL_date, np.nan)
                df[column_name] = pd.to_numeric(((df['BASELINE_DATE'] -
                                                  df.loc[BL_Event,var_column])/30.42) / (np.timedelta64(1, 'D')),errors='coerce')
                df=df.reset_index()
                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)
            else:
                df[column_name] = np.nan
                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)
        final_df=pd.concat(DF_list, ignore_index=True)
        final_df[column_name] = final_df[column_name].where(final_df['BASELINE_EVENT']!=BL_Event, np.nan)
        DF = pd.merge(DF,final_df, on=['PFIZER_CODE','BASELINE_EVENT'], how='left')
        return DF[column_name]
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
P2P_DF['Time_Elapsed_Since_DP3_date'] = Time_Elapsed_Since_BLEvent_Date(P2P_DF, 'DP3', 'DP3', 'Time_Elapsed_Since_DP3_date')
P2P_DF['Time_Elapsed_Since_Actual_PPS_date'] = Time_Elapsed_Since_BLEvent_Date(P2P_DF, 'Actual PPS', 'PHASE_3_START', 'Time_Elapsed_Since_Actual_PPS_date')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#if basline is DP3 then corresponding variance column values are changed to blank
# variance_columns = [ 'PFIZER_CODE','BASELINE_EVENT','BASELINE_DATE','PHASE_3_START','DP3', 'Time_Elapsed_Since_DP3_date', 'Time_Elapsed_Since_Actual_PPS_date']
# P2P_DF[variance_columns]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def date_if_DP3(DF,BL_Event, target_column, column_name):
    try:
        codes = list(DF['PFIZER_CODE'].unique())
        DF_list=[]
        for code in codes:
            df = DF[['BASELINE_EVENT','BASELINE_DATE','PFIZER_CODE',target_column]][DF['PFIZER_CODE'] == code]
            if df['BASELINE_EVENT'].isin([BL_Event]).any():
                df = df.set_index('BASELINE_EVENT')

                BL_date = df.loc[BL_Event,'BASELINE_DATE']
                TR_date = df.loc[BL_Event, target_column]

                df=df.reset_index()

                df[column_name] = np.where(df['BASELINE_EVENT']!=BL_Event, TR_date,np.nan)
                df[column_name] = df[column_name].where(df['BASELINE_DATE']>=BL_date, np.nan)

                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)

            else:
                df[column_name] = ''
                df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
                DF_list.append(df)

        final_df=pd.concat(DF_list, ignore_index=True).drop_duplicates()
        final_df[column_name] = pd.to_datetime(final_df[column_name], utc=True, errors='coerce')
        DF = pd.merge(DF,final_df, on=['PFIZER_CODE','BASELINE_EVENT'], how='left').reset_index(drop=True)
        return DF[column_name]
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
P2P_DF['PPS_Date_at_DP3'] = date_if_DP3(P2P_DF, 'DP3', 'PHASE_3_START', 'PPS_Date_at_DP3')

P2P_DF['NDA_Sub_Date_at_DP3'] = date_if_DP3(P2P_DF, 'DP3', 'NDA_SUBMISSION', 'NDA_Sub_Date_at_DP3')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#if basline is DP3 then corresponding variance column values are changed to blank
# variance_columns = [ 'PFIZER_CODE','BASELINE_EVENT','BASELINE_DATE','PHASE_3_START','NDA_SUBMISSION', 'PPS_Date_at_DP3', 'NDA_Sub_Date_at_DP3']
# P2P_DF[variance_columns]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# The key here is that the change is calculated against the value from the previous baseline milestone date
def diff_against_previous_value(df,cal_col,new_cal_col,diff_type='MNTS_diff'):
    try:
        df = df.sort_values(['PFIZER_CODE','BASELINE_DATE']).reset_index(drop=True)
        if diff_type=='MNTS_diff':

            df[new_cal_col] = df.groupby('PFIZER_CODE')[cal_col].diff()

            return df
        elif diff_type=='Date_diff':

            df[new_cal_col] = (df.groupby(['PFIZER_CODE'])[cal_col]
                                              .diff().div(30.42).div(np.timedelta64(1, 'D'))
                                              .fillna('')
                                              )
            return df
    except Exception as e:
        print (f'*****error at calculating diff_against_previous_value:{e}')

P2P_DF = diff_against_previous_value(P2P_DF,'PSTART_TO_APP_CT_MNTS','CHANGE_PSTART_TO_APP_CT_MNTS')
P2P_DF = diff_against_previous_value(P2P_DF,'PSTART_TO_SUB_CT_MNTS','CHANGE_PSTART_TO_SUB_CT_MNTS')
P2P_DF = diff_against_previous_value(P2P_DF,'VAR_PSTART_TO_SUB_BTW_BL','CHANGE_VAR_PSTART_TO_SUB_BTW_BL')
P2P_DF = diff_against_previous_value(P2P_DF,'Time_Elapsed_Since_DP3_date','CHANGE_TIME_ELAPSED_SINCE_DP3_DATE')
P2P_DF = diff_against_previous_value(P2P_DF,'NDA_SUBMISSION','CHANGE_NDA_SUBMISSION','Date_diff')
P2P_DF = diff_against_previous_value(P2P_DF,'NDA_APPROVAL','CHANGE_NDA_APPROVAL','Date_diff')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# moving column source to last second and adding Run_date Column at last.
try:
    col_name="Source"
    last_col = P2P_DF.pop(col_name)
    P2P_DF.insert(74, col_name, last_col)
except Exception as e:
        print('Unable to find', '->', e)
        pass

curr_date = pd.to_datetime(date.today(), utc=True)
P2P_DF['Run_Date'] = curr_date

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# sorting columns by code and by baseline date
try:
    codes = list(P2P_DF['PFIZER_CODE'].unique())
    df_list=[]
    for code in codes:
        P2P_DF1 = P2P_DF[P2P_DF['PFIZER_CODE']==code].copy()
        P2P_DF1 = P2P_DF1.sort_values(by='BASELINE_DATE')
        df_list.append(P2P_DF1)
    P2P_DF = pd.concat(df_list)
except Exception as e:
        print('Unable to find', '->', e)
        pass
# P2P_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# converting all the calculated columns to numeric data type
try:
    list_cols=['FSD_TO_LSLV_CT_MNTS', 'LSLV_TO_SUB_CT_MNTS', 'PSTART_TO_SUB_CT_MNTS', 'VAR_PSTART_TO_SUB_BTW_BL',
               'LSLV_TO_MAA_SUB_CT_MNTS', 'PSTART_TO_MAA_SUB_CT_MNTS', 'PPStart_Date_MS_Var_MNTS',
               'LSLV_NDA_Sub_Date_MS_Var_MNTS', 'NDA_Sub_Date_MS_Var_MNTS', 'LSLV_MAA_Sub_Date_MS_Var_MNTS',
               'MAA_Sub_Date_MS_Var_MNTS', 'PPStart_Date_MS_Var_DP3', 'LSLV_NDA_Sub_Date_MS_Var_DP3',
               'NDA_Sub_Date_MS_Var_DP3', 'LSLV_MAA_Sub_Date_MS_Var_DP3', 'MAA_Sub_Date_MS_Var_DP3', 'Time_Elapsed_Since_DP3',
              'Time_Elapsed_Since_Actual_PPS', 'Time_Elapsed_Since_DP3_date', 'Time_Elapsed_Since_Actual_PPS_date',
              'PSTART_TO_APP_CT_MNTS','SUB_TO_APP_CT_MNTS', 'CHANGE_PSTART_TO_APP_CT_MNTS', 'CHANGE_PSTART_TO_SUB_CT_MNTS',
               'CHANGE_VAR_PSTART_TO_SUB_BTW_BL', 'CHANGE_TIME_ELAPSED_SINCE_DP3_DATE', 'CHANGE_NDA_SUBMISSION',
               'CHANGE_NDA_APPROVAL']
    for col in list_cols:
        P2P_DF[col]=pd.to_numeric(P2P_DF[col],errors='coerce')#.astype(float)#.dtypes
except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

p2P_MAIN_HIST_PROD_COMPUTED_df = P2P_DF # Compute a Pandas dataframe to write into P2P_MAIN_HIST_PROD_COMPUTED


# Write recipe outputs
p2P_MAIN_HIST_PROD_COMPUTED = dataiku.Dataset("P2P_MAIN_HIST_PROD_COMPUTED")
p2P_MAIN_HIST_PROD_COMPUTED.write_with_schema(p2P_MAIN_HIST_PROD_COMPUTED_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Pivoting the Variance reason columns

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# creating a list with columns to which the Varaiance reasons columns would be pivoted
try:
    n_columns_list= P2P_DF.columns

    P2P_RC_DF = P2P_DF.copy()
    P2P_RC_DF[['Pivotal Program Start Variance Reason', 'LSLV for NDA Submission Variance Reason', 'NDA Submission Variance Reason', 'Pivotal Program Start to NDA Submission Variance Reason']] = P2P_RC_DF[['PPS_VAR_RC1', 'LSLV_NDA_SUB_VARIANCE_RC1', 'NDA_SUB_VARIANCE_RC1', 'NDA_PPS_TO_SUB_VARIANCE_RC1']]

    P2P_PIVOT_DF = P2P_RC_DF.melt(id_vars=n_columns_list, var_name='Baseline_Variance_Categories',
                                  value_name='Variance_Reasons')
except Exception as e:
        print('Unable to find', '->', e)
        pass
# P2P_PIVOT_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    codes = list(P2P_DF['PFIZER_CODE'].unique())
    df_list=[]
    for code in codes:
        P2P_PIVOT_DF1 = P2P_PIVOT_DF[P2P_PIVOT_DF['PFIZER_CODE']==code].copy()
        P2P_PIVOT_DF1 = P2P_PIVOT_DF1.sort_values(by='BASELINE_DATE')
        df_list.append(P2P_PIVOT_DF1)
    P2P_PIVOT_DF = pd.concat(df_list)
    P2P_PIVOT_DF=P2P_PIVOT_DF.reset_index(drop=True)
except Exception as e:
        print('Unable to find', '->', e)
        pass
# P2P_PIVOT_DF[:5]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

P2P_PIVOT_HIST_PROD_COMPUTED_df = P2P_PIVOT_DF#... # Compute a Pandas dataframe to write into P2P_MAIN_HISTORICAL_STAGING_COMPUTED


# Write recipe outputs
P2P_PIVOT_HIST_PROD_COMPUTED_DATA = dataiku.Dataset("P2P_PIVOT_HIST_PROD_COMPUTED")
P2P_PIVOT_HIST_PROD_COMPUTED_DATA.write_with_schema(P2P_PIVOT_HIST_PROD_COMPUTED_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Pivoting Milestone

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# creating a list with columns to which the Varaiance reasons columns would be pivoted
try:
    y_columns_list=[ 'PROJECT_NAME', 'BUSINESS_CATEGORY','CANDIDATE_PORTFOLIO_PRIORITY', 'MODALITY','BASELINE_DATE','BASELINE_EVENT', 'PFIZER_CODE',
                    'Source', 'Run_Date',  'PSTART_TO_SUB_CT_MNTS','PHASE_3_START' , 'LSLV_NDA_SUBMISSION', 'NDA_SUBMISSION',
                    'NDA_APPROVAL','MAA_APPROVAL']

    P2P_MIL_DF = P2P_DF[y_columns_list].copy()
    P2P_MIL_DF[['Phase 3 Start' , 'LSLV NDA Submission', 'NDA Submission','NDA APPROVAL','MAA APPROVAL']] = P2P_MIL_DF[['PHASE_3_START' , 'LSLV_NDA_SUBMISSION', 'NDA_SUBMISSION','NDA_APPROVAL','MAA_APPROVAL']]

    P2P_MIL_PIVOT_DF = P2P_MIL_DF.melt(id_vars=y_columns_list, var_name='Milestone', value_name='Milestone_Date').drop(['PHASE_3_START' , 'LSLV_NDA_SUBMISSION', 'NDA_SUBMISSION','NDA_APPROVAL','MAA_APPROVAL'], axis=1)

    P2P_MIL_PIVOT_DF = P2P_MIL_PIVOT_DF[['PFIZER_CODE','PROJECT_NAME', 'BUSINESS_CATEGORY', 'CANDIDATE_PORTFOLIO_PRIORITY','MODALITY','BASELINE_DATE','BASELINE_EVENT','PSTART_TO_SUB_CT_MNTS', 'Milestone', 'Milestone_Date','Source', 'Run_Date']]
#     P2P_MIL_PIVOT_DF['BASELINE_DATE'] = pd.to_datetime(P2P_MIL_PIVOT_DF['BASELINE_DATE'],unit='s').dt.date
#     P2P_MIL_PIVOT_DF['BASELINE_DATE'] = pd.to_datetime(P2P_MIL_PIVOT_DF['BASELINE_DATE'],utc=True)
except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

P2P_PIVOT_DATE_HIST_PROD_df = P2P_MIL_PIVOT_DF#... # Compute a Pandas dataframe to write into P2P_MAIN_HISTORICAL_STAGING_COMPUTED


# Write recipe outputs
P2P_PIVOT_DATE_HIST_PROD_DATA = dataiku.Dataset("P2P_PIVOT_DATE_HIST_PROD")
P2P_PIVOT_DATE_HIST_PROD_DATA.write_with_schema(P2P_PIVOT_DATE_HIST_PROD_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Older scripts and function

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# if baseline event is older than DP3 event then variance column values are blank
# cols_list_2 =

# P2P_DF_1 = P2P_DF.loc[P2P_DF.groupby(['PFIZER_CODE']).BASELINE_DATE.idxmin()]

# # P2P_DF_1[variance_columns]
# variance_columns1 = ['VAR_PSTART_TO_SUB_BTW_BL','PPStart_Date_MS_Var_DP3',
#                     'LSLV_NDA_Sub_Date_MS_Var_DP3',
#                      'NDA_Sub_Date_MS_Var_DP3', 'LSLV_MAA_Sub_Date_MS_Var_DP3', 'MAA_Sub_Date_MS_Var_DP3']
# P2P_DF1[variance_columns] = P2P_DF[variance_columns1].loc[P2P_DF.groupby(['PFIZER_CODE']).BASELINE_DATE.idxmin()] .where(P2P_DF['BASELINE_EVENT']=='DP3', np.nan)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# older function with condition for dp3 and events before dp3
# def var_date(DF, var_column, column_name):
#     try:
#         codes = list(DF['PFIZER_CODE'].unique())
#         DF_list=[]
#         for code in codes:
#             df = DF[['BASELINE_EVENT','PFIZER_CODE',var_column]][DF['PFIZER_CODE'] == code]
#             if df['BASELINE_EVENT'].str.contains('DP3').any():
#                 df = df.set_index('BASELINE_EVENT')
#                 df[column_name] = pd.to_numeric(((df[var_column] -
#                                                   df.loc['DP3',var_column])/30.42) / (np.timedelta64(1, 'D')),errors='coerce')

#                 df=df.reset_index()
#                 df=df[['PFIZER_CODE','BASELINE_EVENT',column_name]]
#                 DF_list.append(df)
#         final_df=pd.concat(DF_list, ignore_index=True)
#         final_df=final_df
#         final_df[column_name] = final_df[column_name].where(final_df['BASELINE_EVENT']!='DP3', np.nan)
#         DF = pd.merge(DF,final_df, on=['PFIZER_CODE','BASELINE_EVENT'], how='left')
#         return DF[column_name]
#     except Exception as e:
#         print('Unable to find', '->', e)
#         pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# try:
#     codes = list(P2P_DF['PFIZER_CODE'].unique())
#     DF_list=[]
#     for code in codes:
#         df = P2P_DF[['BASELINE_EVENT','PFIZER_CODE','PSTART_TO_SUB_CT_MNTS']][P2P_DF['PFIZER_CODE'] == code]
#         # dp3 = df.loc['DP3','PSTART_TO_SUB_CT_MNTS']#.values['DP3']
#         if df['BASELINE_EVENT'].str.contains('DP3').any():
#             df = df.set_index('BASELINE_EVENT')
#             df['VAR_PSTART_TO_SUB_BTW_BL'] = pd.to_numeric(df['PSTART_TO_SUB_CT_MNTS'] - df.loc['DP3','PSTART_TO_SUB_CT_MNTS'])
#             df=df.reset_index()
#             df=df[['PFIZER_CODE','BASELINE_EVENT','VAR_PSTART_TO_SUB_BTW_BL']]
#             DF_list.append(df)
#     final_df=pd.concat(DF_list, ignore_index=True)
#     final_df=final_df
#     # display(final_df)
#     P2P_DF = pd.merge(P2P_DF,final_df, on=['PFIZER_CODE','BASELINE_EVENT'], how='left')
# except Exception as e:
#     print('Unable to find', '->', e)
#     pass
