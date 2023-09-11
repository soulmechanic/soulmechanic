# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime, timezone, date
from P2P_DataTransformFunctions import (GetDiffBwColumns, ChangeDateFormat, force_proj_in_out, overwrite_data_p2p)

# Read recipe inputs
p2P_MAIN_PROD = dataiku.Dataset("P2P_MAIN_PROD_QUERY")
p2P_MAIN_PROD_df = p2P_MAIN_PROD.get_dataframe()

p2P_Historical_15212020 = dataiku.Dataset("P2P_Historical_15212020")
p2P_Historical_15212020_df = p2P_Historical_15212020.get_dataframe()

kpi_Historical_15212020 = dataiku.Dataset("KPI_Historical_15212020")
kpi_Historical_15212020_df = kpi_Historical_15212020.get_dataframe()

p2P_KPI_PROD = dataiku.Dataset("P2P_KPI_PROD_QUERY")
p2P_KPI_PROD_df = p2P_KPI_PROD.get_dataframe()

PROJ_FORCE_IN_OUT = dataiku.Dataset("PROJ_FORCE_IN_OUT")
PROJ_FORCE_IN_OUT_df = PROJ_FORCE_IN_OUT.get_dataframe()

OVERWRITE_P2P = dataiku.Dataset("P2P_OVERWRITE_TABLE")
OVERWRITE_P2P_DF = OVERWRITE_P2P.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
kpi_Historical_15212020_df['KPI_Source'] = 'Historical'

p2P_KPI_PROD_df['KPI_Source'] = 'Prod'

p2P_Historical_15212020_df['P2P_Source'] = 'Historical'

p2P_MAIN_PROD_df['P2P_Source'] = 'Prod'

#temporaryly removing B801a project with Baseline even E2 (later find a method to do it)

# p2P_MAIN_PROD_df = p2P_MAIN_PROD_df.drop(p2P_MAIN_PROD_df[(p2P_MAIN_PROD_df['PFIZER_CODE'] == 'B801a') &
#                                                           (p2P_MAIN_PROD_df['BASELINE_EVENT'] == 'E2')].index)


p2P_MAIN_PROD_df = force_proj_in_out(p2P_MAIN_PROD_df, PROJ_FORCE_IN_OUT_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
kpi_Historical_15212020_df['KPI_LE'] = np.where((kpi_Historical_15212020_df['KPI_LE']== '00:00.0'),
                                                '', kpi_Historical_15212020_df['KPI_LE'])
# kpi_Historical_15212020_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### generating P2P_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    columns_list=['PFIZER_CODE', 'PROJECT_NAME', 'BUSINESS_CATEGORY', 'MODALITY', 'BASELINE_EVENT',
                  'BASELINE_DATE', 'P2P_Source']

    p2P_Historical_15212020_df = p2P_Historical_15212020_df[columns_list]

    p2P_MAIN_PROD_df = p2P_MAIN_PROD_df[columns_list]

    P2P_H_P_UNION_DF = (pd.concat([p2P_Historical_15212020_df,p2P_MAIN_PROD_df])
                        .reset_index(drop=True))
    P2P_H_P_UNION_DF['BASELINE_DATE'] = pd.to_datetime(P2P_H_P_UNION_DF['BASELINE_DATE'], utc=True)
    P2P_H_P_UNION_DF = (P2P_H_P_UNION_DF.sort_values(by=['PFIZER_CODE','BASELINE_DATE'])
                        .drop_duplicates(subset=['PFIZER_CODE','BASELINE_EVENT'], keep='first').reset_index(drop=True))
except Exception as e:
        print('Unable to find', '->', e)
        pass

# P2P_H_P_UNION_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# To Overwite certain values manually defiend in a static editable table "OVERWRITE_CIS_FIH"
P2P_COLUMNS = P2P_H_P_UNION_DF.columns.tolist()
remove_key_cols = {'PFIZER_CODE','BASELINE_EVENT'}
P2P_COLUMNS = [ele for ele in P2P_COLUMNS if ele not in remove_key_cols]

for col in P2P_COLUMNS:
    P2P_H_P_UNION_DF[col] = P2P_H_P_UNION_DF.apply(lambda x:overwrite_data_p2p(OVERWRITE_P2P_DF, x['PFIZER_CODE'], x['BASELINE_EVENT'],col,x[col]),axis=1)
    
P2P_H_P_UNION_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Generating KPI DATA

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    # renaming the columns accordingly
    kpi_Historical_15212020_df = kpi_Historical_15212020_df.rename(columns={'OS_SnapDate':'OneSource_Date',
                                                                            'CANDIDATE_CODE':'PFIZER_CODE'})

    p2P_KPI_PROD_df = p2P_KPI_PROD_df.rename(columns={'CANDIDATE_CODE':'PFIZER_CODE','SNP_DATE':'OneSource_Date'})

    # filtering the list of columns
    kpi_columns_list=['PFIZER_CODE', 'OneSource_Date', 'GOAL_DESCRIPTION', 'GOAL_STATUS','GOAL_NUMBER',
                      'GOAL_KPI_NUMBER', 'KPI_STATUS', 'KPI_DESCRIPTION','KPI_COMMENTS','KPI_MITIGATION_PLAN',
                      'PROJECT_KPI_GOALS_ID','GOAL_LE','GOAL_TARGET_DATE','GOAL_ACTUAL_DATE','GOAL_ARCHIVE_FLAG',
                      'GOAL_MODIFIED_BY','GOAL_MODIFIED_DATE','KPI_NUMBER','KPI_LE','KPI_TARGET_DATE','KPI_ACTUAL_DATE',
                      'KPI_IN_REVIEW_FLAG','KPI_ACCOUNTABLE_LINE_ID','KPI_ACCOUNTABLE_LINE_DESC','KPI_ACCOUNTABLE_OWNER',
                      'KPI_STUDY_NUMBER','KPI_MODIFIED_BY','KPI_MODIFIED_DATE','KPI_DETAILS_ID', 'KPI_Source'
                    ]

    kpi_Historical_15212020_df = kpi_Historical_15212020_df[kpi_columns_list]

    p2P_KPI_PROD_df = p2P_KPI_PROD_df[kpi_columns_list]

    # appending both the data historical and stage data

    KPI_H_P_UNION_DF = (kpi_Historical_15212020_df.append(p2P_KPI_PROD_df)
                        .reset_index(drop=True))
#     KPI_H_S_UNION_DF['OneSource_Date'] = pd.to_datetime(KPI_H_S_UNION_DF['OneSource_Date'], utc=True)
except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def StrDateToISOFormat(DF, list_date_cols):
    try:
        for col in list_date_cols:
            DF[col] = pd.to_datetime(DF[col], utc=True, errors='coerce')#.dt.strftime('%m-%d-%Y')
        return DF
    except Exception as e:
        print('Unable to find', '->', e)
        pass

list_date_cols = ['OneSource_Date', 'GOAL_LE', 'GOAL_TARGET_DATE', 'GOAL_ACTUAL_DATE',
                  'GOAL_MODIFIED_DATE','KPI_LE', 'KPI_TARGET_DATE', 'KPI_ACTUAL_DATE',
                  'KPI_MODIFIED_DATE']
KPI_H_P_UNION_DF = StrDateToISOFormat(KPI_H_P_UNION_DF, list_date_cols)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# KPI_H_P_UNION_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Join two datasets to create Snapshot KPIs dataset

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Version 3
def get_snapshot_kpi(P2P_df,KPI_df):
    pd.set_option('mode.chained_assignment', None)
    df = P2P_df.merge(KPI_df, on='PFIZER_CODE', how='left')
    df['DateDiff'] = ( df['OneSource_Date'] - df['BASELINE_DATE']).div(np.timedelta64(1, 'D'))#.abs()
    project_codes = list(df['PFIZER_CODE'].unique())
    DF_list=[]
    for project_code in project_codes:
        project_code_df = df[df['PFIZER_CODE']==project_code]
        b_events = list(project_code_df['BASELINE_EVENT'].unique())
        for b_event in b_events:
            filter_df = project_code_df[(project_code_df['BASELINE_EVENT'] == b_event)]
            if (filter_df['DateDiff']<0).all():
                filter_df['DateDiff'] = filter_df['DateDiff'].abs()
                filter_df = (filter_df[(filter_df['DateDiff'] == filter_df['DateDiff'].min())]
                             .sort_values(by=['BASELINE_EVENT','GOAL_KPI_NUMBER']))
                DF_list.append(filter_df)
            else:
                filter_df['DateDiff'] = np.where(filter_df['DateDiff'] < 0, 99999,filter_df['DateDiff'])
                filter_df = (filter_df[(filter_df['DateDiff'] == filter_df['DateDiff'].min())]
                             .sort_values(by=['BASELINE_EVENT','GOAL_KPI_NUMBER']))
                DF_list.append(filter_df)
    final_df=pd.concat(DF_list, ignore_index=True)
    return final_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# generating snapshot kpi using custom function
final_df = get_snapshot_kpi(P2P_H_P_UNION_DF, KPI_H_P_UNION_DF)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# the historical baselines with a baseline date of 3/15/2021, 4/27/2021, and 5/9/2021 will not have Snapshot KPI information linked to them
# logic applied on 05/26/2021
list_of_dates_exceluded = ['2021-03-15 00:00:00+00:00', '2021-04-27 00:00:00+00:00', '2021-05-09 00:00:00+00:00']
list_of_snp_kpi_cols = ['OneSource_Date', 'GOAL_DESCRIPTION', 'GOAL_STATUS', 'GOAL_NUMBER', 'GOAL_KPI_NUMBER',
                     'KPI_STATUS', 'KPI_DESCRIPTION', 'KPI_COMMENTS', 'KPI_MITIGATION_PLAN',
                     'PROJECT_KPI_GOALS_ID',
                     'GOAL_LE', 'GOAL_TARGET_DATE', 'GOAL_ACTUAL_DATE', 'GOAL_ARCHIVE_FLAG', 'GOAL_MODIFIED_BY',
                     'GOAL_MODIFIED_DATE', 'KPI_NUMBER', 'KPI_LE', 'KPI_TARGET_DATE', 'KPI_ACTUAL_DATE',
                     'KPI_IN_REVIEW_FLAG', 'KPI_ACCOUNTABLE_LINE_ID', 'KPI_ACCOUNTABLE_LINE_DESC', 'KPI_ACCOUNTABLE_OWNER',
                     'KPI_STUDY_NUMBER', 'KPI_MODIFIED_BY', 'KPI_MODIFIED_DATE', 'KPI_DETAILS_ID']

final_df[list_of_snp_kpi_cols] = final_df[list_of_snp_kpi_cols].where((['P2P_Source'] != 'Historical') and
                                                                      (~final_df['BASELINE_DATE'].isin(list_of_dates_exceluded)), 
                                                                      np.nan)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
curr_date = pd.to_datetime(date.today(), utc=True)
final_df['Run_Date'] = curr_date

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
final_df = final_df[['PFIZER_CODE', 'PROJECT_NAME', 'BUSINESS_CATEGORY', 'MODALITY', 'BASELINE_DATE', 'BASELINE_EVENT',
                      'OneSource_Date', 'GOAL_DESCRIPTION', 'GOAL_STATUS', 'GOAL_NUMBER', 'GOAL_KPI_NUMBER',
                     'KPI_STATUS', 'KPI_DESCRIPTION', 'KPI_COMMENTS', 'KPI_MITIGATION_PLAN', 'P2P_Source','KPI_Source',
                     'PROJECT_KPI_GOALS_ID',
                     'GOAL_LE', 'GOAL_TARGET_DATE', 'GOAL_ACTUAL_DATE', 'GOAL_ARCHIVE_FLAG', 'GOAL_MODIFIED_BY',
                     'GOAL_MODIFIED_DATE', 'KPI_NUMBER', 'KPI_LE', 'KPI_TARGET_DATE', 'KPI_ACTUAL_DATE',
                     'KPI_IN_REVIEW_FLAG', 'KPI_ACCOUNTABLE_LINE_ID', 'KPI_ACCOUNTABLE_LINE_DESC', 'KPI_ACCOUNTABLE_OWNER',
                     'KPI_STUDY_NUMBER', 'KPI_MODIFIED_BY', 'KPI_MODIFIED_DATE', 'KPI_DETAILS_ID', 'Run_Date']]
# final_df.head(200)



# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

p2P_SNAP_KPI_HIST_PROD_COMPUTED_df = final_df # Compute a Pandas dataframe to write into P2P_SNAP_KPI_HIST_PROD_COMPUTED


# Write recipe outputs
p2P_SNAP_KPI_HIST_PROD_COMPUTED = dataiku.Dataset("P2P_SNAP_KPI_HIST_PROD_COMPUTED")
p2P_SNAP_KPI_HIST_PROD_COMPUTED.write_with_schema(p2P_SNAP_KPI_HIST_PROD_COMPUTED_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Previous versions of the code

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Version 1
# def get_snapshot_kpi(P2P_df,KPI_df):
#     df = P2P_df.merge(KPI_df, on='PFIZER_CODE')
#     df['DateDiff'] = ( df['OneSource_Date'] - df['BASELINE_DATE']).div(np.timedelta64(1, 'D'))#.abs()
# #     df['DateDiff'] = np.where(df['OneSource_Date'] < df['BASELINE_DATE'], 99999,df['DateDiff'])
# #     display(df)
#     df['DateDiff'] = np.where(df['DateDiff'] < 0, 99999,df['DateDiff'])
# #     display(df)
#     code_list = list(df['PFIZER_CODE'].unique())
#     DF_list=[]
#     for code in code_list:
# #         print(code)
# #         display(m_df)
#         c_df = df[df['PFIZER_CODE']==code]
# #         display(c_df)
#         b_events = list(c_df['BASELINE_EVENT'].unique())

#         for b in b_events:
# #             print(b)
#             b_df = c_df[(c_df['BASELINE_EVENT'] == b)]

#             b_df = b_df[(b_df['DateDiff'] == b_df['DateDiff'].min())]
#             b_df = (b_df.sort_values(by=['PFIZER_CODE','OneSource_Date'], ascending=False)
#                    .drop_duplicates(subset=['PFIZER_CODE','BASELINE_EVENT', 'GOAL_KPI_NUMBER', 'KPI_DESCRIPTION'], keep='first').reset_index(drop=True)
#                    .sort_values(by=['BASELINE_EVENT','GOAL_KPI_NUMBER']))
#             display(b_df)
#             DF_list.append(b_df)
#     final_df=pd.concat(DF_list, ignore_index=True)
#     return final_df
# # final_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Version 2
# def get_snapshot_kpi(P2P_df,KPI_df):
#     df = P2P_df.merge(KPI_df, on='PFIZER_CODE', how='left')
#     df['DateDiff'] = ( df['OneSource_Date'] - df['BASELINE_DATE']).div(np.timedelta64(1, 'D'))#.abs()

#     code_list = list(df['PFIZER_CODE'].unique())
#     DF_list=[]
#     for code in code_list:
#         c_df = df[df['PFIZER_CODE']==code]
#         b_events = list(c_df['BASELINE_EVENT'].unique())

#         for b in b_events:
#             filter_df = c_df[(c_df['BASELINE_EVENT'] == b)]
#             if (filter_df['DateDiff']<0).all():
#                 filter_df['DateDiff'] = filter_df['DateDiff'].abs()
# #                 display(filter_df[['PFIZER_CODE', 'BASELINE_DATE', 'BASELINE_EVENT','OneSource_Date','DateDiff']])
#                 filter_df = filter_df[(filter_df['DateDiff'] == filter_df['DateDiff'].min())].reset_index(drop=True)
#                 display(filter_df[['PFIZER_CODE', 'BASELINE_DATE', 'BASELINE_EVENT','OneSource_Date', 'GOAL_KPI_NUMBER', 'KPI_DESCRIPTION','DateDiff']])
# #                 filter_df = (filter_df.sort_values(by=['PFIZER_CODE','OneSource_Date'], ascending=False)
# #                        .drop_duplicates(subset=['PFIZER_CODE','BASELINE_EVENT', 'GOAL_KPI_NUMBER', 'KPI_DESCRIPTION'], keep='first').reset_index(drop=True)
# #                        .sort_values(by=['BASELINE_EVENT','GOAL_KPI_NUMBER']))
#                 DF_list.append(filter_df)
#             else:
#                 filter_df['DateDiff'] = np.where(filter_df['DateDiff'] < 0, 99999,filter_df['DateDiff'])
# #                 display(filter_df[['PFIZER_CODE', 'BASELINE_DATE', 'BASELINE_EVENT','OneSource_Date','DateDiff']])
#                 filter_df = filter_df[(filter_df['DateDiff'] == filter_df['DateDiff'].min())].reset_index(drop=True)
#                 display(filter_df[['PFIZER_CODE', 'BASELINE_DATE', 'BASELINE_EVENT','OneSource_Date', 'GOAL_KPI_NUMBER', 'KPI_DESCRIPTION','DateDiff']])
# #                 filter_df = (filter_df.sort_values(by=['PFIZER_CODE','OneSource_Date'], ascending=False)
# #                        .drop_duplicates(subset=['PFIZER_CODE','BASELINE_EVENT', 'GOAL_KPI_NUMBER', 'KPI_DESCRIPTION'], keep='first').reset_index(drop=True)
# #                        .sort_values(by=['BASELINE_EVENT','GOAL_KPI_NUMBER']))
#                 DF_list.append(filter_df)
#     final_df=pd.concat(DF_list, ignore_index=True)
#     return final_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# # filtering all the duplicate Baseline Events

# try:
#     code_list = list(P2P_H_S_UNION_DF['PFIZER_CODE'].unique())
#     df_lst=[]
#     for code in code_list:
#         f_df = P2P_H_S_UNION_DF[P2P_H_S_UNION_DF['PFIZER_CODE']==code]
#         f_df['BASELINE_DATE'] = pd.to_datetime(f_df['BASELINE_DATE'], utc=True)
#         display(f_df)#
#         f_df=f_df.sort_values(by='BASELINE_DATE')
#         d_df = f_df.drop_duplicates(subset=['BASELINE_EVENT'], keep='first')
#         display(d_df)
#         df_lst.append(d_df)
#     P2P_DF = pd.concat(df_lst).reset_index(drop=True)
#     P2P_DF['BASELINE_DATE'] = pd.to_datetime(P2P_DF['BASELINE_DATE'], utc=True)
# except Exception as e:
#         print('Unable to find', '->', e)
#         pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#     p2P_KPI_PROD_df = p2P_KPI_PROD_df.rename(columns={
#                                                     'CANDPROJCODE':'PFIZER_CODE',
#                                                     'CANDPROJECTKPIGOALSID':'PROJECT_KPI_GOALS_ID',
#                                                     'CANDGOALDESCRIPTION':'GOAL_DESCRIPTION',
#                                                     'CANDGOALLE':'GOAL_LE',
#                                                     'CANDGOALTARGETDATE':'GOAL_TARGET_DATE',
#                                                     'CANDGOALACTUALDATE':'GOAL_ACTUAL_DATE',
#                                                     'CANDGOALSTATUSDESC':'GOAL_STATUS',
#                                                     'CANDGOALARCHIVEFLAG':'GOAL_ARCHIVE_FLAG',
#                                                     'CANDGOALNUMBER':'GOAL_NUMBER',
#                                                     'CANDGOALMODIFIEDBY':'GOAL_MODIFIED_BY',
#                                                     'CANDGOALMODIFIEDDATE':'GOAL_MODIFIED_DATE',
#                                                     'CANDKPINUMBER':'KPI_NUMBER',
#                                                     'CANDGOALKPINUMBER':'GOAL_KPI_NUMBER',
#                                                     'CANDKPIDESCRIPTION':'KPI_DESCRIPTION',
#                                                     'CANDKPILE':'KPI_LE',
#                                                     'CANDKPITARGETDATE':'KPI_TARGET_DATE',
#                                                     'CANDKPIACTUALDATE':'KPI_ACTUAL_DATE',
#                                                     'CANDKPISTATUSDESC':'KPI_STATUS',
#                                                     'CANDKPICOMMENTS':'KPI_COMMENTS',
#                                                     'CANDKPIMITIGATIONPLAN':'KPI_MITIGATION_PLAN',
#                                                     'CANDKPIINREVIEWFLAG':'KPI_IN_REVIEW_FLAG',
#                                                     'CANDKPIACCOUNTABLELINEID':'KPI_ACCOUNTABLE_LINE_ID',
#                                                     'CANDKPIACCOUNTABLELINEDESC':'KPI_ACCOUNTABLE_LINE_DESC',
#                                                     'CANDKPIACCOUNTABLEOWNER':'KPI_ACCOUNTABLE_OWNER',
#                                                     'CANDKPISTUDYNUMBER':'KPI_STUDY_NUMBER',
#                                                     'CANDKPIMODIFIEDBY':'KPI_MODIFIED_BY',
#                                                     'CANDKPIMODIFIEDDATE':'KPI_MODIFIED_DATE',
#                                                     'CANDPROJECTKPIDETAILSID':'KPI_DETAILS_ID',
#                                                     'SNP_DATE':'OneSource_Date'
#                                                     })
