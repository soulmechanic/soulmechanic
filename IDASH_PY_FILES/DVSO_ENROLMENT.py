# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from jsonfunctions import prep_for_JSON
from IDASHFunctions import write_to_folder, jsonify
import json

# Read recipe inputs
stg_DVSO_ENROLLMENT = dataiku.Dataset("STG_DVSO_ENROLLMENT")
stg_DVSO_ENROLLMENT_df = stg_DVSO_ENROLLMENT.get_dataframe()

# prot_id = 'C4891001'

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# filter data based on two conditions
def Filter_df(df,cond1,cond2):
    f_df = df[(df.event==cond1) & (df.curve_type==cond2)].reset_index(drop=True)
    return f_df
# drop rows based on two conditions
def DropFilter_df(df,cond1,cond2):
    df = df.drop(df[(df.event==cond1) & (df.curve_type=="projected")].index)
    f_df = df.drop(df[(df.event==cond2) & (df.curve_type=="projected")].index)
    return f_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
from functools import reduce
def trunc_projected(DF,EVENT_TYPE):

    PLN_DF = Filter_df(DF,EVENT_TYPE,"planned")
    GBY_PLN_DF = pd.DataFrame(PLN_DF.groupby('protocol_id')['event_date'].max())
    PRJ_DF = Filter_df(DF,EVENT_TYPE,"projected")
    PRID_LIST = list(set(PRJ_DF['protocol_id']))


    if not GBY_PLN_DF.empty:
        F_GBY_PLN_DF = GBY_PLN_DF.loc[PRID_LIST]

        PRJ_DFS = []

        for P_ID, row in F_GBY_PLN_DF.iterrows():
            F_PRJ_DF = PRJ_DF[PRJ_DF['protocol_id']==P_ID].set_index("event_date").sort_index()

            T_PRJ_DF = F_PRJ_DF.truncate(after=row['event_date']).reset_index()

            PRJ_DFS.append(T_PRJ_DF)


        if PRJ_DFS:

            return pd.concat(PRJ_DFS, ignore_index=True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def get_enrollment_data(ENRL_DF):

    NON_SC_ENROL_PRJ_DF = DropFilter_df(ENRL_DF,"screenings","enrollments")
    SC_PRJ_DF = trunc_projected(ENRL_DF,"screenings")

    ENROL_PRJ_DF = trunc_projected(ENRL_DF,"enrollments")
    LIST_DFS = [NON_SC_ENROL_PRJ_DF, SC_PRJ_DF, ENROL_PRJ_DF]
    if LIST_DFS:
        return  pd.concat(LIST_DFS, ignore_index=True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
DFS_LIST = []
NPIDs = []
P_LIST = list(set(stg_DVSO_ENROLLMENT_df['protocol_id']))
for P_ID in P_LIST:

    FIL_DF = stg_DVSO_ENROLLMENT_df[stg_DVSO_ENROLLMENT_df['protocol_id']==P_ID].reset_index(drop=True)
    
#     if P_ID ==prot_id:
#         display(FIL_DF[FIL_DF['protocol_id']==prot_id])
    
    IF_CONTAIN_PROJECTED = FIL_DF['curve_type'].str.contains('projected').any()
    if IF_CONTAIN_PROJECTED:
        e_df = get_enrollment_data(FIL_DF)
#         if P_ID ==prot_id:
#             display(e_df[e_df['protocol_id']==prot_id])
        DFS_LIST.append(e_df)
    elif not IF_CONTAIN_PROJECTED:
        NPIDs.append(P_ID)
if DFS_LIST:
    enrollment_df = pd.concat(DFS_LIST, ignore_index=True)

NON_PRJ_DF = stg_DVSO_ENROLLMENT_df[stg_DVSO_ENROLLMENT_df.protocol_id.isin(NPIDs)]

FINAL_DF = pd.concat([enrollment_df,NON_PRJ_DF], ignore_index=True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
df = FINAL_DF.applymap(prep_for_JSON).astype(str)
# JSON_D = (json.dumps([row.dropna().to_dict() for index,row in df.iterrows()],indent=1,ensure_ascii=False,default=str))

# write_to_folder(JSON_D,['IDASH_DVSO_S3_FOLDER'],'DVSO_DSS.txt')


# Write recipe outputs
IDASH_DVSO_ENROLLMENT_SVC_STG_S3 = dataiku.Dataset("IDASH_DVSO_ENROLLMENT_SVC_STG_S3")
IDASH_DVSO_ENROLLMENT_SVC_STG_S3.write_with_schema(df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## older code deleted after few tests

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# DFS_LIST = []
# PIDs = []
# P_LIST = list(set(stg_DVSO_ENROLLMENT_df['protocol_id']))

# for P_ID in P_LIST:

#     FIL_DF = stg_DVSO_ENROLLMENT_df[stg_DVSO_ENROLLMENT_df['protocol_id']==P_ID].reset_index(drop=True)

#     CONTAIN_PROJECTED = FIL_DF['curve_type'].str.contains('projected').any()

#     if CONTAIN_PROJECTED:

#         SC_MAX_E_DATE = FIL_DF['event_date'].loc[(FIL_DF['event']=='screenings') & (FIL_DF['curve_type']=='planned')].max()
#         SC_FIL_DF = FIL_DF.drop(FIL_DF[(FIL_DF.event=="screenings") & (FIL_DF.curve_type=="projected") & (FIL_DF.event_date>SC_MAX_E_DATE)].index)
#         EN_MAX_E_DATE = FIL_DF['event_date'].loc[(FIL_DF['event']=='enrollments') & (FIL_DF['curve_type']=='planned')].max()
#         EN_FIL_DF = SC_FIL_DF.drop(SC_FIL_DF[(SC_FIL_DF.event=="enrollments") & (SC_FIL_DF.curve_type=="projected") & (SC_FIL_DF.event_date>EN_MAX_E_DATE)].index)
#         DFS_LIST.append(EN_FIL_DF)
#     elif not CONTAIN_PROJECTED:
#         DFS_LIST.append(FIL_DF)

# FINAL_DF = pd.concat(DFS_LIST, ignore_index=True)
# FINAL_DF
