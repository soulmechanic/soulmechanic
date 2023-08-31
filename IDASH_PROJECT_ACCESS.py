# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime, timezone, date
import json
#Project Libs
from spfunctions import push_JSON_to_sharepoint
from IDASHFunctions import write_to_folder, jsonify

# Read recipe inputs
snpashotTeamRoster = dataiku.Dataset("SnpashotTeamRoster")
snpashotTeamRoster_df = snpashotTeamRoster.get_dataframe()

pmandPlannerRoster = dataiku.Dataset("PMandPlannerRoster_QUERY")
PM_PLANNER_ROSTER_df = pmandPlannerRoster.get_dataframe()

STOD_StudyRoster_raw = dataiku.Dataset("STOD_StudyRoster")
STOD_StudyRoster_raw_df = STOD_StudyRoster_raw.get_dataframe()

GRW_ONESOURCE_VW = dataiku.Dataset("GRW_ONESOURCE_VW_QUERY")
GRW_ONESOURCE_VW_df = GRW_ONESOURCE_VW.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
GRW_ONESOURCE_VW_df[['LOGON_DOMAIN', 'LOGON_ID']] = GRW_ONESOURCE_VW_df[['LOGON_DOMAIN', 'LOGON_ID']].apply(lambda x: x.astype(str).str.lower())#.str.lower()
GRW_ONESOURCE_VW_df = GRW_ONESOURCE_VW_df.rename(columns={'LOGON_ID':'NTUsername'})

################################################################################################################################

try:
    STOD_StudyRoster_raw_df = STOD_StudyRoster_raw_df.rename(columns={'PEOPLESOFTGUID':'GUID'})
    STOD_StudyRoster_raw_df['GUID'] = pd.to_numeric(STOD_StudyRoster_raw_df['GUID'],  errors='coerce')
    STOD_StudyRoster_df =  pd.merge(STOD_StudyRoster_raw_df,GRW_ONESOURCE_VW_df, on='GUID', how='left')
    STOD_StudyRoster_df['NTUsername'] = STOD_StudyRoster_df['LOGON_DOMAIN']+'\\'+STOD_StudyRoster_df['NTID']
    STOD_StudyRoster_df = (STOD_StudyRoster_df[['CODE','NTUsername']]
                           .drop_duplicates(['CODE','NTUsername'])
                           .sort_values(by='NTUsername').reset_index(drop=True).dropna())
except Exception as e:
        print('Unable to find', '->', e)

################################################################################################################################


PM_PLANNER_ROSTER_df = PM_PLANNER_ROSTER_df.rename(columns={'NTID':'NTUsername'})
pmandPlannerRoster_joined_df= (pd.merge(PM_PLANNER_ROSTER_df, GRW_ONESOURCE_VW_df[['NTUsername','LOGON_DOMAIN']],
                                        on='NTUsername', how='left'))

pmandPlannerRoster_joined_df['NTUsername'] = (pmandPlannerRoster_joined_df['LOGON_DOMAIN']
                                              +'\\'+pmandPlannerRoster_joined_df['NTUsername'])


################################################################################################################################

snpashotTeamRoster_df = snpashotTeamRoster_df[['Code','NTUsername']].rename(columns={'Code': 'code'})
STOD_StudyRoster_df = STOD_StudyRoster_df[['CODE','NTUsername']].rename(columns={'CODE': 'code'})
PM_PLANNER_ROSTER_df = pmandPlannerRoster_joined_df[['Code','NTUsername']].rename(columns={'Code': 'code'})

dflist = [snpashotTeamRoster_df, STOD_StudyRoster_df, PM_PLANNER_ROSTER_df]

ProjectAccessDF = pd.concat(dflist)
ProjectAccessDF = ProjectAccessDF.drop_duplicates(['code', 'NTUsername']).dropna()
ProjectAccessDF = ProjectAccessDF.loc[ProjectAccessDF['NTUsername']!='amer']
ProjectAccessDF = ProjectAccessDF.loc[ProjectAccessDF['NTUsername']!='apac']

def add_project_access_codes(df, col_name):
    try:
        df = df[df['NTUsername'].notnull()].sort_values(by=["NTUsername"])
        df = df.rename(columns={'NTUsername': 'ntid'})
        Final_lst=[]
        uniq_NTID = list(df["ntid"].unique())
        for NTID in uniq_NTID:
            final_dict= {}

            F_df = df[df['ntid']==NTID]

            code_list = list(F_df['code'].unique())
            ntid_dict = F_df[['ntid']].drop_duplicates().to_dict(orient='records')


            final_dict['ntid'] = NTID
            final_dict[col_name] = code_list

            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        final_df = final_df[['ntid','code']].reset_index(drop=True)

        return final_df
    except Exception as e:
        return 'Unable to find', '->', str(e)

prj_access_df = add_project_access_codes(ProjectAccessDF, 'code')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Publish json

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# generating json from dataframe
prj_access_json = jsonify(prj_access_df,'recipe_from_notebook_notebook_editor_for_compute_PROJECT_ACCESS_FINALCopy1_1',
                          'Access')

#######################################################################################################################

 # publishing json file to S3 folder
filenameMAIN = 'ProjectAccess_DSS.txt'
folders = ['PROJECT_ACCESS_S3_FOLDER']
write_to_folder(prj_access_json,folders,filenameMAIN)
