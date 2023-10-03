# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import re
from datetime import date
from datetime import datetime, timedelta
import functools
import json
from jsonfunctions import prep_for_JSON
from spfunctions import push_JSON_to_sharepoint
from IDASHFunctions import write_to_folder, jsonify
# from StudyData_Python_Functions import *
# gen_dict_for_each_study, gen_dvso_rec_logic, Get_Study_Milestones_Dict, Get_Study_NextMilestone_Dict



# Read recipe inputs
qms_MOPS_STUDY_ISSUES_QUERY_PREP = dataiku.Dataset("QMS_MOPS_STUDY_ISSUES_QUERY_PREP")
qms_MOPS_STUDY_ISSUES_QUERY_PREP_df = qms_MOPS_STUDY_ISSUES_QUERY_PREP.get_dataframe()

mops_CRC_WI_STUDIES_QUERY = dataiku.Dataset("MOPS_CRC_WI_STUDIES_QUERY")
mops_CRC_WI_STUDIES_QUERY_df = mops_CRC_WI_STUDIES_QUERY.get_dataframe()

qms_MOPS_STUDY_ATTRIBUTES_QUERY = dataiku.Dataset("QMS_MOPS_STUDY_ATTRIBUTES_QUERY")
qms_MOPS_STUDY_ATTRIBUTES_QUERY_df = qms_MOPS_STUDY_ATTRIBUTES_QUERY.get_dataframe()

distinct_STUDY_LISTING_QUERY = dataiku.Dataset("DISTINCT_STUDY_LISTING_QUERY")
distinct_STUDY_LISTING_QUERY_df = distinct_STUDY_LISTING_QUERY.get_dataframe()

study_MILESTONES_CRC_QUERY = dataiku.Dataset("STUDY_MILESTONES_CRC_QUERY")
study_MILESTONES_CRC_QUERY_df = study_MILESTONES_CRC_QUERY.get_dataframe()

study_MILESTONES_MAIN_QUERY = dataiku.Dataset("STUDY_MILESTONES_MAIN_QUERY")
study_MILESTONES_MAIN_QUERY_df = study_MILESTONES_MAIN_QUERY.get_dataframe()

ar_UDDL_TRAFFIC_LIGHTS_PREP = dataiku.Dataset("AR_UDDL_TRAFFIC_LIGHTS_QUERY")
ar_UDDL_TRAFFIC_LIGHTS_PREP_df = ar_UDDL_TRAFFIC_LIGHTS_PREP.get_dataframe()

# made changes to source data to cdq on 12/3/2021
ar_UDDL_COUNTRY_STATUS = dataiku.Dataset("AR_UDDL_COUNTRY_STATUS")
ar_UDDL_COUNTRY_STATUS_df = ar_UDDL_COUNTRY_STATUS.get_dataframe()

DVSO_RECRUITMENT_DATA_QUERY = dataiku.Dataset("CDQ_DVSO_RECRUITMENT_DATA_QUERY")
DVSO_RECRUITMENT_DATA_QUERY_df = DVSO_RECRUITMENT_DATA_QUERY.get_dataframe()

STUDY_COMPLETION_QUERY = dataiku.Dataset("STUDY_COMPLETION_QUERY")
STUDY_COMPLETION_QUERY_df = STUDY_COMPLETION_QUERY.get_dataframe()

# made changes to source data to SES Oracle on 05/09/2022
NON_DVSO_STUDIES_QUERY = dataiku.Dataset("NON_DVSO_SES_DATA")
NON_DVSO_STUDIES_QUERY_df = NON_DVSO_STUDIES_QUERY.get_dataframe()

StudyMilestoneHashTable = dataiku.Dataset("StudyMilestoneHashTable")
StudyMilestoneHashTable_df = StudyMilestoneHashTable.get_dataframe()

StudyDataIntegrated = dataiku.Dataset("StudyDataIntegrated")
StudyDataIntegrated_orig_df = StudyDataIntegrated.get_dataframe()

DVSO_STUDY_NEXT_MILESTONE_DATE_QUERY = dataiku.Dataset("DVSO_STUDY_NEXT_MILESTONE_DATE_QUERY")
DVSO_STUDY_NEXT_MILESTONE_DATE_QUERY_df = DVSO_STUDY_NEXT_MILESTONE_DATE_QUERY.get_dataframe()

# made changes to source data to cv_study_task_data on 03/12/2022
STUDY_NEXT_MILESTONE_DATE = dataiku.Dataset("STUDY_NEXT_MILESTONE_DATE")
STUDY_NEXT_MILESTONE_DATE_df = STUDY_NEXT_MILESTONE_DATE.get_dataframe()
STUDY_NEXT_MILESTONE_DATE_df = STUDY_NEXT_MILESTONE_DATE_df[['Study_Number', 'StudyNextMilestone', 'StudyNextMilestoneDate']]

# made changes to source data to cdq on 12/3/2021
DVSO_RECRUITMENT_CURVE_YN = dataiku.Dataset("CDQ_DVSO_RECRUITMENT_CURVE_YN")
DVSO_RECRUITMENT_CURVE_YN_df = DVSO_RECRUITMENT_CURVE_YN.get_dataframe()

DVSO_RAND_NEXT_MILESTONE_PLW_JOINED = dataiku.Dataset("DVSO_RAND_NEXT_MILESTONE_PLW_JOINED")
DVSO_RAND_NEXT_MILESTONE_PLW_JOINED_df = DVSO_RAND_NEXT_MILESTONE_PLW_JOINED.get_dataframe()

STUDY_ENROLLED_PSI_YN_QUERY = dataiku.Dataset("STUDY_ENROLLED_PSI_YN_QUERY")
STUDY_ENROLLED_PSI_YN_QUERY_df = STUDY_ENROLLED_PSI_YN_QUERY.get_dataframe()

MOPS_STUDYROLES_CPMANDCSTL_QUERY = dataiku.Dataset("MOPS_STUDYROLES_CPMANDCSTL_QUERY")
MOPS_STUDYROLES_CPMANDCSTL_QUERY_df = MOPS_STUDYROLES_CPMANDCSTL_QUERY.get_dataframe()

AR_UDDL_STUDY_PRIORITY = dataiku.Dataset("AR_UDDL_STUDY_PRIORITY")
AR_UDDL_STUDY_PRIORITY_df = AR_UDDL_STUDY_PRIORITY.get_dataframe()

# AR_UDDL_STUDY_PRIORITY = dataiku.Dataset("study_goals_and_prioritization_idash_rms_temp")
# AR_UDDL_STUDY_PRIORITY_df = AR_UDDL_STUDY_PRIORITY.get_dataframe()

PLW_WI_STUDYCPM_QUERY = dataiku.Dataset("PLW_WI_STUDYCPM_QUERY")
PLW_WI_STUDYCPM_QUERY_df = PLW_WI_STUDYCPM_QUERY.get_dataframe()

STUDY_POC_QUERY = dataiku.Dataset("STUDY_POC_QUERY")
STUDY_POC_QUERY_df = STUDY_POC_QUERY.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Master List of Study Numbers

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Joining study listing and Study priority to get study priority column.
dist_STUDY_LIST_df = pd.merge(distinct_STUDY_LISTING_QUERY_df,
                              AR_UDDL_STUDY_PRIORITY_df[['Study_Number','Study_Priority']],
                              on='Study_Number',how='left')


# generating the date by n days from today.
N = 90

date_N_days = pd.to_datetime(datetime.now() + timedelta(days=N),utc=True)

# Function to generate logic used for study list whether or not include based on logic.
def gen_dist_list_study(DF,VALUES, _COL, _list):
    try:
        conditions=[(DF['study_status_plan']=='P-Proposed') &
                    (DF['Study_Priority'].isin(['Priority 2', 'Priority 3', 'Priority 1b', 'Priority 1a'])),
                    (DF['study_status_plan']=='P-Proposed') &
                    (DF['Study_Final_Approved_Protocol_Date']<= date_N_days),
                    (DF['study_status_plan'].isin(_list))]
        values = VALUES
        DF[_COL] = np.select(conditions, values)
        return DF[_COL]
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# Include everything except 'P-Proposed' in status column.
dist_status_lst = list(dist_STUDY_LIST_df['study_status_plan'].unique())
dist_status_lst = [s for s in dist_status_lst if s != 'P-Proposed']

dist_STUDY_LIST_df['inclusion'] = gen_dist_list_study(dist_STUDY_LIST_df,['include','include','include'],
                                                      'inclusion', dist_status_lst)
# creating distinct list of studies
DIST_STUDYLIST_DF = pd.DataFrame(dist_STUDY_LIST_df.Study_Number[dist_STUDY_LIST_df['inclusion']=='include'])
#                                  .unique())#.reset_index(drop=True)
DIST_STUDYLIST_DF = DIST_STUDYLIST_DF.drop_duplicates().reset_index(drop=True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### User Defined Functions

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to generate dict with Key column as separate column
def gen_dict_for_each_study(df,dict_col,study_col):
    try:
        unique_list = list(df[study_col].unique())
        Final_lst=[]
        for each_study in unique_list:
            final_dict={}
            F_DF = df[df[study_col]==each_study].iloc[:,1:].copy()
            F_DICT = [{k:v for k,v in m.items() if pd.notnull(v)} for m in F_DF.to_dict(orient='records')]
            final_dict["Study_Number"]=each_study
            final_dict[dict_col]=F_DICT
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# Function to generate logic used for DVSO recruitment
def gen_dvso_rec_logic(DF,PCNT_COL,COMPLET_COL,VALUES):
    try:
        conditions=[(DF[COMPLET_COL]==1),(DF[PCNT_COL]>=100)&(DF[COMPLET_COL]!=1),(DF[PCNT_COL]<100)&(DF[COMPLET_COL]!=1)]
        values = VALUES
        DF[COMPLET_COL] = np.select(conditions, values)
        return DF[COMPLET_COL]
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

def Get_Study_Milestones_Dict(df,lookupdf):
    try:
        study_list = list(df['Study_Number'])
        MilestoneName_list = list(lookupdf['MilestoneName'])
        date_list = list(lookupdf['date'])
        pcnt_list = list(lookupdf['pcnt'])
        baseline_list = list(lookupdf['baseline'])
        df['Study_Pcnt_Comp_SAP_dummy'] = np.nan
        df['Study_SAP_Baseline_dummy'] = np.nan
        df['Study_Final_CSR_Baseline_dummy'] = np.nan
        df['Study_Pcnt_Study_Completion'] = np.nan
        Final_lst=[]
        for each_study in study_list:
            study_mile_DF = df[df['Study_Number']==each_study].copy()
            final_dict={}
            for MilestoneName,date,pcnt,baseline in zip(MilestoneName_list,date_list,pcnt_list,baseline_list):
                mile_df = study_mile_DF[[date,pcnt,baseline]].rename(columns={date:'date',pcnt:'pcnt',baseline:'baseline'}).copy()
                _dict = mile_df.to_dict(orient='records')
                mile_dict={k:v for k,v in _dict[0].items() if pd.notnull(v) and v!='' and str(v)!='nan'}
                if mile_dict !={}:
                    final_dict["Study_Number"]=each_study
                    final_dict[MilestoneName]=mile_dict
                else:
                    final_dict["Study_Number"]=each_study
                    final_dict[MilestoneName]=np.nan
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# function to drop columns after generating study milestones dict
def drop_cols(df,lookupdf):
    try:
        MilestoneName_list = list(lookupdf['MilestoneName'])
        date_list = list(lookupdf['date'])
        pcnt_list = list(lookupdf['pcnt'])
        baseline_list = list(lookupdf['baseline'])
        df['Study_Pcnt_Comp_SAP_dummy'] = np.nan
        df['Study_SAP_Baseline_dummy'] = np.nan
        df['Study_Final_CSR_Baseline_dummy'] = np.nan
        df['Study_Pcnt_Study_Completion'] = np.nan
        lists_combined = MilestoneName_list+date_list+pcnt_list+baseline_list
        final_df=df.drop(lists_combined, axis = 1)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

def Get_Study_NextMilestone_Dict(df):
    try:
        study_list = list(df['Study_Number'])
        Final_lst=[]
        for each_study in study_list:
            final_dict={}
            study_nxt_mile_DF = df[df['Study_Number']==each_study].copy()
            NxtMile_df = study_nxt_mile_DF.rename(columns={'StudyNextMilestone':'nextmilestone',
                                                        'StudyNextMilestoneDate':'date'}).iloc[:,1:].copy()
            _dict = NxtMile_df.to_dict(orient='records')
            NxtMile_dict={k:v for k,v in _dict[0].items() if pd.notnull(v) and v!='' and str(v)!='nan'}
            if NxtMile_dict !={}:
                final_dict["Study_Number"]=each_study
                final_dict['DVSO_Study_NextMilestone']=NxtMile_dict
            else:
                final_dict["Study_Number"]=each_study
                final_dict['DVSO_Study_NextMilestone']=np.nan
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# Function to replace float or str datatype values in a dict or list with int data type values
class Decoder(json.JSONDecoder):
    def decode(self, s):
        result = super().decode(s)
        return self._decode(result)

    def _decode(self, o):
        if isinstance(o, str) or isinstance(o, float):
            try:
                return int(o)
            except ValueError:
                return o
        elif isinstance(o, dict):
            return {k: self._decode(v) for k, v in o.items()}
        elif isinstance(o, list):
            return [self._decode(v) for v in o]
        else:
            return o

# function to convert date columns to required format
def StrDateToISOFormat(DF, list_date_cols):
    try:
        for col in list_date_cols:
            DF[col] = pd.to_datetime(DF[col], utc=True)#.dt.strftime('%m-%d-%Y')
        return DF
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Study_Milestones_CRC

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# merge Study_Milestones_CRC on MOPS_CRC_WI_STUDIES to restrict CRCs matching studies returned from MOPS_CRC_WI_STUDIES

# mops_CRC_WI_STUDIES_QUERY_df = mops_CRC_WI_STUDIES_QUERY_df[['STUDY_NUMBER']].rename(columns={'STUDY_NUMBER':'Study_Number'})

Study_Milestones_CRC_DF = pd.merge(mops_CRC_WI_STUDIES_QUERY_df,study_MILESTONES_CRC_QUERY_df,on='Study_Number',how='left')

study_MILESTONES_MAIN_QUERY_df=study_MILESTONES_MAIN_QUERY_df.append(Study_Milestones_CRC_DF)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### MOPS Issues

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
##### Logic: if CATEGORY RLIKE '^COVID19' then IsContinuity := true => CONTINUITYDATA else  ISSUESDATA

# qms_MOPS_STUDY_ISSUES_QUERY_PREP_df = qms_MOPS_STUDY_ISSUES_QUERY_PREP_df.rename(columns={

# generating continuity data
CONTINUITY_F_DF = (qms_MOPS_STUDY_ISSUES_QUERY_PREP_df[qms_MOPS_STUDY_ISSUES_QUERY_PREP_df['Category']
                                                       .str.contains("COVID19", na=False)].copy()).applymap(prep_for_JSON)
CONTINUITYDATA_DF=gen_dict_for_each_study(CONTINUITY_F_DF,'CONTINUITYDATA','Study_Number')

# generating Issues data
ISSUE_F_DF=(qms_MOPS_STUDY_ISSUES_QUERY_PREP_df[~qms_MOPS_STUDY_ISSUES_QUERY_PREP_df['Category']
                                                       .str.contains("COVID19", na=False)].copy()).applymap(prep_for_JSON)
ISSUEDATA_DF=gen_dict_for_each_study(ISSUE_F_DF,'ISSUESDATA','Study_Number')

# merging both continuity and issues data
CONT_ISSUE_DF = pd.merge(CONTINUITYDATA_DF,ISSUEDATA_DF, on='Study_Number',how='outer')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    ContOverallStatus_DF = CONTINUITY_F_DF[['Study_Number','Impact']].rename(columns={'Impact':'ContOverallStatus'})
    conditions=[(ContOverallStatus_DF['ContOverallStatus']=='Gray'),(ContOverallStatus_DF['ContOverallStatus']=='Yellow'),
                (ContOverallStatus_DF['ContOverallStatus']=='Red')]
    values = [3,2,1]
    ContOverallStatus_DF['Rank'] = np.select(conditions, values)
except Exception as e:
        print('Unable to find', '->', e)
        pass

ContOverallStatus_DF = (ContOverallStatus_DF.sort_values(by='Rank').drop_duplicates(subset=['Study_Number'],keep='first')
                        .reset_index(drop=True).drop('Rank', axis = 1))

# ContOverallStatus_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### MOPS_STUDY_ATTRIBUTES_QUERY

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# qms_MOPS_STUDY_ATTRIBUTES_QUERY_df=qms_MOPS_STUDY_ATTRIBUTES_QUERY_df.rename(columns={'STUDY_NUMBER':'Study_Number'})

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Country Status

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
COUNTRY_STATUS_df = ar_UDDL_COUNTRY_STATUS_df.rename(columns={'count':'cnt','country':'name','study_number':'Study_Number'})

COUNTRY_STATUS_df = pd.merge(DIST_STUDYLIST_DF,COUNTRY_STATUS_df,on='Study_Number',how='inner')#.dropna()

Country_S_DF=gen_dict_for_each_study(COUNTRY_STATUS_df,'Country_Status','Study_Number')
Country_S_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### DVSO Recruitment

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
STUDY_COMP_df=STUDY_COMPLETION_QUERY_df.assign(SA_TOD_COMPLET=0, SA_TOT_COMPLET=0,SS_TOD_COMPLET=0, SS_TOT_COMPLET=0)
STUDY_COMP_df['SA_TOD_COMPLET'] = np.where(STUDY_COMP_df['HUNDRED_PCNT_COMP_HUNDRED_PCNT_SITES_ACTIVE']==100,1,0)
STUDY_COMP_df['SA_TOT_COMPLET'] = np.where(STUDY_COMP_df['HUNDRED_PCNT_COMP_HUNDRED_PCNT_SITES_ACTIVE']==100,1,0)
STUDY_COMP_df['SS_TOD_COMPLET'] = np.where(STUDY_COMP_df['LSFV_PCT_COMPLETE']==100,1,0)
STUDY_COMP_df['SS_TOT_COMPLET'] = np.where(STUDY_COMP_df['LSFV_PCT_COMPLETE']==100,1,0)

STUDY_COMP_df = STUDY_COMP_df[['Study_Number', 'SA_TOD_COMPLET', 'SA_TOT_COMPLET', 'SS_TOD_COMPLET', 'SS_TOT_COMPLET']]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# NON_DVSO_df=NON_DVSO_STUDIES_QUERY_df#.rename(columns={'STUDY_NUMBER':'Study_Number'})

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# DVSO_df = DVSO_RECRUITMENT_DATA_QUERY_df.rename(columns={'STUDY_NUMBER':'Study_Number'})

DVSO_df = pd.merge(DVSO_RECRUITMENT_DATA_QUERY_df,STUDY_COMP_df,on='Study_Number',how='left')

# DVSO_df=DVSO_df.fillna(0).apply(pd.to_numeric, downcast='signed', errors='ignore')

DVSO_df['SA_TOD_COMPLET'] = gen_dvso_rec_logic(DVSO_df,'SA_TOD_PCNT','SA_TOD_COMPLET',[1,2,-1])
DVSO_df['SR_TOD_COMPLET'] = gen_dvso_rec_logic(DVSO_df,'SR_TOD_PCNT','SR_TOD_COMPLET',[1,2,-1])
DVSO_df['SS_TOD_COMPLET'] = gen_dvso_rec_logic(DVSO_df,'SS_TOD_PCNT','SS_TOD_COMPLET',[1,2,-1])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# cretaing this dataset for calculations of SR_ACT_RAND_PCNT column

SR_ACT_RAND_PCNT_df = functools.reduce(lambda x,y: pd.merge(x,y, on='Study_Number', how='outer'),
                                       [NON_DVSO_STUDIES_QUERY_df,
                                        study_MILESTONES_MAIN_QUERY_df[['Study_Number','Study_Planned_No_Subjects']]])

SR_ACT_RAND_PCNT_df['SR_ACT_RAND_PCNT'] = np.where((SR_ACT_RAND_PCNT_df['SR_ACT_RAND_CNT'].notna()) &
                                                   (SR_ACT_RAND_PCNT_df['Study_Planned_No_Subjects'].notna()) &
                                                   (SR_ACT_RAND_PCNT_df['Study_Planned_No_Subjects']!=0.0),
                                                   round((SR_ACT_RAND_PCNT_df.SR_ACT_RAND_CNT/SR_ACT_RAND_PCNT_df.Study_Planned_No_Subjects)*100),
                                                   np.nan)
SR_ACT_RAND_PCNT_df['SR_ACT_RAND_PCNT'] = np.where((SR_ACT_RAND_PCNT_df['SR_ACT_RAND_CNT'].notna()) &
                                                   (SR_ACT_RAND_PCNT_df['Study_Planned_No_Subjects']==0.0),
                                                   -1,
                                                   (SR_ACT_RAND_PCNT_df['SR_ACT_RAND_PCNT']))
SR_ACT_RAND_PCNT_df = SR_ACT_RAND_PCNT_df[['Study_Number','SR_ACT_RAND_PCNT']]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# need to confirm with fernando about the join
DVSO_final_df = (functools.reduce(lambda x,y: pd.merge(x,y, on='Study_Number', how='outer'),[DVSO_df,
                                                                                             NON_DVSO_STUDIES_QUERY_df,
                                                                                             SR_ACT_RAND_PCNT_df]))
# DVSO_final_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Last Month Study Traffic status

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    LM_TRAF_STAT_DF = (ar_UDDL_TRAFFIC_LIGHTS_PREP_df[['study_number','lastmo_study_traffic_light','study_goal_category',
                                                       'gb']].rename(columns={'study_number':'Study_Number',
                                                                    'lastmo_study_traffic_light':'LastMo_STUDY_TRAFFIC_LIGHT',
                                                                    'study_goal_category':'STUDY_GOAL_CATEGORY',
                                                                    'gb':'GB'}))
except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### DVSO Study Next Milestone Date

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Changed the data source from DVSO to CV_STUDY_TASKS_DATA

STUDY_NEXT_MILESTONE_DATE_df = Get_Study_NextMilestone_Dict(STUDY_NEXT_MILESTONE_DATE_df.applymap(prep_for_JSON))#.head()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### DVSO RECRUITMENT CURVE YN

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# DVSO_RECRUITMENT_CURVE_YN_df = DVSO_RECRUITMENT_CURVE_YN_df.rename(columns={'STUDY_NUMBER':'Study_Number'})
# DVSO_RECRUITMENT_CURVE_YN_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### DVSO Rand Next Milestone

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to generate logic used for DVSO recruitment (DVSO Rand Next Milestone)
def gen_PLWMilestone(DF,MILES_COL,VALUES):
    try:
        conditions=[(DF[MILES_COL]=='25 % randomized'),(DF[MILES_COL]=='50 % randomized'),
                    (DF[MILES_COL]=='75 % randomized'),(DF[MILES_COL]=='100 % randomized')]
        values = VALUES
        NEW_COL = np.select(conditions, values)
        return NEW_COL
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# function to calculate date diff with condition (DVSO Rand Next Milestone)
def delta(mile_col,dvso_proj,twentyfive,fifty,seventy,hundred):
    try:
        if mile_col=='25 % randomized':
            date_diff = (dvso_proj-twentyfive)
        elif mile_col=='50 % randomized':
            date_diff = (dvso_proj-fifty)
        elif mile_col=='75 % randomized':
            date_diff = (dvso_proj-seventy)
        elif mile_col=='100 % randomized':
            date_diff = (dvso_proj-hundred)
        return date_diff/(np.timedelta64(1, 'D'))
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# function to generate dict with study number as key column (DVSO Rand Next Milestone)
def Get_Dict(df,col_name,key_col):
    try:
        study_list = list(df[key_col])
        Final_lst=[]
        for each_study in study_list:
            final_dict={}
            study_nxt_mile_DF = df[df[key_col]==each_study].copy()
            NxtMile_df = study_nxt_mile_DF.iloc[:,1:].copy()
            _dict = NxtMile_df.to_dict(orient='records')
            NxtMile_dict={k:v for k,v in _dict[0].items() if pd.notnull(v) and v!='' and str(v)!='nan'}
            if NxtMile_dict !={}:
                final_dict['Study_Number']=each_study
                final_dict[col_name]=NxtMile_dict
            else:
                final_dict['Study_Number']=each_study
                final_dict[col_name]=np.nan
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# shorten the dataframe name
# DVSO_RND_NXMILE_DF = DVSO_RAND_NEXT_MILESTONE_PLW_JOINED_df.copy()
DVSO_RND_NXMILE_DF= StrDateToISOFormat(DVSO_RAND_NEXT_MILESTONE_PLW_JOINED_df,['DVSO_RANDOMIZED_PROJECTION'])
# DVSO_RND_NXMILE_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
values = ['Study_TwentyFive_Pcnt_Subjects_Randomized_Date','Study_Fifty_Pcnt_Enrollment_Date',
          'Study_SeventyFive_Pcnt_Enrollment_Date','Study_LSFV_Date']
DVSO_RND_NXMILE_DF['PLWMilestone'] = gen_PLWMilestone(DVSO_RND_NXMILE_DF,'NEXT_PLANNED_MILESTONE',values)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
DVSO_RND_NXMILE_DF['delta']=DVSO_RND_NXMILE_DF.apply(lambda x:delta(x['NEXT_PLANNED_MILESTONE'],
                                                                    x['DVSO_RANDOMIZED_PROJECTION'],
                                                                    x['TwentyFiveRand'],
                                                                    x['FiftyRand'],
                                                                    x['SeventyFiveRand'],
                                                                    x['HundredRand']), axis=1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    DVSO_RND_NXMILE_DF=DVSO_RND_NXMILE_DF.rename(columns={'NEXT_PLANNED_MILESTONE':'milestone',
                                                         'DVSO_RANDOMIZED_PROJECTION':'date',
                                                         }).applymap(prep_for_JSON)
except Exception as e:
        print('Unable to rename', '->', str(e))
        pass
# removing delta column for time being
DVSO_RND_NXMILE_DF = Get_Dict(DVSO_RND_NXMILE_DF[['Study_Number','milestone','date','PLWMilestone','delta']],
                              'DVSO_Rand_Next_Milestone','Study_Number')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Study_CPM and Study_CSTL

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    # removes ';" from end of the string
    MOPS_STUDYROLES_CPMANDCSTL_QUERY_df['Study_CPM'] = MOPS_STUDYROLES_CPMANDCSTL_QUERY_df['Study_CPM'].str.rstrip(";")
    MOPS_STUDYROLES_CPMANDCSTL_QUERY_df['Study_CSTL'] = MOPS_STUDYROLES_CPMANDCSTL_QUERY_df['Study_CSTL'].str.rstrip(";")

    PLW_WI_STUDYCPM_merge_df = pd.merge(mops_CRC_WI_STUDIES_QUERY_df,PLW_WI_STUDYCPM_QUERY_df,on='Study_Number',how='left').drop('CRC_STUDY_YN',axis=1).dropna()

    # reorders name to last name , First Name
    PLW_WI_STUDYCPM_merge_df['Study_CPM_First'] = PLW_WI_STUDYCPM_merge_df['Study_CPM'].str.extract(r'^(?P<First>\S+)')
    PLW_WI_STUDYCPM_merge_df['Study_CPM_Last'] = PLW_WI_STUDYCPM_merge_df['Study_CPM'].str.extract(r'^.*?(?P<Last>\S+)?$')
    PLW_WI_STUDYCPM_merge_df['Study_CPM'] = PLW_WI_STUDYCPM_merge_df['Study_CPM_Last'] + ',' + PLW_WI_STUDYCPM_merge_df['Study_CPM_First']
    PLW_WI_STUDYCPM_merge_df = PLW_WI_STUDYCPM_merge_df.drop(['Study_CPM_First','Study_CPM_Last'],axis=1).replace('Rue,Sherrie', 'La Rue,Sherrie')

    MOPS_STUDYROLES_CPMANDCSTL_QUERY_df = PLW_WI_STUDYCPM_merge_df.append(MOPS_STUDYROLES_CPMANDCSTL_QUERY_df)
except Exception as e:
    print('Unable to find', '->', str(e))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Study Priority

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# try:
#     AR_UDDL_STUDY_PRIORITY_df=AR_UDDL_STUDY_PRIORITY_df.rename(columns={'study_number':'Study_Number',
#                                                                         'study_priority':'Study_Priority',
#                                                                        'recruit_priority':'Recruit_Priority'})
# except Exception as e:
#         print('Unable to rename', '->', str(e))
#         pass
AR_UDDL_STUDY_PRIORITY_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Merge all datasets to Unique Study Listing

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
DashType_STUDY_DF = distinct_STUDY_LISTING_QUERY_df[['Study_Number','DashType']]
List_DF = [DIST_STUDYLIST_DF,study_MILESTONES_MAIN_QUERY_df,qms_MOPS_STUDY_ATTRIBUTES_QUERY_df,DVSO_final_df,CONT_ISSUE_DF,
  DashType_STUDY_DF,LM_TRAF_STAT_DF,STUDY_NEXT_MILESTONE_DATE_df,DVSO_RECRUITMENT_CURVE_YN_df, DVSO_RND_NXMILE_DF,
   ContOverallStatus_DF,STUDY_ENROLLED_PSI_YN_QUERY_df, MOPS_STUDYROLES_CPMANDCSTL_QUERY_df, STUDY_POC_QUERY_df,
 AR_UDDL_STUDY_PRIORITY_df,Country_S_DF]

empty_df_index  = next((i for i, df in enumerate(List_DF) if not isinstance(df, pd.DataFrame) or df.empty), None)
empty_df_index

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:

    study_final_df = functools.reduce(lambda x,y: pd.merge(x,y, on='Study_Number', how='left'), [
                                                                                                DIST_STUDYLIST_DF,
                                                                                                study_MILESTONES_MAIN_QUERY_df,
                                                                                                qms_MOPS_STUDY_ATTRIBUTES_QUERY_df,
                                                                                                DVSO_final_df,
                                                                                                CONT_ISSUE_DF,
                                                                                                DashType_STUDY_DF,
                                                                                                LM_TRAF_STAT_DF,
                                                                                                STUDY_NEXT_MILESTONE_DATE_df,
                                                                                                DVSO_RECRUITMENT_CURVE_YN_df,
                                                                                                DVSO_RND_NXMILE_DF,
                                                                                                ContOverallStatus_DF,
                                                                                                STUDY_ENROLLED_PSI_YN_QUERY_df,
                                                                                                MOPS_STUDYROLES_CPMANDCSTL_QUERY_df,
                                                                                                STUDY_POC_QUERY_df,
                                                                                                AR_UDDL_STUDY_PRIORITY_df,
                                                                                                Country_S_DF
                                                                                                ]).applymap(prep_for_JSON)
    study_final_df=study_final_df.sort_values(by='Study_Number').reset_index(drop=True)
except Exception as e:
        print('Unable to left Join', '->', str(e))
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
study_miles_df = Get_Study_Milestones_Dict(study_final_df,StudyMilestoneHashTable_df)
dropped_cols_df = drop_cols(study_final_df,StudyMilestoneHashTable_df)

StudyDataIntegrated_df = pd.merge(dropped_cols_df,study_miles_df,on='Study_Number',how='left')

#######################################################################################################################

try:
    StudyDataIntegrated_NWI_df = StudyDataIntegrated_df[~StudyDataIntegrated_df['Study_Number'].str.contains("WI")]
    f_StudyDataIntegrated_df = StudyDataIntegrated_df[StudyDataIntegrated_df['Study_Number'].str.contains("WI")].drop('CRC_STUDY_YN',axis=1)
    StudyDataIntegrated_WI_df = pd.merge(mops_CRC_WI_STUDIES_QUERY_df,f_StudyDataIntegrated_df,
                                         on='Study_Number',how='left')
    StudyDataIntegrated_df = StudyDataIntegrated_NWI_df.append(StudyDataIntegrated_WI_df).reset_index(drop=True)
except Exception as e:
        print('Unable to find', '->', str(e))

#######################################################################################################################

# fill in place for study_poc col with Study_cpm jsut for wi studies
def fill_study_poc(df):
    m1 = df['Study_Number'].str.contains('WI')
    df.loc[m1,'Study_POC'] = df.loc[m1,'Study_POC'].fillna(df.loc[m1,'Study_CPM'])
    return df

StudyDataIntegrated_df = fill_study_poc(StudyDataIntegrated_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Output

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#convert df to json,dropping NA elements
try:
    StudyDataIntegrated_json = json.dumps([row.dropna().to_dict() for index,row in StudyDataIntegrated_df.iterrows()],
                                          indent=1,ensure_ascii=False,default=str)
except Exception as e:
        print('Unable to find', '->', str(e))

#######################################################################################################################

# converting all the float values to int
try:
    StudyDataIntegrated_json=json.loads(StudyDataIntegrated_json, cls=Decoder)
    StudyDataIntegrated_json=json.dumps(StudyDataIntegrated_json,indent=1,ensure_ascii=False,default=str)
except Exception as e:
        print('Unable to find', '->', str(e))

#######################################################################################################################
DSS_ENV_URL = dataiku.get_custom_variables()["DSS_ENV_URL"]
source = DSS_ENV_URL + '/projects/GBL_DAI_IDASH/recipes/compute_STUDY_INTEGRATED_MAIN_OUTPUT/'
filenameMAIN = 'StudyDataIntegrated_DSS.txt'

#tidy json and add metadata wrapper
StudyDataIntegrated_json = StudyDataIntegrated_json.replace('\n','\r\n')
#NOTE: original JSON for this component did not have Metadata tag
StudyDataIntegrated_json = '{"Metadata" :{"run_date": "'+date.today().strftime('%d-%b-%Y')+'","run_date_time":"'+datetime.now().strftime('%m/%d/%Y %H:%M:%S')+'", "Source": "' + source + '"},"Study":'+ StudyDataIntegrated_json +'}'

#######################################################################################################################

# writing json file to S3 folder
write_to_folder(StudyDataIntegrated_json,['STUDYINTEGRATED_S3_FOLDER'],filenameMAIN)

#######################################################################################################################

study_INTEGRATED_MAIN_OUTPUT_df = StudyDataIntegrated_df


# Write recipe outputs
study_INTEGRATED_MAIN_OUTPUT = dataiku.Dataset("STUDY_INTEGRATED_MAIN_OUTPUT")
study_INTEGRATED_MAIN_OUTPUT.write_with_schema(study_INTEGRATED_MAIN_OUTPUT_df)
