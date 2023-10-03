# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import sys
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime, timedelta
import functools
import json
#Project Libs
from spfunctions import push_JSON_to_sharepoint , read_Lists_from_SP_to_DSS
from IDASHFunctions import write_to_folder, jsonify
#from spfunctions import read_Lists_from_SP_to_DSS
from jsonfunctions import prep_for_JSON #, xplode_json
#from jsonfunctions import xplode_json
pd.set_option("display.expand_frame_repr", True)

# Read recipe inputs

# Main Portfolio Data
candidate_Main_Raw_prepared = dataiku.Dataset("Candidate_Main_Raw")
candidate_Main_Raw_prepared_df = candidate_Main_Raw_prepared.get_dataframe()

# list of Candidate code only Data
Candidate_Code = dataiku.Dataset("Candidate_Code")
Candidate_Code_df = Candidate_Code.get_dataframe()

# Main Milestones Data
milestones_Main_Raw_prepared = dataiku.Dataset("Milestones_Main_Raw")
milestones_Main_Raw_prepared_df = milestones_Main_Raw_prepared.get_dataframe()

# Next Milestones Data
NEXT_MILESTONES_DATA = dataiku.Dataset("NEXT_MILESTONES_DATA")
NEXT_MILESTONES_DATA_DF = NEXT_MILESTONES_DATA.get_dataframe()

# CANDIDATE_FLAG_FINAL Data
CANDIDATE_FLAG_FINAL = dataiku.Dataset("CANDIDATE_FLAG_FINAL")
CANDIDATE_FLAG_FINAL_DF = CANDIDATE_FLAG_FINAL.get_dataframe()

# Study_CANDIDATE_FLAG_FINAL Data
STUDY_CANDIDATES_FINAL = dataiku.Dataset("STUDY_CANDIDATES_FINAL")
STUDY_CANDIDATES_FINAL_DF = STUDY_CANDIDATES_FINAL.get_dataframe()

# HBU_Projects Data
HBU_PROJECTS_FINAL = dataiku.Dataset("HBU_PROJECTS_DATA")
HBU_PROJECTS_DATA_DF = HBU_PROJECTS_FINAL.get_dataframe()

# only code and Medicine lead name from Snapshot_Candidate_Team_Roster data
Snapshot_Candidate_Team_Roster = dataiku.Dataset("Snapshot_Candidate_Team_Roster")
Medicine_Lead_DF = Snapshot_Candidate_Team_Roster.get_dataframe()


# lookup table for China and Japan aprroval and submission data
JP_CH_LOOKUP_DF = dataiku.Dataset("OperationalDevelopmentPriorities")
JP_CH_LOOKUP_DF = JP_CH_LOOKUP_DF.get_dataframe()

# Table for DashType data
DASHTYPE_QUERY = dataiku.Dataset("DASHTYPE_QUERY")
DASHTYPE_QUERY_DF = DASHTYPE_QUERY.get_dataframe()

# Data added 04/25/2022
CONTINIUITY_DATA_QUERY = dataiku.Dataset("CONTINIUITY_DATA_QUERY")
CONTINIUITY_DATA_QUERY_df = CONTINIUITY_DATA_QUERY.get_dataframe()

# Data added 05/16/2022
PDA_BREATHROUGH_FLAG_Y = dataiku.Dataset("PDA_BREATHROUGH_FLAG_Y")
PDA_BREATHROUGH_FLAG_Y_df = PDA_BREATHROUGH_FLAG_Y.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Json Build and export

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#prep_for_JSON is a library function that brute force formats all date types to YYYY/MM/DD strings
candidate_main_prepped_df = candidate_Main_Raw_prepared_df.applymap(prep_for_JSON)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
candidate_main_prepped_df['Project_Manager'] = candidate_main_prepped_df['Project_Manager'].str.replace(', ',',').str.replace(',',', ')
candidate_main_prepped_df['Project_Planner'] = candidate_main_prepped_df['Project_Planner'].str.replace(', ',',').str.replace(',',', ')
candidate_main_prepped_df['Exec_Review_By'] = candidate_main_prepped_df['Exec_Review_By'].str.replace(', ',',').str.replace(',',', ')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Preparing milestones data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# prep_for_JSON is a library function that brute force formats all date types to YYYY/MM/DD strings
milestones_main_prepped_df = milestones_Main_Raw_prepared_df.applymap(prep_for_JSON)

# sort milestones data by Code column
milestones_sorted_df = milestones_main_prepped_df.sort_values(by=["Code"])

# Rename column names
milestones_sorted_df = milestones_sorted_df.rename(columns={'Candidate_Milestone_Display_Name':'name','Candidate_Milestone_Name':'Milestone','Candidate_Task_Core_Code':'core','Candidate_Task_Duplicate_Milestone_Descriptor':'desc','Candidate_Task_End_Date':'date','Candidate_Task_Pcnt_Comp':'pcnt','Candidate_Task_Current_GEM':'gem'})

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to generate dict with Key column as separate column
def gen_dict_for_each_id(df,dict_col,key_col):
    try:
        unique_list = list(df[key_col].unique())
        Final_lst=[]
        for each_id in unique_list:
            final_dict={}
            F_DF = df[df[key_col]==each_id].iloc[:,1:].copy()
            F_DICT = [{k:v for k,v in m.items() if pd.notnull(v)} for m in F_DF.to_dict(orient='records')]
            final_dict[key_col]=each_id
            final_dict[dict_col]=F_DICT
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        return 'Unable to find', '->', str(e)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    F_df = milestones_sorted_df.drop([ 'Candidate_Task_Core_Code_Short_Desc'], axis=1)
    code_lst = list(F_df['Code'].unique())
    final=[]
    for code in code_lst:
        mil_df = F_df[F_df['Code']==code].sort_values(['date'], ascending=True)
        # renaming the milestone name with suffix
        s = mil_df.groupby(['Code', 'Milestone']).cumcount()
        mil_df['Milestone'] = (mil_df.Milestone + s[s>0].astype(str)).fillna(mil_df.Milestone)

        mil_lst = mil_df['Milestone'].tolist()
        for mil in mil_lst:
            final_dict={}
            df = mil_df[mil_df['Milestone']==mil].drop([ 'Milestone'], axis=1).sort_values(['date'], ascending=True)
            df2dict=df.set_index('Code').transpose().to_dict(orient='dict')
            dicttolist=[[k,{k1:v1 for k1,v1 in v.items() if v1 is not None and str(v1)!='nan'}] for k,v in df2dict.items()]
            df2=pd.DataFrame(dicttolist,columns=['Code', mil])
            final.append(df2)
    final_df = pd.concat(final)
    final_df = final_df.groupby('Code', as_index=False).first()
except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    code_lst = list(milestones_sorted_df['Code'].unique())
    final_milestones_lst=[]
    for code in code_lst:
        final_dict={}

        df = milestones_sorted_df[milestones_sorted_df['Code']==code].sort_values(['date'], ascending=False)

        # renaming the milestone name with suffix
        s = df.groupby(['Code', 'Milestone']).cumcount()
        df['Milestone'] = (df.Milestone + s[s>0].astype(str)).fillna(df.Milestone)
        df = df.loc[:, df.columns != 'Code'].sort_values(['date'])
        df = df.loc[:, df.columns != 'Candidate_Task_Core_Code_Short_Desc']

        # creating dict2
        df = df.reset_index()
        dict2 = df['Milestone'].to_list()

        final_dict['Code'] = code
        final_dict['Milestones'] = dict2

        final_milestones_lst.append(final_dict)
    milestone_df = pd.DataFrame(final_milestones_lst)
    candidate_milestones_join_df = functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'), [candidate_main_prepped_df,final_df, milestone_df])
except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Genarating Next Milestone and Next approval data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def next_milestone(df):
    try:
        col_list = ['Code', 'Candidate_Task_Core_Code_Short_Desc',
                    'Candidate_Task_Duplicate_Milestone_Descriptor', 'MS_Date']
        curr_dt = pd.to_datetime("now", utc=True)
        ms_df = df[col_list]
        ms_df = ms_df.dropna(subset=['MS_Date', 'Candidate_Task_Core_Code_Short_Desc'])
        ms_df = ms_df.fillna('')
        ms_df['Next_MS'] = (ms_df['Candidate_Task_Core_Code_Short_Desc']
                            + ' ' +  ms_df['Candidate_Task_Duplicate_Milestone_Descriptor'])
        ms_df = ms_df[ms_df['MS_Date'] > curr_dt]
        #ms_df = ms_df.set_index('Next_MS')
        ms_df = ms_df.groupby(['Next_MS','Code'])['MS_Date'].min() #'Next_MS',
        ms_df = ms_df.reset_index()
        ms_df = ms_df.sort_values(by=['Code','MS_Date'])
        ms_df = ms_df.drop_duplicates(subset=['Code'])
        ms_df = ms_df.reset_index(drop=True)
        ms_df = ms_df.rename(columns={"MS_Date":"Next_MS_Date"})
        return ms_df
    except Exception as e:
        print('Unable to find', '->', e)
        pass


###########################################################################################################################


def next_approval(df):
    try:
        list_task_codes = ['9200','9220','8100','9210']
        cols_list = ['Code','Candidate_Task_Core_Code_Short_Desc',
                     'Candidate_Task_Core_Code' , 'MS_Date']
        curr_dt_sub30 = pd.to_datetime("now", utc=True) - timedelta(days=30)
        ms_df = df[cols_list]
        ms_df = ms_df[ms_df['Candidate_Task_Core_Code'].isin(list_task_codes)]
        ms_df = ms_df[ms_df['MS_Date'] > (curr_dt_sub30)]
        ms_df = ms_df.groupby(['Candidate_Task_Core_Code','Code',
                               'Candidate_Task_Core_Code_Short_Desc'])['MS_Date'].min()
        ms_df = ms_df.reset_index()
        ms_df = ms_df.sort_values(by=['Code','MS_Date'])
        ms_df = ms_df.drop_duplicates(subset=['Code'])
        ms_df = ms_df.reset_index(drop=True)
        ms_df['Candidate_Task_Core_Code_Short_Desc'] = ms_df.Candidate_Task_Core_Code_Short_Desc.str.split().str.get(0)
        ms_df = ms_df.rename(columns={"Candidate_Task_Core_Code_Short_Desc": "Next_Approval","MS_Date":"Next_Approval_Date"})
        ms_df = ms_df[['Code','Next_Approval','Next_Approval_Date']]
        return ms_df
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Creating Next Milestone and Next Approval DFs and Finnaly merging with Main DF
NEXT_MILES_DF = next_milestone(NEXT_MILESTONES_DATA_DF)


###########################################################################################################################


NEXT_APPR_DF = next_approval(NEXT_MILESTONES_DATA_DF)


###########################################################################################################################


can_miles_nxma_join_df = (functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'),
                                           [candidate_milestones_join_df, NEXT_MILES_DF, NEXT_APPR_DF]))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### GPD Y or N Flag data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def SubsetOfDF(df, condition, col_name):
    try:
        if condition == 'OnlySubset':
            df = df[['Code']].copy()
            df = df.drop_duplicates(subset=['Code']).copy()
            return df
        elif condition == 'SubsetWithYNFlag':
            df = df[['Code']].copy()
            df[col_name] = 'Y'
            df.reset_index(drop=True)
            df = df.drop_duplicates(subset=['Code']).copy()
            return df
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Adding columns study_YN and Milestn_YN
Milestone_Code_Raw_df = SubsetOfDF(milestones_Main_Raw_prepared_df, 'SubsetWithYNFlag', 'Milestn_YN')

STUDY_CANDIDATES_Code_DF = STUDY_CANDIDATES_FINAL_DF.rename(columns={"candidate_code": "Code"})
STUDY_CANDIDATES_Code_DF = SubsetOfDF(STUDY_CANDIDATES_Code_DF, 'SubsetWithYNFlag', 'study_YN')

# Mergeing all DFs
final_merge_df = (functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'),
                    [Candidate_Code_df, CANDIDATE_FLAG_FINAL_DF, STUDY_CANDIDATES_Code_DF, Milestone_Code_Raw_df])
                  .drop_duplicates(subset=['Code']).reset_index(drop=True))


###########################################################################################################################


# Creating GPD_Display column using where condition
conditions = [ (final_merge_df['Can_YN']== 'Y') & (final_merge_df['Milestn_YN']== 'Y'),
             (final_merge_df['Can_YN']== 'Y'),
             (final_merge_df['study_YN']== 'Y')]
values = ['Y', 'N', 'N']

final_merge_df['GPD_Display'] = np.select(conditions, values)

idx = np.where((final_merge_df['GPD_Display']=='Y') | (final_merge_df['GPD_Display']=='N'))
final_merge_df = final_merge_df.loc[idx].sort_values(by=['Code'])


###########################################################################################################################


# merging the GPD_Display column to the main DF.
CAN_MI_NXMA_GPD_DF = functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='inner'),
                                      [can_miles_nxma_join_df,final_merge_df[['Code','GPD_Display']]])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### ADD Hospital (HBU) tags to HBU projects

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
CAN_MI_NXMA_GPD_HBU_DF = (functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'),
                                           [CAN_MI_NXMA_GPD_DF, HBU_PROJECTS_DATA_DF]))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Add in Mecdicine Lead name

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# reformatting the first and last names
Medicine_Lead_DF['Medicine_Lead'] = Medicine_Lead_DF['Medicine_Lead'].str.replace(', ',',').str.replace(',',', ')
Medicine_Lead_DF['Global_Clinical_Lead'] = Medicine_Lead_DF['Global_Clinical_Lead'].str.replace(', ',',').str.replace(',',', ')
Medicine_Lead_DF['Global_Clinical_Lead'] = Medicine_Lead_DF['Global_Clinical_Lead'].str.replace("'",'').str.replace('.','')


###########################################################################################################################


# Medicine_Lead_DF (this also adds duplicates as there are multiple medicine leads to a project)
CAN_MI_NXMA_GPD_HBU_MLN_DF = (functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'),
                                              [CAN_MI_NXMA_GPD_HBU_DF,
                                               Medicine_Lead_DF[['Code','Medicine_Lead', 'Global_Clinical_Lead']]]))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Add Dev Japan and Dev China

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def add_dev_japan_china(df, Lookup_df, condition, region, col_name):
    try:
        df = df[df['milestone'] == condition]
        join_df = functools.reduce(lambda x,y: pd.merge(x,y, left_on=region, right_on='Code', how='left'),
                                   [Lookup_df[['Code_LP', region]], df])
        join_df = join_df[join_df['milestone'].notnull()].sort_values(by=["Code_LP"])
        Final_lst=[]
        uniq_proj_code = list(join_df["Code_LP"].unique())
        dict_list = [ {k:v for k,v in m.items() if pd.notnull(v)} for m in join_df[['date', 'pcnt', 'gem']].to_dict(orient='records')]
        for each_code, each_dict in zip(uniq_proj_code,dict_list):
            final_dict= {}
            final_dict['Code'] = each_code
            final_dict[col_name] = each_dict
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', e)
        pass

JP_CH_LOOKUP_DF = JP_CH_LOOKUP_DF.rename(columns={"Code": "Code_LP"})
milestones_sorted_df = milestones_sorted_df.rename(columns={"Candidate_Task_Core_Code_Short_Desc": "milestone"})

Submit_JNDA_df = add_dev_japan_china(milestones_sorted_df, JP_CH_LOOKUP_DF, 'Submit JNDA', 'Japan','ep_JNDA_Submission_Date')
Submit_CNDA_df = add_dev_japan_china(milestones_sorted_df, JP_CH_LOOKUP_DF, 'ChinaSubmission', 'China','ep_CNDA_Submission_Date')
Appr_JNDA_df = add_dev_japan_china(milestones_sorted_df, JP_CH_LOOKUP_DF, 'JNDA Approval', 'Japan','ep_JNDA_Approval_Date')
Appr_CNDA_df = add_dev_japan_china(milestones_sorted_df, JP_CH_LOOKUP_DF, 'China Approval', 'China','ep_CNDA_Approval_Date')

CAN_MI_NXMA_GPD_HBU_MLN_JC_DF = (functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'),
                                [CAN_MI_NXMA_GPD_HBU_MLN_DF,Submit_JNDA_df, Appr_JNDA_df,Submit_CNDA_df, Appr_CNDA_df])
                                 .sort_values(by=['Code']).reset_index(drop=True))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Continuity Data - changed the data source from sharepoint to CV_CANDIDATE_BCP Table - changes made - 3/21/2022 in Dev. Currently waiting for goahead for Prod untill then the below code is commented

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Genarating Continuity Data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to generate dict with Key column as separate column
def gen_dict_for_each_code(df,dict_col,study_col):
    try:
        unique_list = list(df[study_col].unique())
        Final_lst=[]
        for each_study in unique_list:
            final_dict={}
            F_DF = df[df[study_col]==each_study].iloc[:,1:].copy()
            F_DICT = [{k:v for k,v in m.items() if pd.notnull(v)} for m in F_DF.to_dict(orient='records')]
            final_dict["Code"]=each_study
            final_dict[dict_col]=F_DICT
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

CONTINIUITY_DATA_QUERY_df = CONTINIUITY_DATA_QUERY_df.applymap(prep_for_JSON)
Dict_CONTINIUITY_DATA_QUERY_df = gen_dict_for_each_code(CONTINIUITY_DATA_QUERY_df,"Continuity_Data","Code")

# made changes by avinash here on 01.23.2023 as the continuity data was coming in empty 
if not Dict_CONTINIUITY_DATA_QUERY_df.empty:
    CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_DF = CAN_MI_NXMA_GPD_HBU_MLN_JC_DF.merge(Dict_CONTINIUITY_DATA_QUERY_df,on="Code",how="left")#.iat[3,1]
else:
    Dict_CONTINIUITY_DATA_QUERY_df =pd.DataFrame()
    Dict_CONTINIUITY_DATA_QUERY_df['Code']=''
    Dict_CONTINIUITY_DATA_QUERY_df['Continuity_Data']=''
    CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_DF = CAN_MI_NXMA_GPD_HBU_MLN_JC_DF.merge(Dict_CONTINIUITY_DATA_QUERY_df,on="Code",how="left")#.iat[3,1]
# made changes by avinash here on 01.23.2023 as the continuity data was coming in empty

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_DF[CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_DF['Continuity_Data'].isnull()]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Merging DashType Data with candidate_milestones merged data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# merging candidate+milestones to continuity data
DASHTYPE_QUERY_DF = DASHTYPE_QUERY_DF.sort_values(by=["Code"])
CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_Dash_DF = pd.merge(CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_DF,DASHTYPE_QUERY_DF,on="Code",how="left")
CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_Dash_DF = CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_Dash_DF.applymap(prep_for_JSON)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Adding Breathrough Field

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_Dash_BR_DF = pd.merge(CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_Dash_DF,PDA_BREATHROUGH_FLAG_Y_df,
                                                  on="Code",how="left")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Creating json from merged candidate_milestones_continuity dataframe

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
portfoliodata_json = jsonify(CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_Dash_BR_DF,'compute_Porfolio_main_out','Candidate')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# #### Pushing json to SharePoint Documents

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# push json to S3 folder and SharePoint online folder
filenameMAIN = 'PortfolioDataIntegrated_DSS.txt'
folders = ['PORTFOLIOINTEGRATED_S3_FOLDER']
write_to_folder(portfoliodata_json,folders,filenameMAIN)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Compute recipe outputs from inputs
porfolio_main_out_df = CAN_MI_NXMA_GPD_HBU_MLN_JC_CON_Dash_BR_DF

# Write recipe outputs
porfolio_main_out = dataiku.Dataset("Porfolio_main_out")
porfolio_main_out.write_with_schema(porfolio_main_out_df)
