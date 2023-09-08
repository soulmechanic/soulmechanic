# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import functools
import datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime

# Read recipe inputs
candidate_MAIN_RAW_QUERY = dataiku.Dataset("CANDIDATE_MAIN_RAW_QUERY")
candidate_MAIN_RAW_QUERY_df = candidate_MAIN_RAW_QUERY.get_dataframe()

milestones_MAIN_RAW_QUERY = dataiku.Dataset("MILESTONES_MAIN_RAW_QUERY")
milestones_MAIN_RAW_QUERY_df = milestones_MAIN_RAW_QUERY.get_dataframe()

# only code and Medicine lead name from Snapshot_Candidate_Team_Roster data
SNAPSHOT_CANDIDATE_TEAM_ROSTER = dataiku.Dataset("SNAPSHOT_CANDIDATE_TEAM_ROSTER")
Medicine_Lead_DF = SNAPSHOT_CANDIDATE_TEAM_ROSTER.get_dataframe()

# lookup table for China and Japan aprroval and submission data
JP_CH_LOOKUP_DF = dataiku.Dataset("OperationalDevelopmentPriorities")
JP_CH_LOOKUP_DF = JP_CH_LOOKUP_DF.get_dataframe()

# Table for Compound data for PDA
COMPOUND_PDA_QUERY = dataiku.Dataset("COMPOUND_PDA_QUERY")
Compound_PDA_Query_DF = COMPOUND_PDA_QUERY.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### Proper Formating of Name Columns

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
candidate_main_prepped_df = candidate_MAIN_RAW_QUERY_df.copy()

candidate_main_prepped_df['Project_Manager'] = candidate_main_prepped_df['Project_Manager'].str.replace(', ',',').str.replace(',',', ')
candidate_main_prepped_df['Project_Planner'] = candidate_main_prepped_df['Project_Planner'].str.replace(', ',',').str.replace(',',', ')
candidate_main_prepped_df['Exec_Review_By'] = candidate_main_prepped_df['Exec_Review_By'].str.replace(', ',',').str.replace(',',', ')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
Medicine_Lead_DF['Medicine_Lead'] = Medicine_Lead_DF['Medicine_Lead'].str.replace(', ',',').str.replace(',',', ')
Medicine_Lead_DF['Global_Clinical_Lead'] = Medicine_Lead_DF['Global_Clinical_Lead'].str.replace(', ',',').str.replace(',',', ')
Medicine_Lead_DF['Global_Clinical_Lead'] = Medicine_Lead_DF['Global_Clinical_Lead'].str.replace("'",'').str.replace('.','')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### PDA Data Extract to Redshift

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def prep_milestone(df):
    # sort milestones data by Code column
    df = df.sort_values(by=["Code"])#.applymap(date_to_UTC)

    # Rename column names
    df = df.rename(columns={'Candidate_Milestone_Display_Name':'name',
                            'Candidate_Milestone_Name':'Milestone',
                            'Candidate_Task_Core_Code':'core',
                            'Candidate_Task_Duplicate_Milestone_Descriptor':'desc',
                            'Candidate_Task_End_Date':'date',
                            'Candidate_Task_Pcnt_Comp':'pcnt',
                            'Candidate_Task_Current_GEM':'gem'})

    # generating display_date
    # 1. IF GEM milestone exists and LE milestone date is planned date (not an actual date ->  pcnt = 0), then display GEM date
    # 2. IF LE date is an actual date, display LE date even if GEM milestone date exists.
    # 3. if gem is empty and pcnt is equal to 0 then display_date equal to le date

    cond1 = ((df['gem'].notna()) & (df['pcnt']==0))
    cond2 = ((df['date'].notna()) & (df['pcnt']==100))
    cond3 = ((df['gem'].isna()) & (df['pcnt']==0))


    df.loc[cond1,'display_date'] = df.loc[cond1,'gem']
    df.loc[cond2,'display_date'] = df.loc[cond2,'date']
    df.loc[cond3,'display_date'] = df.loc[cond3,'date']
    df['display_date'] = pd.to_datetime(df['display_date'])
    return df.reset_index(drop=True)

milestones_prep_df = prep_milestone(milestones_MAIN_RAW_QUERY_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Transforming Long to wide so to be loaded to Redshift table

PDA_DF = milestones_prep_df.pivot_table(index=['Code'], columns='name', values='display_date',aggfunc='min').reset_index()

CANDIDATE_MEDICINE_PDA_DF = functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'),
                                                [candidate_main_prepped_df,
                                                 Medicine_Lead_DF[['Code','Medicine_Lead', 'Global_Clinical_Lead']],
                                                PDA_DF])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
list_of_attrs = ['Code','Candidate_Business_Category','Compound_Name','Short_Description', 'Medium_Description','Phase',
                    'Unit_Detail','Type','Subtype','Portfolio_Priority','Status','Project_Manager','Medicine_Lead',
                    'Project_Planner', 'Global_Clinical_Lead','Phase I','POM','ESS','ESoE','SOCA','Phase II',
                     'PSS','POC','DP3','Pivotal Program Start (PPS)','Pivotal Prep Investment', 'Phase III','FIH','NDA Submission','NDA Approval',
                     'MAA Submission','MAA Approval','JNDA Submission','JNDA Approval','CNDA Submission','CNDA Approval']

PDA_candidate_milestones_join_df = CANDIDATE_MEDICINE_PDA_DF[list_of_attrs]

# renaming column names
PDA_candidate_milestones_join_df = PDA_candidate_milestones_join_df.rename(columns={'Candidate_Business_Category':'Category',
                                                                                   'Compound_Name':'Medicine',
                                                                                   'Medium_Description':'Candidate',
                                                                                   'Unit_Detail':'Unit',
                                                                                   'PSS':'POC_Study_Start'})


# replacing space in column names with '_'
PDA_candidate_milestones_join_df.columns = PDA_candidate_milestones_join_df.columns.str.replace(' ','_')

# filtering the dataframe based on conditions
# Portfolio_Priority_Filter_list = ['Lightspeed', 'Priority 2', 'Priority 3']
PDA_candidate_milestones_join_df = (PDA_candidate_milestones_join_df[(PDA_candidate_milestones_join_df['Status']
                                                                      .isin(['Ongoing',
                                                                             'Awaiting Dev Decision',
                                                                            'Strategic Hold'
                                                                            ]))]
                                    .reset_index(drop=True))
#                                  & (PDA_candidate_milestones_join_df['Portfolio_Priority'].isin(Portfolio_Priority_Filter_list))].reset_index(drop=True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
PDA_candidate_milestones_join_df = PDA_candidate_milestones_join_df.rename(columns={'Pivotal_Program_Start_(PPS)':'Pivotal_Program_Start_PPS'})
# PDA_candidate_milestones_join_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
JP_CH_LOOKUP_DF['Drug_Program_Code'] = JP_CH_LOOKUP_DF['Code'].str[:-1]

OP_compound_join_df = pd.merge(JP_CH_LOOKUP_DF,Compound_PDA_Query_DF,
                                                     on='Drug_Program_Code', how='left')
PDA_cand_mile_OP_compd_join_df = pd.merge(PDA_candidate_milestones_join_df,OP_compound_join_df,
                                                     on='Code', how='left')
# PDA_cand_mile_OP_compd_join_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def earliest_dates(DF,cols_list,col_name):
    try:
        NEW_DF = DF.copy()
        NEW_DF[col_name] = NEW_DF[cols_list].stack().dropna().groupby(level=0).min()#NEW_DF[cols_list].min(axis=1)
        DF = pd.merge(DF, NEW_DF[['Code',col_name]], on='Code', how='left')
        return DF[col_name]
    except Exception as e:
        print('Unable to find', '->', e)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
PDA_cand_mile_OP_compd_join_df['Earliest_Approval_Date']=earliest_dates(PDA_cand_mile_OP_compd_join_df,
                                                           ['NDA_Approval','MAA_Approval','JNDA_Approval','CNDA_Approval'],
                                                           'Earliest_Approval_Date')
PDA_cand_mile_OP_compd_join_df['Earliest_Submission_Date']=earliest_dates(PDA_cand_mile_OP_compd_join_df,
                                                           ['NDA_Submission','MAA_Submission','JNDA_Submission','CNDA_Submission'],
                                                           'Earliest_Submission_Date')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def extract_datepart(df, dt_cols):
    for col in dt_cols:
        df[col] = df[col].dt.year
    return df

# this function gets earliest regions based on earliest date of approval and submission
# and also accomadates if same data exists for multiple columns
def earliest_region(orig_df):
    df = orig_df.copy()

    APPR_SUB_LIST = ["NDA_Approval", "MAA_Approval","CNDA_Approval", "JNDA_Approval",
                    "NDA_Submission", "MAA_Submission","CNDA_Submission", "JNDA_Submission"]
    # extracting year from datetime columns
    dt_cols = APPR_SUB_LIST + ['Earliest_Approval_Date', 'Earliest_Submission_Date']
    df = extract_datepart(df, dt_cols)

    NDA_a_list = ["Code", "NDA_Approval", "MAA_Approval","CNDA_Approval", "JNDA_Approval"]
    MAA_a_list = ["Code", "MAA_Approval","CNDA_Approval", "JNDA_Approval", "NDA_Approval"]
    CNDA_a_list = ["Code","CNDA_Approval", "JNDA_Approval", "NDA_Approval", "MAA_Approval"]
    JNDA_a_list = ["Code", "JNDA_Approval", "NDA_Approval", "MAA_Approval","CNDA_Approval"]
    NDA_s_list = ["Code", "NDA_Submission", "MAA_Submission","CNDA_Submission", "JNDA_Submission"]
    MAA_s_list = ["Code", "MAA_Submission","CNDA_Submission", "JNDA_Submission", "NDA_Submission"]
    CNDA_s_list = ["Code","CNDA_Submission", "JNDA_Submission", "NDA_Submission", "MAA_Submission"]
    JNDA_s_list = ["Code", "JNDA_Submission", "NDA_Submission", "MAA_Submission","CNDA_Submission"]

    _list = []
    for i, EAD, ESD, each_code in zip(range(len(df.index)),list(df['Earliest_Approval_Date']),
                                      list(df['Earliest_Submission_Date']),list(df.Code)):

        final_dict= {}
        final_dict['Code'] = each_code

        final_dict['NDA_Earliest_Approval_Region'] = (df[NDA_a_list] == EAD).idxmax(axis=1)[i]
        final_dict['MAA_Earliest_Approval_Region'] = (df[MAA_a_list] == EAD).idxmax(axis=1)[i]
        final_dict['CNDA_Earliest_Approval_Region'] = (df[CNDA_a_list] == EAD).idxmax(axis=1)[i]
        final_dict['JNDA_Earliest_Approval_Region'] = (df[JNDA_a_list] == EAD).idxmax(axis=1)[i]
        final_dict['NDA_Earliest_Submission_Region'] = (df[NDA_s_list] == ESD).idxmax(axis=1)[i]
        final_dict['MAA_Earliest_Submission_Region'] = (df[MAA_s_list] == ESD).idxmax(axis=1)[i]
        final_dict['CNDA_Earliest_Submission_Region'] = (df[CNDA_s_list] == ESD).idxmax(axis=1)[i]
        final_dict['JNDA_Earliest_Submission_Region'] = (df[JNDA_s_list] == ESD).idxmax(axis=1)[i]
        _list.append(final_dict)
    final_df = pd.DataFrame(_list)

    final_df.replace({"NDA_Approval": "US", "MAA_Approval": "EU",
                                                 "CNDA_Approval": "China", "JNDA_Approval": "Japan",
                                                 "Code":""}, inplace=True)
    final_df.replace({"NDA_Submission": "US", "MAA_Submission": "EU",
                                                 "CNDA_Submission": "China", "JNDA_Submission": "Japan",
                                                   "Code":""}, inplace=True)
    APPR_LIST = ['NDA_Earliest_Approval_Region', 'MAA_Earliest_Approval_Region',  'CNDA_Earliest_Approval_Region',
                 'JNDA_Earliest_Approval_Region']
    SUB_LIST = ['NDA_Earliest_Submission_Region', 'MAA_Earliest_Submission_Region', 'CNDA_Earliest_Submission_Region',
                'JNDA_Earliest_Submission_Region']

    final_df["Earliest_Approval_Region"] = final_df[APPR_LIST].apply(lambda x: "/".join(set([str(i) for i in x if pd.notnull(i)])), axis=1)
    final_df["Earliest_Submission_Region"] = final_df[SUB_LIST].apply(lambda x: "/".join(set([str(i) for i in x if pd.notnull(i)])), axis=1)

    final_df = final_df[["Code", "Earliest_Approval_Region", "Earliest_Submission_Region"]]
    final_df.replace({"EU/US" : "US/EU"}, inplace=True)


#     RESULT_DF = pd.merge(orig_df,final_df, on='Code',how='left')

    return final_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
PDA_earliest_region_df = earliest_region(PDA_cand_mile_OP_compd_join_df)
PDA_cand_mile_OP_compd_ER_join_df = PDA_cand_mile_OP_compd_join_df.merge(PDA_earliest_region_df)
# PDA_cand_mile_OP_compd_ER_join_df.dtypes

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# df[[column for column in df.columns if is_datetime(df[column])]]
def extract_Date_from_Datetime(df):
    for DT_Col in df.columns:
        if is_datetime(df[DT_Col]):
            df[DT_Col] = pd.to_datetime(df[DT_Col]).apply(lambda x: x.date())
            df[DT_Col].replace({pd.NaT: ""}, inplace=True)
    return df

PDA_cand_mile_OP_compd_ER_join_df = extract_Date_from_Datetime(PDA_cand_mile_OP_compd_ER_join_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# move column to any position in schema
def move_column(df,col,position_to_move):
    if col in df:
        column_to_move = df.pop(col)
        df.insert(position_to_move,col,column_to_move)
    return

move_column(PDA_cand_mile_OP_compd_ER_join_df,'Subtype',len(PDA_cand_mile_OP_compd_ER_join_df.columns)-1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
PDA_DATA_EXTRACT_df = PDA_cand_mile_OP_compd_ER_join_df.drop(['Japan','China','Drug_Program_Code','Compound_Number'],axis=1) #... # Compute a Pandas dataframe to write into Porfolio_main_out

# Write recipe outputs
PDA_DATA_EXTRACT = dataiku.Dataset("PDA_DATA_EXTRACT")
PDA_DATA_EXTRACT.write_with_schema(PDA_DATA_EXTRACT_df)
