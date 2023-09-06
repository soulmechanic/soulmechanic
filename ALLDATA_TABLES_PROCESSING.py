# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import functools
from datetime import datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime

# Read recipe inputs
# AllDATA_PROJECT_df = dataiku.Dataset("AllDATA_PROJECT").get_dataframe()
ALLDATA_TEAM_ROLES_df = dataiku.Dataset("ALLDATA_TEAM_ROLES").get_dataframe()

Stage_Order_Mapping_df = dataiku.Dataset("Stage_Order_Mapping").get_dataframe()
STATUS_BINNED_MAPPING_df = dataiku.Dataset("STATUS_BINNED_MAPPING").get_dataframe()

Milestones_Order_df = dataiku.Dataset("Milestones_Order").get_dataframe()

ALLDATA_30DAY_CHANGE_TRACKING_df = dataiku.Dataset("ALLDATA_30DAY_CHANGE_TRACKING").get_dataframe(columns=['PROJECT_CODE', 'PROJECT_DELTA', 'MILESTONE_DELTA2'])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### PIVOTED_MILESTONE Data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def format_date(value):
    try:
        if isinstance(value, date):
            if not pd.isnull(value):
                return value.strftime('%m-%d-%Y')
            else: #empty dates are still date instances, will be set to none here
                pass
        else:
            return value

    except:
        return value

def prep_for_Snowflake_datetime(value):
    try:
        if isinstance(value, datetime):
#             print(type(value))
            if not pd.isnull(value):
                return pd.Timestamp(value).tz_convert('UTC').floor(freq='d')  + pd.DateOffset(minutes=600)
            else: #empty dates are still date instances, will be set to none here
                pass
        else:
            return value

    except:
        return value


def prep_for_Snowflake_datetime_NTZ(value):
    try:
        if isinstance(value, datetime):
#             print(type(value))
            if not pd.isnull(value):
#                 print(type(value))
                return pd.Timestamp(value).tz_localize(None)#.floor(freq='d')  + pd.DateOffset(minutes=600)
            else: #empty dates are still date instances, will be set to none here
                pass
        else:
            return value

    except:
        return value


def format_date_new(df):
    date_cols = df.select_dtypes(include='datetime64[ns]').columns
    df[date_cols] = df[date_cols].apply(remove_timezone)
    return df

def format_FloatValues(df):
    float_cols = df.select_dtypes(include='float').columns
    for float_col in float_cols:
        df[float_col] = np.floor(pd.to_numeric(df[float_col], errors='coerce')).astype('Int64')
#     df[float_cols] = df[float_cols].astype(np.int64,errors='ignore')
    return df

def extract_Date_from_Datetime(df):
    for DT_Col in df.columns:
        if is_datetime(df[DT_Col]):
            df[DT_Col] = pd.to_datetime(df[DT_Col]).apply(lambda x: x.date())
            df[DT_Col].replace({pd.NaT: ""}, inplace=True)
            df = df.applymap(format_date)
    return df

def milestones_cols_from_DX_XFN(df,cols_list):
    DX_cols_list = cols_list + ['PROJECT_CODE','PROJ_TYPE']
    df_DX = df[DX_cols_list][df['PROJ_TYPE']=='DX_XFN']
    df_DX['PROJ_TYPE'] = df_DX['PROJ_TYPE'].replace('DX_XFN','CA_XFN')
    df_CA = df.drop(columns=cols_list)
    df = df_CA.merge(df_DX,on=['PROJECT_CODE','PROJ_TYPE'],how='outer')
    df = df[df['PROJ_TYPE']=='CA_XFN'].reset_index(drop=True)

    return df

def sort_cols_except_few(df,few_cols):
    cols = list(df.loc[:, ~df.columns.isin(few_cols)].columns)
    cols.sort()
    final_cols = few_cols + cols
    return df[final_cols]

def SortMilestoneColumns(df1,df2):
    MilesColsOrderList = []
    df1=df1.sort_values(by=['MILESTONE_ORDER'])
    for Mcol in list(df1['CANDIDATE_MILESTONE_DISPLAY_NAME']):
        FCol = Mcol+'_FORECAST'
        PCol = Mcol+'_PERCENT'
        MilesColsOrderList.append(FCol)
        MilesColsOrderList.append(PCol)
    cols = list(df2.loc[:, df2.columns.isin(['PROJECT_CODE','PROJ_TYPE'])].columns)
    df2 = df2[cols+MilesColsOrderList]
#     df2[MilesColsOrderList] = df2[MilesColsOrderList].astype('Int64')
    return df2

def earliest_dates(df,ECol_Name,DateCols_list):
    df[ECol_Name] = df[DateCols_list].stack().dropna().groupby(level=0).min()
    return df

def earliest_ColName(df,ECol_Name,DateCols_list):
    DateCols_list = sorted(DateCols_list,reverse=True)
    df['First'] = df[DateCols_list].stack().dropna().groupby(level=0).idxmin(axis="columns").str.get(1).str.split('_').str[0]
    DateCols_list = sorted(DateCols_list)
    df['Second'] = df[DateCols_list].stack().dropna().groupby(level=0).idxmin(axis="columns").str.get(1).str.split('_').str[0]
    df[ECol_Name] = (df[['First','Second']].astype(str)
          .apply(lambda x: '/'.join(x) if x.First != x.Second else x.First ,axis=1)).replace('nan','')
    df = df.drop(columns=['First','Second'])
    return df

def earliest_Descp(df,JoinDf,Dcol_Name, Con_Col,JoinCol,List_Milestones, NewList_Milestones,aggfunc='min'):
    # filter required columns and row wise
    JoinDf=JoinDf[['PROJECT_CODE','PROJ_TYPE','DUPLICATE_MILESTONE_DESCRIPTOR',
                   'Candidate_Milestone_Display_Name','PLAN_FINISH']].drop_duplicates()



    JoinDf=JoinDf[(JoinDf['Candidate_Milestone_Display_Name'].isin(List_Milestones)) & (JoinDf['PROJ_TYPE']=='CA_XFN')]

    if aggfunc=='none':
        JoinDf = JoinDf.reset_index()
        JoinDf['PROJECT_CODE'] = JoinDf['PROJECT_CODE']+"|"+JoinDf['PROJ_TYPE']+"|"+JoinDf['index'].astype(str)+"-"+JoinDf['DUPLICATE_MILESTONE_DESCRIPTOR']
        JoinDf =JoinDf.drop('index',axis=1).dropna(subset=['PROJECT_CODE'])

        JoinDf = pd.pivot(JoinDf, index='PROJECT_CODE',columns = 'Candidate_Milestone_Display_Name',
                         values='PLAN_FINISH').reset_index().rename_axis(None, axis=1)

        JoinDf['DUPLICATE_MILESTONE_DESCRIPTOR'] = JoinDf['PROJECT_CODE'].str.split('-').str[1]
        JoinDf['PROJECT_CODE'] = JoinDf['PROJECT_CODE'].str.split('-').str[0]

    if aggfunc=='min':
        JoinDf=(JoinDf.pivot_table(index=['PROJECT_CODE','DUPLICATE_MILESTONE_DESCRIPTOR'],
                                   columns = 'Candidate_Milestone_Display_Name', values='PLAN_FINISH', aggfunc='min')
                .reset_index().rename_axis(None, axis=1))

    # combining two columns into one (exp.:NDA Approval and MAA Approval)
    JoinDf = pd.melt(JoinDf, id_vars=['PROJECT_CODE','DUPLICATE_MILESTONE_DESCRIPTOR'],
                     value_vars=List_Milestones,value_name=List_Milestones[0])



    # merging pivoted data to input data
    df = (df.merge(JoinDf,left_on=['PROJECT_CODE']+[JoinCol],right_on=['PROJECT_CODE']+[JoinDf.columns[3]],how='left')
          .drop(columns=[JoinDf.columns[3],JoinDf.columns[2]]))#,JoinCol]))

    # combining rows of DUPLICATE_MILESTONE_DESCRIPTOR column with same project code
    Tempdf = df['DUPLICATE_MILESTONE_DESCRIPTOR'].groupby([df.PROJECT_CODE]).apply(tuple).reset_index().replace('(nan,)','')

    df = df.drop(['DUPLICATE_MILESTONE_DESCRIPTOR'],axis=1,errors='ignore')
    df = df.merge(Tempdf,on='PROJECT_CODE',how='left')

    # regex and remove some unwanted nan and commas
    df['DUPLICATE_MILESTONE_DESCRIPTOR'] = (df['DUPLICATE_MILESTONE_DESCRIPTOR'].astype(str).replace('(nan,)','')
                                            .str.replace(r',\)', ')')).str.replace("'","")
    # combining the Earliest type with DUPLICATE_MILESTONE_DESCRIPTOR ex.: NDA/MAA ('Pivotal Ph 2')
    df[Dcol_Name] = df[Con_Col]  + ' '+df['DUPLICATE_MILESTONE_DESCRIPTOR']


    #drop unwanted columns and duplicate rows
    df = (df.drop(['DUPLICATE_MILESTONE_DESCRIPTOR']+List_Milestones,axis=1,errors='ignore')
          .drop_duplicates(subset=['PROJECT_CODE'],keep='first'))
    return df

def MilesPivotTable(df, Values,AggType):
    if AggType=='min':
        df = (df.pivot_table( index=['PROJECT_CODE','PROJ_TYPE'],columns = 'Candidate_Milestone_Display_Name',
                             values=Values, aggfunc=AggType).reset_index().rename_axis(None, axis=1))
        return df
    if AggType=='none':
        df = df.reset_index()
        df['PROJECT_CODE'] = df['PROJECT_CODE']+"|"+df['PROJ_TYPE']+"|"+df['index'].astype(str)
        df = pd.pivot(df, index='PROJECT_CODE',columns = 'Candidate_Milestone_Display_Name',
                         values=Values).reset_index().rename_axis(None, axis=1)
        if Values=='PLAN_FINISH':

            df.loc[:, df.columns != 'PROJECT_CODE'].apply(pd.to_datetime)
        df['TMP_PROJECT_CODE'] = df['PROJECT_CODE'].str.split('|').str[0]
        df['PROJ_TYPE'] = df['PROJECT_CODE'].str.split('|').str[1]
        for colm in ['TMP_PROJECT_CODE']:
            col = df.pop(colm)
            df.insert(0, colm, col)
        df = df.drop_duplicates().drop('PROJECT_CODE',axis=1).rename(columns={'TMP_PROJECT_CODE':'PROJECT_CODE'})
        df = df[df['PROJ_TYPE']=='CA_XFN'].reset_index(drop=True)
        return df
    if AggType=='YrAchv':
        df = df.reset_index()
        df['PROJECT_CODE'] = df['PROJECT_CODE']+"|"+df['PROJ_TYPE']+"|"+df['index'].astype(str)
        df = pd.pivot(df, index='PROJECT_CODE',columns = 'Candidate_Milestone_Display_Name',
                         values=Values).reset_index().rename_axis(None, axis=1)
        return df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
dataset_list = ['ALLDATA_UNPIVOTED_MILESTONES']

aggfunc= 'min'

for dataset in dataset_list:
    if aggfunc=='min':
        ALLDATA_UNPIVOTED_MILESTONES_df = dataiku.Dataset(dataset).get_dataframe()
        PIVOTED_MILESTONES_DF1 = MilesPivotTable(ALLDATA_UNPIVOTED_MILESTONES_df,'PLAN_FINISH','min')
        PIVOTED_MILESTONES_DF2 = MilesPivotTable(ALLDATA_UNPIVOTED_MILESTONES_df,'PCT_COMPLETE','min')
        PIVOTED_MILESTONES_DF = PIVOTED_MILESTONES_DF1.merge(PIVOTED_MILESTONES_DF2,how='left',
                                                             on=['PROJECT_CODE','PROJ_TYPE'],
                                                             suffixes=('_FORECAST', '_PERCENT'))


#     if aggfunc=='none':
#         ALLDATA_UNPIVOTED_MILESTONES_df = dataiku.Dataset(dataset).get_dataframe()
#         ALLDATA_UNPIVOTED_MILESTONES_df = ALLDATA_UNPIVOTED_MILESTONES_df.applymap(prep_for_Snowflake_datetime_NTZ)
#         PIVOTED_MILESTONES_DF1 = MilesPivotTable(ALLDATA_UNPIVOTED_MILESTONES_df,'PLAN_FINISH','none')
#         PIVOTED_MILESTONES_DF2 = MilesPivotTable(ALLDATA_UNPIVOTED_MILESTONES_df,'PCT_COMPLETE','none')
#         PIVOTED_MILESTONES_DF = PIVOTED_MILESTONES_DF1.merge(PIVOTED_MILESTONES_DF2,how='left',
#                                                              on=['PROJECT_CODE'],
#                                                              suffixes=('_FORECAST', '_PERCENT'))
#         PIVOTED_MILESTONES_DF.columns = PIVOTED_MILESTONES_DF.columns.str.replace(' ','_')
#         PIVOTED_MILESTONES_DF['PROJECT_TRACKING_CODE'] = PIVOTED_MILESTONES_DF['PROJECT_CODE'].str.split('|').str[0]
#         PIVOTED_MILESTONES_DF['PROJ_TYPE'] = PIVOTED_MILESTONES_DF['PROJECT_CODE'].str.split('|').str[1]
#         PIVOTED_MILESTONES_DF = PIVOTED_MILESTONES_DF.applymap(prep_for_Snowflake_datetime)
#         PIVOTED_MILESTONES_DF = format_date_new(PIVOTED_MILESTONES_DF)
#         for colm in ['PROJ_TYPE','PROJECT_TRACKING_CODE']:
#             col = PIVOTED_MILESTONES_DF.pop(colm)
#             PIVOTED_MILESTONES_DF.insert(0, colm, col)
#         PIVOTED_MILESTONES_DF = PIVOTED_MILESTONES_DF.drop('PROJECT_CODE',axis=1).rename(columns={'PROJECT_TRACKING_CODE':
#                                                                                                  'PROJECT_CODE'})

    # Write recipe outputs
#     ALLDATA_PIVOTED_MILESTONES = dataiku.Dataset("ALLDATA_PIVOTED_MILESTONES").write_with_schema(PIVOTED_MILESTONES_DF)


    list_LD_CS = ['LD_FORECAST','CS_FORECAST','LD_PERCENT','CS_PERCENT']
    PIVOTED_MILESTONES_DF[list_LD_CS] = (PIVOTED_MILESTONES_DF.groupby(['PROJECT_CODE'])[list_LD_CS]
                                          .fillna(method='bfill'))


    ## PRISM, ESD, ESD milestones and dates will always come from 70D resource plan (proj_type = ‘DX_XFN’)
    cols_list = ['ESD_FORECAST','SDS_FORECAST']
    PIVOTED_MILESTONES_DF = milestones_cols_from_DX_XFN(PIVOTED_MILESTONES_DF,cols_list)

    PIVOTED_MILESTONES_DF = SortMilestoneColumns(Milestones_Order_df,PIVOTED_MILESTONES_DF)
    PIVOTED_MILESTONES_DF.columns = PIVOTED_MILESTONES_DF.columns.str.replace(' ','_')

    # sorting columns after 'PROJECT_CODE' and'PROJ_TYPE'
#     PIVOTED_MILESTONES_DF = sort_cols_except_few(PIVOTED_MILESTONES_DF,['PROJECT_CODE','PROJ_TYPE'])

    # converting datetime columns to date string columns
#     PIVOTED_MILESTONES_DF = extract_Date_from_Datetime(PIVOTED_MILESTONES_DF)

    # Earliest Approval and submission
    Acols_list = ['NDA_Approval_FORECAST','MAA_Approval_FORECAST']
    PIVOTED_MILESTONES_DF = earliest_dates(PIVOTED_MILESTONES_DF,'EARLIEST_APP',Acols_list)

    PIVOTED_MILESTONES_DF = earliest_ColName(PIVOTED_MILESTONES_DF,'EARLIEST_APP_TYPE',Acols_list)

    Scols_list = ['NDA_Submission_FORECAST','MAA_Submission_FORECAST']
    PIVOTED_MILESTONES_DF = earliest_dates(PIVOTED_MILESTONES_DF,'EARLIEST_SUB',Scols_list)

    PIVOTED_MILESTONES_DF = earliest_ColName(PIVOTED_MILESTONES_DF,'EARLIEST_SUB_TYPE',Scols_list)

#     display(PIVOTED_MILESTONES_DF.drop_duplicates())
    # Earliest approval and submission descp
    Acols_list_org = ['NDA Approval','MAA Approval']
    PIVOTED_MILESTONES_DF = earliest_Descp(PIVOTED_MILESTONES_DF,ALLDATA_UNPIVOTED_MILESTONES_df,
                                               'EARLIEST_APP_DESCP', 'EARLIEST_APP_TYPE','EARLIEST_APP',
                                               Acols_list_org,Acols_list,aggfunc=aggfunc)
    Scols_list_org = ['NDA Submission','MAA Submission']

    PIVOTED_MILESTONES_DF = earliest_Descp(PIVOTED_MILESTONES_DF,ALLDATA_UNPIVOTED_MILESTONES_df,
                                               'EARLIEST_SUB_DESCP', 'EARLIEST_SUB_TYPE','EARLIEST_SUB',
                                               Scols_list_org, Scols_list,aggfunc=aggfunc)




    PIVOTED_MILESTONES_DF = PIVOTED_MILESTONES_DF.drop(columns=['PROJ_TYPE','EARLIEST_APP_TYPE','EARLIEST_SUB_TYPE'])


#     PIVOTED_MILESTONES_DF = PIVOTED_MILESTONES_DF.applymap(prep_for_Snowflake_datetime)
    PIVOTED_MILESTONES_DF['PROJECT_CODE'] = PIVOTED_MILESTONES_DF['PROJECT_CODE'].str.split('|').str[0]

    PIVOTED_MILESTONES_DF = format_FloatValues(PIVOTED_MILESTONES_DF)

#     display(PIVOTED_MILESTONES_DF)
    # Write recipe outputs
    PIVOTED_MILESTONES_DF = PIVOTED_MILESTONES_DF.sort_values(PIVOTED_MILESTONES_DF.columns[0], ascending = True).reset_index(drop=True)
    ALL_DATA_VALIDATION = dataiku.Dataset("ALLDATA_PIVOTED_MILESTONES").write_from_dataframe(PIVOTED_MILESTONES_DF)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ### PROJECT_METADATA

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def LD_where_multi_cond(choices,df,ColName):
    current_year = datetime.now().year
    conditions = [
        # condition one
                    (df['PROJECT_CODE'].str.contains('70D'))
                  & (df['STAGE']=='LD')
                  & (df['STATUS']=='Ongoing')
                  & (df['STAGE_PHASE_START_DATE'].dt.year==current_year)
                  & (df['STAGE_PHASE_START_DATE']!=df['PROJECT_CREATION_DATE']),
        # condition two
                    (df['PROJECT_CODE'].str.contains('70D'))
                  & (df['STAGE']=='LD')
                  & (df['STATUS']=='Ongoing')
                  & (df['STAGE_PHASE_START_DATE']==df['PROJECT_CREATION_DATE'])
                  & (df['CMPD_PROJECT_SOURCE']=='External')]
    choices = [choices,df[ColName]]
    df[ColName] = np.select(conditions, choices, default=df[ColName])
    return df

def LD_where_multi_cond1(choices,df,ColName):
    current_year = datetime.now().year
    if ColName=="LD_FORECAST":
        conditions = [
            # condition one
                        (df['PROJECT_CODE'].str.contains('70D'))
                        & (df['STAGE']=='LD')
                        & (df['STATUS']=='Ongoing')
                        & (df['STAGE_PHASE_START_DATE']==df['PROJECT_CREATION_DATE'])
                        & (df['CMPD_PROJECT_SOURCE']=='External')
                        & (df["LD_FORECAST"].isnull())]
        choices = [choices]
        df[ColName] = np.select(conditions, choices, default=df[ColName])
        return df

    if ColName=="LD_PERCENT":
        conditions = [
            # condition two
                        (df['PROJECT_CODE'].str.contains('70D'))
                      & (df['STAGE']=='LD')
                      & (df['STATUS']=='Ongoing')
                      & (df['STAGE_PHASE_START_DATE']==df['PROJECT_CREATION_DATE'])
                      & (df['CMPD_PROJECT_SOURCE']=='External')
                      & (df["LD_PERCENT"].isnull())]
        choices = [choices]
        df[ColName] = np.select(conditions, choices, default=df[ColName])
        return df


def MilesPivotForYrAchv(df):
    PIVOTED_MILESTONES_DF1 = MilesPivotTable(df,'PLAN_FINISH','YrAchv')
    PIVOTED_MILESTONES_DF2 = MilesPivotTable(df,'PCT_COMPLETE','YrAchv')
    PIVOTED_MILESTONES_DF = PIVOTED_MILESTONES_DF1.merge(PIVOTED_MILESTONES_DF2,how='left',
                                                         on=['PROJECT_CODE'],
                                                         suffixes=('_FORECAST', '_PERCENT'))
    PIVOTED_MILESTONES_DF.columns = PIVOTED_MILESTONES_DF.columns.str.replace(' ','_')
    PIVOTED_MILESTONES_DF['TMP_PROJECT_CODE'] = PIVOTED_MILESTONES_DF['PROJECT_CODE'].str.split('|').str[0]

    PIVOTED_MILESTONES_DF['PROJ_TYPE'] = PIVOTED_MILESTONES_DF['PROJECT_CODE'].str.split('|').str[1]

    for colm in ['PROJ_TYPE','PROJECT_CODE','TMP_PROJECT_CODE']:
        col = PIVOTED_MILESTONES_DF.pop(colm)
        PIVOTED_MILESTONES_DF.insert(0, colm, col)

    PIVOTED_MILESTONES_DF = (PIVOTED_MILESTONES_DF[PIVOTED_MILESTONES_DF['PROJ_TYPE']=='CA_XFN']
                             .drop(['PROJ_TYPE','PROJECT_CODE'],axis=1)
                             .rename(columns={'TMP_PROJECT_CODE':'PROJECT_CODE'})
                             .reset_index(drop=True).drop_duplicates())

    return PIVOTED_MILESTONES_DF


def flag_current_yr_achivements(df,alldata_df,MilesList):

    df = df[['PROJECT_CODE','PLAN_FINISH','PCT_COMPLETE','DUPLICATE_MILESTONE_DESCRIPTOR',
             'Candidate_Milestone_Display_Name']].copy()

    df = df.loc[(df['PLAN_FINISH'].dt.year==current_year) &
                (df['PCT_COMPLETE']==100) &
                (df['Candidate_Milestone_Display_Name'].isin(MilesList))]



#     df['count']=(df.groupby(['PROJECT_CODE','Candidate_Milestone_Display_Name'])['Candidate_Milestone_Display_Name']
#                  .transform('size'))

    df.loc[(df['DUPLICATE_MILESTONE_DESCRIPTOR'].notnull()),'UNPIV_CYA'] = df['Candidate_Milestone_Display_Name'] + "(" +df['DUPLICATE_MILESTONE_DESCRIPTOR']+") Achieved"

    df['UNPIV_CYA'] = df['UNPIV_CYA'].fillna(df['Candidate_Milestone_Display_Name']+' Achieved')


    df['UNPIV_CYA'] = df.groupby(['PROJECT_CODE'])['UNPIV_CYA'].transform(lambda x: ', '.join(x))

    df = df.drop_duplicates(subset=['PROJECT_CODE','UNPIV_CYA']).reset_index(drop=True)

    alldata_df = (alldata_df[['PROJECT_CODE','CURRENT_YR_ACHIEVEMENTS','Transitioned']]
                  .merge(df[['PROJECT_CODE','UNPIV_CYA']],
                         right_on='PROJECT_CODE',left_on='PROJECT_CODE',how='left')
                  .drop_duplicates(subset=['PROJECT_CODE','UNPIV_CYA']))

    alldata_df.loc[(alldata_df['CURRENT_YR_ACHIEVEMENTS'].notnull())&
                   (alldata_df["Transitioned"]!='Yes'),
                   'CURRENT_YR_ACHIEVEMENTS']=alldata_df['CURRENT_YR_ACHIEVEMENTS']+','+alldata_df['UNPIV_CYA']


    alldata_df.loc[(alldata_df['CURRENT_YR_ACHIEVEMENTS'].isnull())&
                   (alldata_df["Transitioned"]!='Yes'),
                   'CURRENT_YR_ACHIEVEMENTS']=alldata_df['UNPIV_CYA']

    return alldata_df[['PROJECT_CODE','CURRENT_YR_ACHIEVEMENTS']]

def next_last_milestone(ALLDataDF,MainDF,ListMilesCols,Prefix):
    DF = MainDF[['PROJECT_CODE']+ListMilesCols]
    DF = pd.melt(DF, id_vars=['PROJECT_CODE'],
                 value_vars=ListMilesCols,var_name=Prefix+'_MILESTONE',
                 value_name=Prefix+'_MILESTONE_DATE')
    DF[Prefix+'_MILESTONE'] = (DF[Prefix+'_MILESTONE'].str.replace('_FORECAST','')
                                   .replace('Pivotal_Program_Start_(PPS)','PPS'))
    DF[Prefix+'_MILESTONE_DATE'] = pd.to_datetime(DF[Prefix+'_MILESTONE_DATE'],utc=True)#.dt.tz_convert('US/Eastern')
    curr_datetime = pd.to_datetime("now", utc=True)#.tz_convert('US/Eastern')

    if Prefix.lower() == 'next':
        DF = DF[DF[Prefix+'_MILESTONE_DATE'] > curr_datetime]
        DF['dense_rank'] = DF.groupby('PROJECT_CODE')[Prefix+'_MILESTONE_DATE'].rank('dense')
    if Prefix.lower() == 'last':
        DF = DF[DF[Prefix+'_MILESTONE_DATE'] < curr_datetime]
        DF['dense_rank'] = DF.groupby('PROJECT_CODE')[Prefix+'_MILESTONE_DATE'].rank('dense', ascending=False)


    TempDF = DF[DF['dense_rank']==1.0].sort_values(by=['PROJECT_CODE',Prefix+'_MILESTONE_DATE']).drop_duplicates()
    TempDF[Prefix+'_MILESTONE'] = (TempDF[['PROJECT_CODE',Prefix+'_MILESTONE']]
                                   .groupby(['PROJECT_CODE'])[Prefix+'_MILESTONE']
                                   .transform(lambda x: '/'.join(x)))
    TempDF[['PROJECT_CODE',Prefix+'_MILESTONE']] = TempDF[['PROJECT_CODE',Prefix+'_MILESTONE']].drop_duplicates()

    OutDF = ALLDataDF.merge(TempDF[['PROJECT_CODE',Prefix+'_MILESTONE',Prefix+'_MILESTONE_DATE']],on='PROJECT_CODE',how='left')
    return OutDF

def wrd_reportig_flag(df,choices,ColName):
#     df[ColName] = 'OUT OF SCOPE'
    unitNotLike = ['Duplicate','Tech','Japan','China','Hospira','Established Pharma']
    projecttypeNotLike = ['Device','Submission','Technology']
    stageorderNotLike = ['00/RELOAD','00/TECH','Duplicate']
    statusbinnedNotLike = ['Placeholder','Awaiting Dev Decision']
    statusbinnedLike = ['Ongoing','On-Hold']
    current_year = str(pd.datetime.now().year)
    conditions = [ (df['DIVISION_REPORTING']=='WRD')
#                   & (df['STAGE_ORDER']!="00/RELOAD")
#                   & (df['STAGE_ORDER']!="00/TECH")
                  & (df['STAGE_ORDER'].str.contains('|'.join(stageorderNotLike))==False)
                  & (df['UNIT'].str.contains('|'.join(unitNotLike))==False)
                  & (df['PROJECT_TYPE'].str.contains('|'.join(projecttypeNotLike))==False)
                  & (df['STATUS_BINNED'].str.contains('|'.join(statusbinnedNotLike))==False)
                  & (df['STATUS_BINNED'].str.contains('|'.join(statusbinnedLike)))
                  & (df['CURRENT_YR_ACHIEVEMENTS']!='')
                    ]
    choices = [choices]
    df[ColName] = np.select(conditions, choices, default=np.nan)
    return df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# FUNCTIONS ALL DATA CURRENT YR ACHIVEMENT TABLE
def alldata_current_yr_achivements_table(df,alldata_df,MilesList):

    df = df[['PROJECT_CODE','PROJ_TYPE','PLAN_FINISH','PCT_COMPLETE','DUPLICATE_MILESTONE_DESCRIPTOR',
             'Candidate_Milestone_Display_Name']].copy()

    df = df.loc[(df['PLAN_FINISH'].dt.year==current_year) &
                (df['PCT_COMPLETE']==100) &
                (df['Candidate_Milestone_Display_Name'].isin(MilesList))]

    df['count']=(df.groupby(['PROJECT_CODE','Candidate_Milestone_Display_Name'])['Candidate_Milestone_Display_Name']
                 .transform('size'))

    df.loc[(df['DUPLICATE_MILESTONE_DESCRIPTOR'].notnull()),'UNPIV_CYA'] = df['Candidate_Milestone_Display_Name'] + "(" +df['DUPLICATE_MILESTONE_DESCRIPTOR']+") Achieved"

    df['UNPIV_CYA'] = df['UNPIV_CYA'].fillna(df['Candidate_Milestone_Display_Name']+' Achieved')

#     df['UNPIV_CYA'] = df.groupby(['PROJECT_CODE'])['UNPIV_CYA'].transform(lambda x: ', '.join(x))

    df = df.drop_duplicates(subset=['PROJECT_CODE','UNPIV_CYA']).reset_index(drop=True)

    alldata_df= alldata_df[['PROJECT_CODE','LD_FORECAST','PROJECT_LONG_NAME','COMPOUND_DISCOVERY_FINANCE_CODE',
                            'CATEGORY','CURRENT_YR_ACHIEVEMENTS']]#[alldata_df['CURRENT_YR_ACHIEVEMENTS']=='LD Achieved'].drop_duplicates()
    df = df[['PROJECT_CODE','PLAN_FINISH','PCT_COMPLETE','DUPLICATE_MILESTONE_DESCRIPTOR','UNPIV_CYA']]

    alldata_df = (alldata_df.merge(df, on='PROJECT_CODE',how='left')
                  .drop_duplicates(subset=['PROJECT_CODE','UNPIV_CYA','CURRENT_YR_ACHIEVEMENTS']))

    alldata_df['CURRENT_YR_ACHIEVEMENTS'] =alldata_df['CURRENT_YR_ACHIEVEMENTS'].fillna(alldata_df['UNPIV_CYA'])
    alldata_df['PLAN_FINISH'] =alldata_df['PLAN_FINISH'].fillna(alldata_df['LD_FORECAST'])
    alldata_df = alldata_df[(alldata_df['UNPIV_CYA'].notnull())]# &

    alldata_df = alldata_df[['PROJECT_CODE','PROJECT_LONG_NAME','CATEGORY','CURRENT_YR_ACHIEVEMENTS',
                             'DUPLICATE_MILESTONE_DESCRIPTOR','PLAN_FINISH',
                             'PCT_COMPLETE','COMPOUND_DISCOVERY_FINANCE_CODE']].rename(columns={'PROJECT_LONG_NAME':
                                                                                               'PROJECT_NAME',
                                                                                               'CURRENT_YR_ACHIEVEMENTS':
                                                                                               'MILESTONE',
                                                                                               'PLAN_FINISH':
                                                                                               'MILESTONE_DATE'}).reset_index(drop=True)


    return alldata_df#alldata_df[['PROJECT_CODE','CURRENT_YR_ACHIEVEMENTS']]

def write_snowflake(dataset,df):
    snowflake_destination = dataiku.Dataset(dataset)

    #pda_STG_ENRICH_PROJECT_METADATA = dataiku.Dataset("PDA_STG_ENRICH_PROJECT_METADATA")
    #snowflake_destination.write_with_schema(df)
    snowflake_destination.spec_item["appendMode"] = True
    with snowflake_destination.get_writer() as writer:
        writer.write_dataframe(df)
    return

def diff_btw_dfs(df1,df2):
    df2['MILESTONE'] = df2['MILESTONE'].str.strip()
    i1 = pd.MultiIndex.from_frame(df1[['PROJECT_CODE','MILESTONE']])
    i2 = pd.MultiIndex.from_frame(df2)
    return df1[~i1.isin(i2)]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # includes all milestones for yr achivement

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
dataset_list = ['ALLDATA_PROJECT_METADATA']
current_year = datetime.now().year

for dataset in dataset_list:

    AllDATA_PROJECT_df = dataiku.Dataset(dataset).get_dataframe()

    # merge with pivoted milestone data

    AllDATA_PROJECT_df =AllDATA_PROJECT_df.merge(PIVOTED_MILESTONES_DF, left_on=['PROJECT_CODE'],
                                                 right_on=['PROJECT_CODE'])#.drop(columns=['PROJECT_CODE'])

    CMPDCode = set(AllDATA_PROJECT_df['COMPOUND_DISCOVERY_FINANCE_CODE'].loc[(AllDATA_PROJECT_df["COMPOUND_DISCOVERY_FINANCE_CODE"].isin(set(AllDATA_PROJECT_df["PROJECT_CODE"])))])

    AllDATA_PROJECT_df.loc[(AllDATA_PROJECT_df["PROJECT_CODE"].isin(CMPDCode)),'Transitioned'] = 'Yes'


    # Mergeing all DFs
    AllDATA_PROJECT_df = pd.merge(AllDATA_PROJECT_df,Stage_Order_Mapping_df[['STAGE','STAGE_ORDER']],on='STAGE')

    AllDATA_PROJECT_df = pd.merge(AllDATA_PROJECT_df,STATUS_BINNED_MAPPING_df[['STATUS','STATUS_BINNED']],on='STATUS')

    testListCols = ['PROJECT_CODE','COMPOUND_DISCOVERY_FINANCE_CODE','CMPD_PROJECT_SOURCE','STATUS','STAGE',
                   'STAGE_PHASE_START_DATE','PROJECT_CREATION_DATE','LD_FORECAST','LD_PERCENT','CURRENT_YR_ACHIEVEMENTS',
                   'Transitioned']
    # LD Forcast and LD Percent

    AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS'] = np.nan


    LD_FILTR_COND1 = ((AllDATA_PROJECT_df['PROJECT_CODE'].str.contains('70D'))
              & (AllDATA_PROJECT_df['STAGE']=='LD')
              & (AllDATA_PROJECT_df['STATUS']=='Ongoing')
              & (AllDATA_PROJECT_df['STAGE_PHASE_START_DATE']==AllDATA_PROJECT_df['PROJECT_CREATION_DATE'])
              & (AllDATA_PROJECT_df['CMPD_PROJECT_SOURCE']!='External'))

    AllDATA_PROJECT_df.loc[LD_FILTR_COND1, "LD_FORECAST"] = pd.NaT

    AllDATA_PROJECT_df.loc[LD_FILTR_COND1, "LD_PERCENT"] = np.nan

    AllDATA_PROJECT_df.loc[(AllDATA_PROJECT_df["LD_FORECAST"].dt.year.astype('Int64')==current_year)
                           &(AllDATA_PROJECT_df["LD_PERCENT"]==100)
#                            & (AllDATA_PROJECT_df['STAGE']=='LD')
                           & (AllDATA_PROJECT_df['STATUS']=='Ongoing')
                           &(AllDATA_PROJECT_df["Transitioned"]!='Yes') , "CURRENT_YR_ACHIEVEMENTS"] = "LD Achieved"


    AllDATA_PROJECT_df =LD_where_multi_cond1(100,AllDATA_PROJECT_df,'LD_PERCENT')


    AllDATA_PROJECT_df = LD_where_multi_cond1(AllDATA_PROJECT_df['STAGE_PHASE_START_DATE'],AllDATA_PROJECT_df,'LD_FORECAST')


    LD_FILTR_COND2 = ((AllDATA_PROJECT_df['PROJECT_CODE'].str.contains('70D'))
                      & (AllDATA_PROJECT_df["LD_FORECAST"].dt.year.astype('Int64')==current_year)
                      &(AllDATA_PROJECT_df["LD_PERCENT"]==100)
                      & (AllDATA_PROJECT_df['STAGE']=='LD')
                      & (AllDATA_PROJECT_df['STATUS']=='Ongoing')
                      & (AllDATA_PROJECT_df['STAGE_PHASE_START_DATE']==AllDATA_PROJECT_df['PROJECT_CREATION_DATE'])
                      & (AllDATA_PROJECT_df['CMPD_PROJECT_SOURCE']=='External')
                      & (AllDATA_PROJECT_df["Transitioned"]!='Yes'))

    AllDATA_PROJECT_df.loc[LD_FILTR_COND2, "CURRENT_YR_ACHIEVEMENTS"] = "LD Achieved"




    YRACHIV_AllDATA_PROJECT_df = MilesPivotForYrAchv(ALLDATA_UNPIVOTED_MILESTONES_df)

    # writing data to Yr Achievement Table
    LD_ACHIV_DF = (YRACHIV_AllDATA_PROJECT_df.merge(AllDATA_PROJECT_df[['PROJECT_CODE','CURRENT_YR_ACHIEVEMENTS',
                                                                        'Transitioned','PROJECT_LONG_NAME',
                                                                        'COMPOUND_DISCOVERY_FINANCE_CODE', 'CATEGORY']],
                                         on='PROJECT_CODE')).drop_duplicates()

    PORTFOLIO_ALL_DATA_TEST_df = dataiku.Dataset("PORTFOLIO_ALL_DATA_IN").get_dataframe()

    PORTFOLIO_ALL_DATA_TEST_df = PORTFOLIO_ALL_DATA_TEST_df[['PROJECT_CODE','CURRENT_YR_ACHIEVEMENTS']][PORTFOLIO_ALL_DATA_TEST_df['CURRENT_YR_ACHIEVEMENTS'].notnull()]
    PORTFOLIO_ALL_DATA_TEST_df['CURRENT_YR_ACHIEVEMENTS'] = PORTFOLIO_ALL_DATA_TEST_df['CURRENT_YR_ACHIEVEMENTS'].str.split(',')
    PORTFOLIO_ALL_DATA_TEST_df = PORTFOLIO_ALL_DATA_TEST_df.explode('CURRENT_YR_ACHIEVEMENTS').rename(columns={'CURRENT_YR_ACHIEVEMENTS':'MILESTONE'})

    MilesList = list(set(Milestones_Order_df['CANDIDATE_MILESTONE_DISPLAY_NAME'][Milestones_Order_df['YR_ACHIEVEMENTS_Y_N']=='Yes']))

    MilesList.remove('LD')

    YR_ACHIV_DF = alldata_current_yr_achivements_table(ALLDATA_UNPIVOTED_MILESTONES_df,LD_ACHIV_DF,MilesList)

    YR_ACHIV_FINAL_DF = diff_btw_dfs(YR_ACHIV_DF,PORTFOLIO_ALL_DATA_TEST_df)


#     YR_ACHIV_FINAL_DF['PCT_COMPLETE'] = YR_ACHIV_FINAL_DF['PCT_COMPLETE'].fillna(0.0).apply(np.int64)

#     if not YR_ACHIV_FINAL_DF.empty:

#         YR_ACHIV_FINAL_DF['RUN_DATE']=current_date

#         write_snowflake('ALLDATA_YR_ACHIEVEMENTS_TEST',YR_ACHIV_FINAL_DF)

    ALLDATA_YR_ACHIEVEMENTS_TEST = dataiku.Dataset("ALLDATA_YR_ACHIEVEMENTS_STG").write_with_schema(YR_ACHIV_FINAL_DF)





    # continue with Portfolio ALL Data table
    YRACHIV_AllDATA_PROJECT_df = (YRACHIV_AllDATA_PROJECT_df
                                  .merge(AllDATA_PROJECT_df[['PROJECT_CODE','CURRENT_YR_ACHIEVEMENTS','Transitioned']],
                                         on='PROJECT_CODE')).drop_duplicates()



    MilesList = list(set(Milestones_Order_df['CANDIDATE_MILESTONE_DISPLAY_NAME'][Milestones_Order_df['YR_ACHIEVEMENTS_Y_N']=='Yes']))
    MilesList.remove('LD')

    YRACHIV_AllDATA_PROJECT_df = flag_current_yr_achivements(ALLDATA_UNPIVOTED_MILESTONES_df,YRACHIV_AllDATA_PROJECT_df,MilesList)


#     MilesList = list(set(Milestones_Order_df['CANDIDATE_MILESTONE_DISPLAY_NAME'][Milestones_Order_df['YR_ACHIEVEMENTS_Y_N']=='Yes']))
#     MilesList.remove('LD')
#     for Mile in MilesList:

#         Mile = Mile.replace(' ','_')

#         YRACHIV_AllDATA_PROJECT_df = flag_current_yr_achivements(YRACHIV_AllDATA_PROJECT_df ,
#                                                                  'CURRENT_YR_ACHIEVEMENTS',Mile)

    YRACHIV_AllDATA_PROJECT_df = YRACHIV_AllDATA_PROJECT_df[['PROJECT_CODE',
                                                             'CURRENT_YR_ACHIEVEMENTS']].drop_duplicates()

    AllDATA_PROJECT_df =AllDATA_PROJECT_df.merge(YRACHIV_AllDATA_PROJECT_df,
                                                 on='PROJECT_CODE',how='left').drop_duplicates()


    AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS_y'] = (AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS_y']
                                                       .fillna(AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS_x']))

    AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS_y']= (AllDATA_PROJECT_df[['PROJECT_CODE',
                                                                          'CURRENT_YR_ACHIEVEMENTS_y']]
                                                      .loc[AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS_y'].notnull()]
                                   .groupby(['PROJECT_CODE'])['CURRENT_YR_ACHIEVEMENTS_y']
                                   .transform(lambda x: ','.join(x)))#.str.split(", ").map(set).str.join(", ")


    AllDATA_PROJECT_df = (AllDATA_PROJECT_df.drop(['CURRENT_YR_ACHIEVEMENTS_x','Transitioned'],axis=1)
                          .rename(columns={'CURRENT_YR_ACHIEVEMENTS_y':'CURRENT_YR_ACHIEVEMENTS'})
                          .sort_values(['PROJECT_CODE', 'CURRENT_YR_ACHIEVEMENTS'], ascending=True)
                          .drop_duplicates('PROJECT_CODE'))


    ListMilesCols = ['NDA_Submission','NDA_Approval','FIH','SOCA',
                     'Pivotal_Program_Start_(PPS)','CS','LD','ESoE','ESoE_Study_Start',
                    'POM','POC','POC_Study_Start','Pivotal_Prep_Investment','Phase_III',
                    'DP3','MAA_Approval','MAA_Submission']
    ALLMilesPivotTable_df = MilesPivotTable(ALLDATA_UNPIVOTED_MILESTONES_df,'PLAN_FINISH','none')
    ALLMilesPivotTable_df.columns = ALLMilesPivotTable_df.columns.str.replace(' ','_')
    AllDATA_PROJECT_df = next_last_milestone(AllDATA_PROJECT_df,ALLMilesPivotTable_df,ListMilesCols,'NEXT')
    AllDATA_PROJECT_df = next_last_milestone(AllDATA_PROJECT_df,ALLMilesPivotTable_df,ListMilesCols,'LAST')

    AllDATA_PROJECT_df['LD_PERCENT'] = AllDATA_PROJECT_df['LD_PERCENT'].fillna(0.0).apply(np.int64)

    AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS']=AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS'].fillna('Null').str.split(',').apply(lambda x : ','.join(set(x))).replace('Null','')

    AllDATA_PROJECT_df = AllDATA_PROJECT_df.drop('RD_PROJ_TYPE',axis=1)
    #AllDATA_PROJECT_df = AllDATA_PROJECT_df.applymap(prep_for_Snowflake_datetime)
# AllDATA_PROJECT_df[AllDATA_PROJECT_df['CURRENT_YR_ACHIEVEMENTS']!='']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
ALLDATA_30DAY_CHANGE_TRACKING_df = ALLDATA_30DAY_CHANGE_TRACKING_df.rename(columns={'MILESTONE_DELTA2':'MILESTONE_DELTA'})
AllDATA_PROJECT_df = AllDATA_PROJECT_df.merge(ALLDATA_30DAY_CHANGE_TRACKING_df,on='PROJECT_CODE',how='left')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
AllDATA_PROJECT_df = AllDATA_PROJECT_df.sort_values(AllDATA_PROJECT_df.columns[0], ascending = True).reset_index(drop=True)
ALLDATA_PROJECT_METADATA = dataiku.Dataset("PORTFOLIO_ALL_DATA").write_from_dataframe(AllDATA_PROJECT_df)
