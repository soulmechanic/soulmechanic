
# coding: utf-8

# In[ ]:



import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from pandas.io.json import json_normalize
import io
from io import BytesIO
import re
import json

# Read recipe inputs
CAPTARIO_S3_FOLDER = dataiku.Folder("Md6NxkfD")
CAPTARIO_S3_FOLDER_info = CAPTARIO_S3_FOLDER.get_info()

SIMULATION_DF = dataiku.Dataset("SIMULATION")#.get_dataframe()
SIMULATION_DF = SIMULATION_DF.get_dataframe()

Config_Table_DF = dataiku.Dataset("Config_Table").get_dataframe()
PDA_MV_PROJECT_MASTER_DF = dataiku.Dataset("PDA_MV_PROJECT_MASTER").get_dataframe()


# In[ ]:


# SIMULATION_DF.sort_values(by=['SIMULATION_ID'], ignore_index=True).drop_duplicates(subset=['GUID','DATASET_GUID'],keep='first')


# In[ ]:


def insert_incrIds(Cols_list, InDf, OutDf,max_value=[]):
    for col in Cols_list:
        if InDf[col].max()!=[]:
            for M_value in max_value:
                OutDf.insert(0, col, range(M_value, M_value + len(OutDf)))
        OutDf.insert(0, col, range(InDf[col].max(), InDf[col].max() + len(OutDf)))
    return


# In[ ]:


JsonFileName = 'manifest.json'

with CAPTARIO_S3_FOLDER.get_download_stream(JsonFileName) as f:
    JsonData = json.loads(f.read())
#     JsonData = json.loads(JsonData)
    FlattenJson_0 = pd.json_normalize(JsonData)
    FlattenJson_1 = pd.json_normalize(JsonData['ChildSimulations'])
    FlattenJson_1.columns = FlattenJson_1.columns.str.replace("Properties.", "")

    for i, row in Config_Table_DF.iterrows():
        if row['Input_Table'] == 'INITIATIVE':
            INITIATIVE_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()
            InitID = FlattenJson_0['ModelURL'].str.split('/').str[2]
            if InitID[0] in set(INITIATIVE_DF['GUID']):
#                 MaxUDID = _DF['INITIATIVE_ID'].max()+1
                FlattenJson_0['INITIATIVE_ID'] = INITIATIVE_DF['INITIATIVE_ID'].max()+1
                FlattenJson_0['INITIATIVE_GUID'] = InitID[0]
                FlattenJson_0['INITIATIVE_NAME'] = 'Pfizer Initiative'
                FlattenJson_0['INITIATIVE_CREATED_BY'] = 'CAPTARIO'
                AppendInitDF = FlattenJson_0[['INITIATIVE_ID','INITIATIVE_GUID',
                                             'INITIATIVE_NAME','CreatedDate',
                                             'INITIATIVE_CREATED_BY']]

                AppendInitDF = AppendInitDF.rename(columns={'INITIATIVE_GUID':'GUID',
                                                            'INITIATIVE_NAME':'NAME',
                                                            'CreatedDate':'CREATED_ON',
                                                            'INITIATIVE_CREATED_BY':'CREATED_BY'})
                AppendInitDF['CREATED_ON'] = pd.to_datetime(AppendInitDF['CREATED_ON']).dt.date
                AppendInitDF = pd.concat([INITIATIVE_DF,AppendInitDF], ignore_index=True)
                INIT_OUT_DF = (AppendInitDF.sort_values(by=['INITIATIVE_ID'],ignore_index=True)
                               .drop_duplicates(subset=['GUID','CREATED_ON'],keep='first')).fillna('')
#                 INITIATIVE_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(INIT_OUT_DF)


        if row['Input_Table'] == 'SIMULATION':
            SIMULATION_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()
            SimID = FlattenJson_0.iloc[0]['SimulationId']
            if SimID in set(SIMULATION_DF['GUID']):
#                 MaxUDID = _DF['SIMULATION_ID'].max()+1
                FlattenJson_0['SIMULATION_ID'] = SIMULATION_DF['SIMULATION_ID'].max()+1
                FlattenJson_0['DATASET_VERSION'] = FlattenJson_0['SimulationId'].str.split('_').str[1]
                FlattenJson_0['PARTITION_GUID'] = FlattenJson_0['SimulationId']
                AppendSimDF = FlattenJson_0[['SIMULATION_ID','SimulationId','Label',
                                         'CreatedDate','ModelId','DATASET_VERSION',
                                          'PARTITION_GUID']]
                AppendSimDF = AppendSimDF.rename(columns={'SimulationId':'GUID','Label':'LABEL',
                                                         'CreatedDate':'CREATE_TIME', 'ModelId':'DATASET_GUID'})

                SIM_OUT_DF = pd.concat([SIMULATION_DF,AppendSimDF], ignore_index=True)
                SIM_OUT_DF['DATASET_VERSION'] = SIM_OUT_DF['DATASET_VERSION'].astype(int)
                SIM_OUT_DF = (SIM_OUT_DF.sort_values(by=['SIMULATION_ID'], ignore_index=True)
                              .drop_duplicates(subset=['GUID','DATASET_GUID'],keep='first')).fillna('')
#                 SIMULATION_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(SIM_OUT_DF)
        if row['Input_Table'] == 'PORTFOLIO':
            PORTFOLIO_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()
            ModelID = FlattenJson_0.iloc[0]['ModelId']
            if ModelID in set(PORTFOLIO_DF['GUID']):
#                 MaxUDID = _DF['PORTFOLIO_ID'].max()+1
                FlattenJson_0['PORTFOLIO_ID'] = PORTFOLIO_DF['PORTFOLIO_ID'].max()+1
                FlattenJson_0['CREATOR_ID'] = ''
                AppendPortfDF = FlattenJson_0[['PORTFOLIO_ID','INITIATIVE_ID','ModelId',
                                             'ModelName','Mode','CREATOR_ID','CreatedDate']]
                AppendPortfDF = AppendPortfDF.rename(columns={'ModelName':'NAME','Mode':'MODE',
                                                         'CreatedDate':'CREATE_TIME', 'ModelId':'GUID'})

                PORTF_OUT_DF = pd.concat([PORTFOLIO_DF,AppendPortfDF], ignore_index=True).fillna('')
                PORTF_OUT_DF = (PORTF_OUT_DF.sort_values(by=['PORTFOLIO_ID'], ignore_index=True)
                              .drop_duplicates(subset=['GUID','CREATE_TIME'],keep='first')).fillna('')
#                 PORTFOLIO_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PORTF_OUT_DF)

        if row['Input_Table'] == 'COMPOUND':
            COMPOUND_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()
            List_GUIDs = list(set(COMPOUND_DF['CODE_CAPTARIO']))
            f_FJson_DF = FlattenJson_1[~(FlattenJson_1['Compound Number'].isin(List_GUIDs))]

            f_FJson_DF = (f_FJson_DF[['Candidate Code','Compound Number','Compound Type']]
                          .dropna()
                          .rename(columns={'Candidate Code':'CANDIDATE_CODE','Compound Number':'CODE_CAPTARIO',
                                           'Compound Type':'TYPE'}))
            f_FJson_DF = (pd.merge(f_FJson_DF, PDA_MV_PROJECT_MASTER_DF[['CANDIDATE_CODE','COMPOUND_MECHANISM_OF_ACTION']],
                                   on='CANDIDATE_CODE')
                          .rename(columns={'COMPOUND_MECHANISM_OF_ACTION':'MECHANISM_OF_ACTION'}))
            f_FJson_DF = f_FJson_DF[['CODE_CAPTARIO','TYPE','MECHANISM_OF_ACTION']]
            if len(f_FJson_DF)!=0:

                Cols_list = ['COMPOUND_ID']
                insert_incrIds(Cols_list, COMPOUND_DF, f_FJson_DF)
                COMP_OUT_DF = pd.concat([COMPOUND_DF,f_FJson_DF], ignore_index=True).fillna('')
                COMP_OUT_DF = (COMP_OUT_DF.sort_values(by=['COMPOUND_ID'], ignore_index=True)
                                  .drop_duplicates(subset=['CODE_CAPTARIO'],keep='first')).fillna('')
    #                 COMPOUND_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(COMPOUND_DF)



        if row['Input_Table'] == 'PROJECT':
            PROJECT_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()
            MaxUDID = PROJECT_DF['PROJECT_ID'].max()
            List_GUIDs = list(set(PROJECT_DF['GUID']))
            f_DF = FlattenJson_1[~FlattenJson_1['ParentId'].isin(List_GUIDs)]
            f_DF = (f_DF[['ParentId','ParentName','Project Reporting Name','PDA Representative','Compound Number']]
                    .rename(columns={'ParentId':'GUID','ParentName':'NAME','Project Reporting Name':'REPORTING_NAME',
                                    'PDA Representative':'PDA_REP'}))
#             display(f_DF)
            if len(f_DF)!=0:
                f_DF = pd.merge(f_DF,COMPOUND_DF,left_on='Compound Number',right_on='CODE_CAPTARIO')
                Cols_list = ['PROJECT_ID','CANDIDATE_ID','BUSINESS_CATEGORY_ID']

                insert_incrIds(Cols_list, PROJECT_DF, f_DF)
                display(f_DF)

