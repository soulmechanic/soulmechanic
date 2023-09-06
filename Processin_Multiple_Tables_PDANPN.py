# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from pandas.io.json import json_normalize
import io
from io import BytesIO
import re
import json
import zipfile
from zipfile import ZipFile

# Read recipe inputs
CAPTARIO_S3_BUCKET = dataiku.Folder("wD8wbeOz")
CAPTARIO_S3_BUCKET_info = CAPTARIO_S3_BUCKET.get_info()

SIMULATION_DF = dataiku.Dataset("SIMULATION")#.get_dataframe()
SIMULATION_DF = SIMULATION_DF.get_dataframe()

Config_Table_DF = dataiku.Dataset("Config_Table").get_dataframe()
PDA_STG_OS_CANDIDATE_PORTFOLIO_DF = dataiku.Dataset("PDA_STG_OS_CANDIDATE_PORTFOLIO").get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def latestZipFilePath(S3):
    if S3.get_path_details()['children']:
        MaxUpFilePath = [(max(item['fullPath'] for item in S3.get_path_details()['children']
                      if item['lastModified']==max(item['lastModified']
                                                   for item in S3.get_path_details()['children'])))]
        return MaxUpFilePath

# the main function is added to python library, this function is modified according to this wrokflow.
def load_file_from_zip(FileName,S3Folder):

    FilePaths = latestZipFilePath(S3Folder)
    if FilePaths:
        print(FilePaths)
        for FilePath in FilePaths:
            if FilePath.endswith('.zip'):
                with S3Folder.get_download_stream(FilePath) as f:
                    z = zipfile.ZipFile(io.BytesIO(f.read()))
                    out = z.open(FileName)
                    JsonFile = json.load(out)
            return JsonFile

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# flatten json file by position to level rquired recursively
def Json_normalise_by_position(JsonData, list_dict_cols):
    # normalize events
    df = pd.json_normalize(JsonData)

    # explode all columns with lists of dicts
    df = df.apply(lambda x: x.explode()).reset_index(drop=True)

    # # list of columns with dicts
    cols_to_normalize = list_dict_cols

    # if there are keys, which will become column names, overlap with excising column names
    # add the current column name as a prefix
    normalized = list()
    for col in cols_to_normalize:

        d = pd.json_normalize(df[col], sep='_')
        d.columns = [f'{col}_{v}' for v in d.columns]
        normalized.append(d.copy())

    # combine df with the normalized columns
    df = pd.concat([df] + normalized, axis=1).drop(columns=cols_to_normalize)
    return df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# generate incremental user generated ids to be assigned to GUIDs
def insert_incrIds(Cols_list, InDf, OutDf,max_value=[]):
    # itterate through columns in the list of columns specified
    for col in Cols_list:

        # if the max ids mentioned
        if max_value:
    #             for M_value in max_value:
            M_value = int(max_value[0])

            # adding incremental id user generated in ascending order
            OutDf.insert(0, col, range(M_value, M_value + len(OutDf)))
            return OutDf

        # if no max ids are mentioned
        MAXID = int(InDf[col].max()+1)

        # adding incremental id user generated in ascending order
        OutDf.insert(0, col, range(MAXID, MAXID + len(OutDf)))
    return OutDf

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
JsonFileName = 'manifest.json'

# read the manifest json
JsonData = load_file_from_zip(JsonFileName,CAPTARIO_S3_BUCKET)

if JsonData:

    # unnest the json and flatten as a dataframe or table like stucture
    # flatten to level 0 of the nested json
    FlattenJson_0 = pd.json_normalize(JsonData)
    FlattenJson_0['CREATE_TIME'] = JsonData['CreatedDate']
    # flatten the column with dict utilizing the previously flettend dataframe
    FlattenJson_1 = pd.json_normalize(JsonData['ChildSimulations'])
    FlattenJson_1.columns = FlattenJson_1.columns.str.replace("Properties.", "")
    FlattenJson_1['CREATE_TIME'] = JsonData['CreatedDate']

    if len(JsonData['ChildSimulations'])!= 0:

        try:
            # check if these columns present in manifest json
            sim_ColsList = ['SimulationId']
            if all(item in FlattenJson_0.columns for item in sim_ColsList):
                # extracting the unique id value
                LatestSimID = FlattenJson_0.iloc[0]['SimulationId']
        except Exception as e:
                print('Failed to get Latest Simulation ID:', e)

        # itterate through the config table, which provides table wise details
        for i, row in Config_Table_DF.iterrows():

            try:

                # look for Initiative table
                if row['Input_Table'] == 'INITIATIVE':

                    # reading data from table
                    INITIATIVE_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # extracting the unique id value
                    InitID = FlattenJson_0['ModelURL'].str.split('/').str[2]
                    Initiative = FlattenJson_0.iloc[0]['Initiative']

                    # check if these columns present in manifest json
                    init_ColsList = ['CreatedDate']
                    if all(item in FlattenJson_0.columns for item in init_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # check if not the value already exists or skip
                        if not InitID[0] in set(INITIATIVE_DF['GUID']):
                            print("The INITIATIVE ID: "+InitID[0])

                            # locate the values in manifest json and assign it according to required columns in Initiative table

                            if INITIATIVE_DF.empty:
                                FlattenJson_0['INITIATIVE_ID'] = 1000
                            else:
                                FlattenJson_0['INITIATIVE_ID'] = INITIATIVE_DF['INITIATIVE_ID'].max()+1


                            FlattenJson_0['INITIATIVE_GUID'] = InitID[0]
                            FlattenJson_0['INITIATIVE_NAME'] = Initiative
                            FlattenJson_0['INITIATIVE_CREATED_BY'] = 'CAPTARIO'
                            AppendInitDF = FlattenJson_0[['INITIATIVE_ID','INITIATIVE_GUID',
                                                         'INITIATIVE_NAME','CreatedDate',
                                                         'INITIATIVE_CREATED_BY']]

                            AppendInitDF = AppendInitDF.rename(columns={'INITIATIVE_GUID':'GUID',
                                                                        'INITIATIVE_NAME':'NAME',
                                                                        'CreatedDate':'CREATED_ON',
                                                                        'INITIATIVE_CREATED_BY':'CREATED_BY'})
                            AppendInitDF['CREATED_ON'] = pd.to_datetime(AppendInitDF['CREATED_ON']).dt.date

                            # append the data extracted from manifest json on to current working table data
                            AppendInitDF = pd.concat([INITIATIVE_DF,AppendInitDF], ignore_index=True)

                            # check for any duplicates in the data
                            INIT_OUT_DF = (AppendInitDF.sort_values(by=['INITIATIVE_ID'],ignore_index=True)
                                           .drop_duplicates(subset=['GUID'],keep='first')).fillna('')

                            INIT_OUT_DF['CREATED_ON'] = pd.to_datetime(INIT_OUT_DF['CREATED_ON'],utc=True)

                            # loading data to table
                            INITIATIVE_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(INIT_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            print("The Initiative ID: "+InitID[0]+" is Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

            try:
                # look for SIMULATION table
                if row['Input_Table'] == 'SIMULATION':

                    # reading data from table
                    SIMULATION_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    sim_ColsList = ['SimulationId','Label','CreatedDate','ModelId']
                    if all(item in FlattenJson_0.columns for item in sim_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id value
                        SimID = FlattenJson_0.iloc[0]['SimulationId']

                        # check if not the value already exists or skip
                        if not SimID in set(SIMULATION_DF['GUID']):
                            print("The SIMULATION ID: "+SimID)

                            # locate the values in manifest json and assign it according to required columns in SIMULATION table
                            if SIMULATION_DF.empty:
                                FlattenJson_0['SIMULATION_ID'] = 1000
                            else:
                                FlattenJson_0['SIMULATION_ID'] = SIMULATION_DF['SIMULATION_ID'].max()+1

                            FlattenJson_0['DATASET_VERSION'] = FlattenJson_0['SimulationId'].str.split('_').str[1]
                            FlattenJson_0['PARTITION_GUID'] = FlattenJson_0['SimulationId']
                            AppendSimDF = FlattenJson_0[['SIMULATION_ID','SimulationId','Label',
                                                     'CreatedDate','ModelId','DATASET_VERSION',
                                                      'PARTITION_GUID']]
                            AppendSimDF = AppendSimDF.rename(columns={'SimulationId':'GUID','Label':'LABEL',
                                                                     'CreatedDate':'CREATE_TIME', 'ModelId':'DATASET_GUID'})

                            # append the data extracted from manifest json on to current working table data
                            SIM_OUT_DF = pd.concat([SIMULATION_DF,AppendSimDF], ignore_index=True)
                            SIM_OUT_DF['DATASET_VERSION'] = SIM_OUT_DF['DATASET_VERSION'].astype(int)

                            # check for any duplicates in the data
                            SIM_OUT_DF = (SIM_OUT_DF.sort_values(by=['SIMULATION_ID'], ignore_index=True)
                                          .drop_duplicates(subset=['GUID','LABEL'],keep='first')).fillna('')
                            SIM_OUT_DF['CREATE_TIME'] = pd.to_datetime(SIM_OUT_DF['CREATE_TIME'],utc=True)

                            # loading data to table
                            SIMULATION_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(SIM_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            print("The SIMULATION ID: "+SimID+" is Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

            try:
                # look for PORTFOLIO table
                if row['Input_Table'] == 'PORTFOLIO':

                    # reading data from table
                    PORTFOLIO_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    port_ColsList = ['ModelName','Mode','CreatedDate','ModelId']
                    if all(item in FlattenJson_0.columns for item in port_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id value
                        ModelID = FlattenJson_0.iloc[0]['ModelId']
                        InitID = FlattenJson_0['ModelURL'].str.split('/').str[2]


                        # check if not the value already exists or skip
                        if not ModelID in set(PORTFOLIO_DF['GUID']):
                            print("The PORTFOLIO ID: "+ModelID)


                            # locate the values in manifest json and assign it according to required columns in PORTFOLIO table
                            if PORTFOLIO_DF.empty:
                                FlattenJson_0['PORTFOLIO_ID'] = 100000
                            else:
                                FlattenJson_0['PORTFOLIO_ID'] = PORTFOLIO_DF['PORTFOLIO_ID'].max()+1

                            FlattenJson_0['CREATOR_ID'] = 0
                            FlattenJson_0['LATEST_SIMULATION_GUID'] = LatestSimID
                            AppendPortfDF = FlattenJson_0[['PORTFOLIO_ID','ModelId',
                                                         'ModelName','Mode','CREATOR_ID','CreatedDate','LATEST_SIMULATION_GUID']]
                            AppendPortfDF = AppendPortfDF.rename(columns={'ModelName':'NAME','Mode':'MODE',
                                                                     'CreatedDate':'CREATE_TIME', 'ModelId':'GUID'})
                            AppendPortfDF['INITIATIVE_ID'] = FlattenJson_0['INITIATIVE_ID']

                            # append the data extracted from manifest json on to current working table data
                            PORTF_OUT_DF = pd.concat([PORTFOLIO_DF,AppendPortfDF], ignore_index=True).fillna('')

                            # check for any duplicates in the data
                            PORTF_OUT_DF = (PORTF_OUT_DF.sort_values(by=['PORTFOLIO_ID'], ignore_index=True)
                                          .drop_duplicates(subset=['GUID'],keep='first')).fillna('')
                            PORTF_OUT_DF['CREATE_TIME'] = pd.to_datetime(PORTF_OUT_DF['CREATE_TIME'],utc=True)
                            PORTF_OUT_DF['INITIATIVE_ID'] = PORTF_OUT_DF['INITIATIVE_ID'].fillna(0.0).apply(np.int64)

                            # loading data to table
                            PORTFOLIO_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PORTF_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            PORTFOLIO_DF.loc[(PORTFOLIO_DF["GUID"].str.contains(ModelID)), "LATEST_SIMULATION_GUID"] = LatestSimID
                            # loading data to table
                            PORTFOLIO_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PORTFOLIO_DF)
                            print("The PORTFOLIO ID: "+ModelID+" is Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

            try:
                # look for COMPOUND table
                if row['Input_Table'] == 'COMPOUND':

                    # reading data from table
                    COMPOUND_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    comp_ColsList = ['Compound Number','Candidate Code','Compound Type']
                    if all(item in FlattenJson_1.columns for item in comp_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id values and filter out if these values already exist in the table
                        List_GUIDs = list(set(COMPOUND_DF['CODE_CAPTARIO']))
                        f_FJson_DF = FlattenJson_1[~(FlattenJson_1['Compound Number'].isin(List_GUIDs))]

                        # locate the values in manifest json and assign it according to required columns in COMPOUND table
                        f_FJson_DF = (f_FJson_DF[['Candidate Code','Compound Number','Compound Type']]
                                      .dropna()
                                      .rename(columns={'Candidate Code':'CANDIDATE_CODE','Compound Number':'CODE_CAPTARIO',
                                                       'Compound Type':'TYPE'}))
                        f_FJson_DF = (pd.merge(f_FJson_DF, PDA_STG_OS_CANDIDATE_PORTFOLIO_DF,
                                               on='CANDIDATE_CODE')
                                      .rename(columns={'COMPOUND_MECHANISM_OF_ACTION':'MECHANISM_OF_ACTION'}))
                        f_FJson_DF = f_FJson_DF[['CODE_CAPTARIO','TYPE','MECHANISM_OF_ACTION']]

                        # Check and go further only if there are any new compound numbers else skip
                        if len(f_FJson_DF)!=0:

                            # adding incremental id user generated in ascending order
                            Cols_list = ['COMPOUND_ID']
                            insert_incrIds(Cols_list, COMPOUND_DF, f_FJson_DF)

                            # append the data extracted from manifest json on to current working table data
                            COMP_OUT_DF = pd.concat([COMPOUND_DF,f_FJson_DF], ignore_index=True).fillna('')

                            # check for any duplicates in the data
                            COMP_OUT_DF = (COMP_OUT_DF.sort_values(by=['COMPOUND_ID'], ignore_index=True)
                                              .drop_duplicates(subset=['CODE_CAPTARIO'],keep='first')).fillna('')

                            # loading data to table
                            COMPOUND_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(COMPOUND_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            print("The COMPOUND IDs are Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)


            try:
                # look for PROJECT table
                if row['Input_Table'] == 'PROJECT':

                    # reading data from table
                    PROJECT_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    proj_ColsList = ['ParentId','ParentName','Project Reporting Name','PDA Representative',
                                      'Business Category','Candidate Code']
                    if all(item in FlattenJson_1.columns for item in proj_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id values and filter out if these values already exist in the table
                        MaxUDID = PROJECT_DF['PROJECT_ID'].max()
                        List_GUIDs = list(set(PROJECT_DF['GUID']))

                        f_DF = FlattenJson_1[~FlattenJson_1['ParentId'].isin(List_GUIDs)]



                        if not 'Compound Number' in f_DF:
                            f_DF['Compound Number'] =''

                        # locate the values in manifest json and assign it according to required columns in PROJECT table
                        f_DF = (f_DF[['ParentId','ParentName','Project Reporting Name','PDA Representative','Compound Number',
                                      'Business Category','Candidate Code']]
                                .rename(columns={'ParentId':'GUID','ParentName':'NAME','Project Reporting Name':'REPORTING_NAME',
                                                'PDA Representative':'PDA_REP','Compound Number':'CODE_CAPTARIO'
                                                ,'Candidate Code':'CODE'})).reset_index(drop=True)

                        # Check and go further only if there are any new project ids else skip
                        if not f_DF.empty:

                            # reading data from table
                            BUSINESS_CAT_DF = dataiku.Dataset('BUSINESS_CATEGORY').get_dataframe()
                            CANDIDATE_DF = dataiku.Dataset('CANDIDATE').get_dataframe()
                            COMPOUND_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                            f_DF['LATEST_SIMULATION_GUID'] = LatestSimID
                            f_DF['CURRENT_PHASE'] = "NA"


                            # merge compound and business category tables to get compound_id and business_category_id
                            if not f_DF['CODE_CAPTARIO'].isnull().values.all():
                                print('Code_captario is null')
    #                             f_DF = pd.merge(f_DF,COMPOUND_DF[['CODE_CAPTARIO','COMPOUND_ID']],on='CODE_CAPTARIO',how='left')

                            f_DF =(pd.merge(f_DF,CANDIDATE_DF[['CODE','CANDIDATE_ID']],on='CODE',how='left'))
                            f_DF = (pd.merge(f_DF,BUSINESS_CAT_DF[['NAME','BUSINESS_CATEGORY_ID']],
                                            left_on='Business Category',right_on='NAME',how='left',
                                             suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
                                    .drop(columns=(['CODE_CAPTARIO','Business Category','CODE'])))


                            # adding incremental id user generated in ascending order
                            if PROJECT_DF.empty:
                                Cols_list = ['PROJECT_ID']
                                insert_incrIds(Cols_list, PROJECT_DF, f_DF,max_value=['100000'])


                            if not PROJECT_DF.empty:

                                Cols_list = ['PROJECT_ID']
                                insert_incrIds(Cols_list, PROJECT_DF, f_DF)



                            # append the data extracted from manifest json on to current working table data
                            PROJ_OUT_DF = pd.concat([PROJECT_DF,f_DF], ignore_index=True)#.fillna('')

                            # check for any duplicates in the data
                            PROJ_OUT_DF = (PROJ_OUT_DF.sort_values(by=['PROJECT_ID'], ignore_index=True)
                                              .drop_duplicates(subset=['GUID'],keep='first'))#.fillna('')

                            PROJ_OUT_DF[['CANDIDATE_ID','COMPOUND_ID']] = PROJ_OUT_DF[['CANDIDATE_ID','COMPOUND_ID']].fillna(0.0).apply(np.int64)

                            PROJ_OUT_DF['CURRENT_PHASE'] = PROJ_OUT_DF['CURRENT_PHASE'].astype(str)

                            # loading data to table
                            PROJ_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PROJ_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            FList_GUIDs = list(set(FlattenJson_1['ParentId']))
                            PROJECT_DF.loc[(PROJECT_DF["GUID"].isin(FList_GUIDs)), "LATEST_SIMULATION_GUID"] = LatestSimID
                            # loading data to table
                            PROJ_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PROJECT_DF)
                            print("The PROJECT IDs are Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

            try:
                # look for COMPOUND table
                if row['Input_Table'] == 'PROGRAM_OPTION':

                    # reading data from table
                    PROGRAM_OPTION_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    PO_ColsList = ['ModelId','ModelName','CREATE_TIME']
                    if all(item in FlattenJson_1.columns for item in PO_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id values and filter out if these values already exist in the table
                        List_GUIDs = list(set(PROGRAM_OPTION_DF['GUID']))
                        f_PO_DF = FlattenJson_1[~(FlattenJson_1['ModelId'].isin(List_GUIDs))]
                        # locate the values in manifest json and assign it according to required columns in PROJECT table
                        f_PO_DF = (f_PO_DF[['ModelId','ModelName','CREATE_TIME']]
                                .rename(columns={'ModelId':'GUID','ModelName':'NAME'})).reset_index(drop=True)


                        # Check and go further only if there are any new project ids else skip
                        if not f_PO_DF.empty:

                            # adding incremental id user generated in ascending order
                            if PROGRAM_OPTION_DF.empty:
                                Cols_list = ['PROGRAM_OPTION_ID']
                                insert_incrIds(Cols_list, PROGRAM_OPTION_DF, f_PO_DF,max_value=['100000'])


                            if not PROGRAM_OPTION_DF.empty:

                                Cols_list = ['PROGRAM_OPTION_ID']
                                insert_incrIds(Cols_list, PROGRAM_OPTION_DF, f_PO_DF)


                            # append the data extracted from manifest json on to current working table data
                            PO_OUT_DF = pd.concat([PROGRAM_OPTION_DF,f_PO_DF], ignore_index=True).fillna('')

                            # check for any duplicates in the data
                            PO_OUT_DF = (PO_OUT_DF.sort_values(by=['PROGRAM_OPTION_ID'], ignore_index=True)
                                              .drop_duplicates(subset=['GUID'],keep='first')).fillna('')
                            PO_OUT_DF['CREATE_TIME'] = pd.to_datetime(PO_OUT_DF['CREATE_TIME'],utc=True)

                            # loading data to table
                            PO_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PO_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            print("The PROGRAM OPTION IDs are Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")

            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)
    else:

        try:
            # check if these columns present in manifest json
            sim_ColsList = ['SimulationId']
            if all(item in FlattenJson_0.columns for item in sim_ColsList):
                # extracting the unique id value
                LatestSimID = FlattenJson_0.iloc[0]['SimulationId']
        except Exception as e:
                print('Failed to get Latest Simulation ID:', e)

        FlattenJson_0.columns = FlattenJson_0.columns.str.replace("Properties.", "")

        # itterate through the config table, which provides table wise details
        for i, row in Config_Table_DF.iterrows():

            try:

                # look for Initiative table
                if row['Input_Table'] == 'INITIATIVE':

                    # reading data from table
                    INITIATIVE_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # extracting the unique id value
                    InitID = FlattenJson_0['ModelURL'].str.split('/').str[2]
                    Initiative = FlattenJson_0.iloc[0]['Initiative']

                    # check if these columns present in manifest json
                    init_ColsList = ['CreatedDate']
                    if all(item in FlattenJson_0.columns for item in init_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # check if not the value already exists or skip
                        if not InitID[0] in set(INITIATIVE_DF['GUID']):
                            print("The INITIATIVE ID: "+InitID[0])

                            # locate the values in manifest json and assign it according to required columns in Initiative table
                            if INITIATIVE_DF.empty:
                                FlattenJson_0['INITIATIVE_ID'] = 1000
                            else:
                                FlattenJson_0['INITIATIVE_ID'] = INITIATIVE_DF['INITIATIVE_ID'].max()+1
                            FlattenJson_0['INITIATIVE_GUID'] = InitID[0]
                            FlattenJson_0['INITIATIVE_NAME'] = Initiative
                            FlattenJson_0['INITIATIVE_CREATED_BY'] = 'CAPTARIO'
                            AppendInitDF = FlattenJson_0[['INITIATIVE_ID','INITIATIVE_GUID',
                                                         'INITIATIVE_NAME','CreatedDate',
                                                         'INITIATIVE_CREATED_BY']]

                            AppendInitDF = AppendInitDF.rename(columns={'INITIATIVE_GUID':'GUID',
                                                                        'INITIATIVE_NAME':'NAME',
                                                                        'CreatedDate':'CREATED_ON',
                                                                        'INITIATIVE_CREATED_BY':'CREATED_BY'})
                            AppendInitDF['CREATED_ON'] = pd.to_datetime(AppendInitDF['CREATED_ON']).dt.date

                            # append the data extracted from manifest json on to current working table data
                            AppendInitDF = pd.concat([INITIATIVE_DF,AppendInitDF], ignore_index=True)

                            # check for any duplicates in the data
                            INIT_OUT_DF = (AppendInitDF.sort_values(by=['INITIATIVE_ID'],ignore_index=True)
                                           .drop_duplicates(subset=['GUID'],keep='first')).fillna('')

                            INIT_OUT_DF['CREATED_ON'] = pd.to_datetime(INIT_OUT_DF['CREATED_ON'],utc=True)
        #                     display(INIT_OUT_DF)
                            # loading data to table
                            INITIATIVE_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(INIT_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            print("The Initiative ID: "+InitID[0]+" is Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

            try:
                # look for SIMULATION table
                if row['Input_Table'] == 'SIMULATION':

                    # reading data from table
                    SIMULATION_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    sim_ColsList = ['SimulationId','Label','CreatedDate','ModelId']
                    if all(item in FlattenJson_0.columns for item in sim_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id value
                        SimID = FlattenJson_0.iloc[0]['SimulationId']

                        # check if not the value already exists or skip
                        if not SimID in set(SIMULATION_DF['GUID']):
                            print("The SIMULATION ID: "+SimID)

                            # locate the values in manifest json and assign it according to required columns in SIMULATION table
                            if SIMULATION_DF.empty:
                                FlattenJson_0['SIMULATION_ID'] = 1000
                            else:
                                FlattenJson_0['SIMULATION_ID'] = SIMULATION_DF['SIMULATION_ID'].max()+1
                            FlattenJson_0['DATASET_VERSION'] = FlattenJson_0['SimulationId'].str.split('_').str[1]
                            FlattenJson_0['PARTITION_GUID'] = FlattenJson_0['SimulationId']
                            AppendSimDF = FlattenJson_0[['SIMULATION_ID','SimulationId','Label',
                                                     'CreatedDate','ModelId','DATASET_VERSION',
                                                      'PARTITION_GUID']]
                            AppendSimDF = AppendSimDF.rename(columns={'SimulationId':'GUID','Label':'LABEL',
                                                                     'CreatedDate':'CREATE_TIME', 'ModelId':'DATASET_GUID'})

                            # append the data extracted from manifest json on to current working table data
                            SIM_OUT_DF = pd.concat([SIMULATION_DF,AppendSimDF], ignore_index=True)
                            SIM_OUT_DF['DATASET_VERSION'] = SIM_OUT_DF['DATASET_VERSION'].astype(int)

                            # check for any duplicates in the data
                            SIM_OUT_DF = (SIM_OUT_DF.sort_values(by=['SIMULATION_ID'], ignore_index=True)
                                          .drop_duplicates(subset=['GUID','LABEL'],keep='first')).fillna('')
                            SIM_OUT_DF['CREATE_TIME'] = pd.to_datetime(SIM_OUT_DF['CREATE_TIME'],utc=True)
        #                     display(SIM_OUT_DF)
                            # loading data to table
                            SIMULATION_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(SIM_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            print("The SIMULATION ID: "+SimID+" is Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

            try:
                # look for PORTFOLIO table
                if row['Input_Table'] == 'PORTFOLIO':

                    # reading data from table
                    PORTFOLIO_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    port_ColsList = ['ModelName','Mode','CreatedDate','ParentId']
                    if all(item in FlattenJson_0.columns for item in port_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id value
                        ModelID = FlattenJson_0.iloc[0]['ModelId']
                        InitID = FlattenJson_0['ModelURL'].str.split('/').str[2]


                        # check if not the value already exists or skip
                        if not ModelID in set(PORTFOLIO_DF['GUID']):
                            print("The PORTFOLIO ID: "+ModelID)


                            # locate the values in manifest json and assign it according to required columns in PORTFOLIO table
                            FlattenJson_0['PORTFOLIO_ID'] = PORTFOLIO_DF['PORTFOLIO_ID'].max()+1
                            FlattenJson_0['CREATOR_ID'] = 0
                            FlattenJson_0['LATEST_SIMULATION_GUID'] = LatestSimID
                            AppendPortfDF = FlattenJson_0[['PORTFOLIO_ID','ParentId',
                                                         'ModelName','Mode','CREATOR_ID','CreatedDate','LATEST_SIMULATION_GUID']]
                            AppendPortfDF = AppendPortfDF.rename(columns={'ModelName':'NAME','Mode':'MODE',
                                                                     'CreatedDate':'CREATE_TIME', 'ParentId':'GUID'})
                            AppendPortfDF['INITIATIVE_ID'] = FlattenJson_0['INITIATIVE_ID']

                            # append the data extracted from manifest json on to current working table data
                            PORTF_OUT_DF = pd.concat([PORTFOLIO_DF,AppendPortfDF], ignore_index=True).fillna('')

                            # check for any duplicates in the data
                            PORTF_OUT_DF = (PORTF_OUT_DF.sort_values(by=['PORTFOLIO_ID'], ignore_index=True)
                                          .drop_duplicates(subset=['GUID'],keep='first')).fillna('')
                            PORTF_OUT_DF['CREATE_TIME'] = pd.to_datetime(PORTF_OUT_DF['CREATE_TIME'],utc=True)

        #                     display(PORTF_OUT_DF)


                            # loading data to table
                            PORTFOLIO_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PORTF_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            PORTFOLIO_DF.loc[(PORTFOLIO_DF["GUID"].str.contains(ModelID)), "LATEST_SIMULATION_GUID"] = LatestSimID
                            # loading data to table
                            PORTFOLIO_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PORTFOLIO_DF)
                            print("The PORTFOLIO ID: "+ModelID+" is Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

            try:
                # look for COMPOUND table
                if row['Input_Table'] == 'COMPOUND':

                    # reading data from table
                    COMPOUND_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    comp_ColsList = ['Compound Number','Candidate Code','Compound Type']
                    if all(item in FlattenJson_0.columns for item in comp_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id values and filter out if these values already exist in the table
                        List_GUIDs = list(set(COMPOUND_DF['CODE_CAPTARIO']))
                        f_FJson_DF = FlattenJson_1[~(FlattenJson_1['Compound Number'].isin(List_GUIDs))]

                        # locate the values in manifest json and assign it according to required columns in COMPOUND table
                        f_FJson_DF = (f_FJson_DF[['Candidate Code','Compound Number','Compound Type']]
                                      .dropna()
                                      .rename(columns={'Candidate Code':'CANDIDATE_CODE','Compound Number':'CODE_CAPTARIO',
                                                       'Compound Type':'TYPE'}))
                        f_FJson_DF = (pd.merge(f_FJson_DF, PDA_STG_OS_CANDIDATE_PORTFOLIO_DF,
                                               on='CANDIDATE_CODE')
                                      .rename(columns={'COMPOUND_MECHANISM_OF_ACTION':'MECHANISM_OF_ACTION'}))
                        f_FJson_DF = f_FJson_DF[['CODE_CAPTARIO','TYPE','MECHANISM_OF_ACTION']]

                        # Check and go further only if there are any new compound numbers else skip
                        if len(f_FJson_DF)!=0:

                            # adding incremental id user generated in ascending order
                            Cols_list = ['COMPOUND_ID']
                            insert_incrIds(Cols_list, COMPOUND_DF, f_FJson_DF)

                            # append the data extracted from manifest json on to current working table data
                            COMP_OUT_DF = pd.concat([COMPOUND_DF,f_FJson_DF], ignore_index=True).fillna('')

                            # check for any duplicates in the data
                            COMP_OUT_DF = (COMP_OUT_DF.sort_values(by=['COMPOUND_ID'], ignore_index=True)
                                              .drop_duplicates(subset=['CODE_CAPTARIO'],keep='first')).fillna('')
        #                     display(COMP_OUT_DF)

                            # loading data to table
                            COMPOUND_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(COMPOUND_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            print("The COMPOUND IDs are Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)


            try:
                # look for PROJECT table
                if row['Input_Table'] == 'PROJECT':

                    # reading data from table
                    PROJECT_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    proj_ColsList = ['ParentId','ParentName','Project Reporting Name',
                                      'Business Category']
                    if all(item in FlattenJson_0.columns for item in proj_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id values and filter out if these values already exist in the table
                        MaxUDID = PROJECT_DF['PROJECT_ID'].max()
                        List_GUIDs = list(set(PROJECT_DF['GUID']))
                        f_DF = FlattenJson_0[~FlattenJson_0['ParentId'].isin(List_GUIDs)]


                        if not 'Compound Number' in f_DF:
                            f_DF['Compound Number'] =''
                        if not 'PDA Representative' in f_DF:
                            f_DF['PDA Representative'] =''

                        # locate the values in manifest json and assign it according to required columns in PROJECT table
                        f_DF = (f_DF[['ParentId','ParentName','Project Reporting Name','PDA Representative','Compound Number',
                                      'Business Category']]
                                .rename(columns={'ParentId':'GUID','ParentName':'NAME','Project Reporting Name':'REPORTING_NAME',
                                                'PDA Representative':'PDA_REP','Compound Number':'CODE_CAPTARIO'
                                                ,'Candidate Code':'CODE'})).reset_index(drop=True)

                        # Check and go further only if there are any new project ids else skip
                        if not f_DF.empty:

                            # reading data from table
                            BUSINESS_CAT_DF = dataiku.Dataset('BUSINESS_CATEGORY').get_dataframe()
                            COMPOUND_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()
                            CANDIDATE_DF = dataiku.Dataset('CANDIDATE').get_dataframe()

                            f_DF['LATEST_SIMULATION_GUID'] = LatestSimID
                            f_DF['CURRENT_PHASE'] = None

                            # merge compound and business category tables to get compound_id and business_category_id
                            if not f_DF['CODE_CAPTARIO'].isnull().values.all():
                                print('Code_captario is null')
        #                         f_DF = pd.merge(f_DF,COMPOUND_DF[['CODE_CAPTARIO','COMPOUND_ID']],on='CODE_CAPTARIO',how='left')
                            f_DF =(pd.merge(f_DF,CANDIDATE_DF[['CODE','CANDIDATE_ID']],on='CODE',how='left'))
                            f_DF = (pd.merge(f_DF,BUSINESS_CAT_DF[['NAME','BUSINESS_CATEGORY_ID']],
                                            left_on='Business Category',right_on='NAME',how='left',
                                             suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
                                    .drop(columns=(['CODE_CAPTARIO','Business Category','CODE'])))

                             # adding incremental id user generated in ascending order
                            if PROJECT_DF.empty:

                                Cols_list = ['PROJECT_ID']
                                insert_incrIds(Cols_list, PROJECT_DF, f_DF,max_value=[100000])

                            if not PROJECT_DF.empty:

                                Cols_list = ['PROJECT_ID']
                                insert_incrIds(Cols_list, PROJECT_DF, f_DF)

                            # append the data extracted from manifest json on to current working table data
                            PROJ_OUT_DF = pd.concat([PROJECT_DF,f_DF], ignore_index=True)#.fillna('')

                            # check for any duplicates in the data
                            PROJ_OUT_DF = (PROJ_OUT_DF.sort_values(by=['PROJECT_ID'], ignore_index=True)
                                              .drop_duplicates(subset=['GUID'],keep='first'))#.fillna('')
                            PROJ_OUT_DF[['CANDIDATE_ID','COMPOUND_ID']] = PROJ_OUT_DF[['CANDIDATE_ID','COMPOUND_ID']].fillna(0.0).apply(np.int64)
                            PROJ_OUT_DF['CURRENT_PHASE'] = PROJ_OUT_DF['CURRENT_PHASE'].astype(str)

                            # loading data to table
                            PROJ_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PROJ_OUT_DF)

                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            FList_GUIDs = list(set(FlattenJson_0['ParentId']))
                            PROJECT_DF.loc[(PROJECT_DF["GUID"].isin(FList_GUIDs)), "LATEST_SIMULATION_GUID"] = LatestSimID
                            # loading data to table
                            PROJ_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PROJECT_DF)
                            print("The PROJECT IDs are Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

            try:
                # look for COMPOUND table
                if row['Input_Table'] == 'PROGRAM_OPTION':

                    # reading data from table
                    PROGRAM_OPTION_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                    # check if these columns present in manifest json
                    PO_ColsList = ['ModelId','ModelName','CREATE_TIME']
                    if all(item in FlattenJson_0.columns for item in PO_ColsList):
                        print("Required attribute are present in Manifest.json file for "+ row['Output_Table'] + " Table")

                        # extracting the unique id values and filter out if these values already exist in the table
                        List_GUIDs = list(set(PROGRAM_OPTION_DF['GUID']))
                        f_PO_DF = FlattenJson_0[~(FlattenJson_0['ModelId'].isin(List_GUIDs))]
                        # locate the values in manifest json and assign it according to required columns in PROJECT table
                        f_PO_DF = (f_PO_DF[['ModelId','ModelName','CREATE_TIME']]
                                .rename(columns={'ModelId':'GUID','ModelName':'NAME'})).reset_index(drop=True)


                        # Check and go further only if there are any new project ids else skip
                        if not f_PO_DF.empty:

                            # adding incremental id user generated in ascending order
                            Cols_list = ['PROGRAM_OPTION_ID']
                            insert_incrIds(Cols_list, PROGRAM_OPTION_DF, f_PO_DF)

                            # append the data extracted from manifest json on to current working table data
                            PO_OUT_DF = pd.concat([PROGRAM_OPTION_DF,f_PO_DF], ignore_index=True).fillna('')

                            # check for any duplicates in the data
                            PO_OUT_DF = (PO_OUT_DF.sort_values(by=['PROGRAM_OPTION_ID'], ignore_index=True)
                                              .drop_duplicates(subset=['GUID'],keep='first')).fillna('')
                            PO_OUT_DF['CREATE_TIME'] = pd.to_datetime(PO_OUT_DF['CREATE_TIME'],utc=True)

                            # loading data to table
                            PO_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PO_OUT_DF)


                            print("Completed writing data to "+ row['Output_Table'] + " Table")
                        else:
                            print("The PROGRAM OPTION IDs are Already Loaded to "+ row['Output_Table'] + " Table")
                    else:
                        print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")

            except Exception as e:
                print('Failed while writing data to ' + row['Output_Table'] + " Table", e)

else:
    print("No manifest.json present in S3 Bucket")