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

PROGRAM_OPTION_DF = dataiku.Dataset("PROGRAM_OPTION_OUT").get_dataframe()

INITIATIVE_DF = dataiku.Dataset("INITIATIVE_OUT").get_dataframe()

PROJECT_DF = dataiku.Dataset("PROJECT_OUT").get_dataframe()

SIMULATION_DF = dataiku.Dataset("SIMULATION_OUT").get_dataframe()

PORTFOLIO_DF = dataiku.Dataset("PORTFOLIO_OUT").get_dataframe()

Config_Table_DF = dataiku.Dataset("Config_Table").get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def latestZipFilePath(S3):
    if S3.get_path_details()['children']:
        MaxUpFilePath = [(max(item['fullPath'] for item in S3.get_path_details()['children']
                      if item['lastModified']==max(item['lastModified']
                                                   for item in S3.get_path_details()['children'])))]
        return MaxUpFilePath


# the main function is added to python library, this function is modified according to this wrokflow.
def load_file_from_zip(FileName,S3Folder):
    try:
        FilePaths = latestZipFilePath(S3Folder)
        if FilePaths:
            for FilePath in FilePaths:
                print(FilePaths)
                if FilePath.endswith('.zip'):
                    with S3Folder.get_download_stream(FilePath) as f:
                        z = zipfile.ZipFile(io.BytesIO(f.read()))
                        out = z.open(FileName)
                        JsonFile = json.load(out)
                return JsonFile
    except Exception as e:
        print('Failed at load_file_from_zip', e)


# flatten json file by position to level rquired recursively
def Json_normalise_by_position(JsonData, list_dict_cols=[]):
    try:
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
    except Exception as e:
            print('Failed at Json_normalise_by_position', e)

# generate incremental user generated ids to be assigned to GUIDs
def insert_incrIds(Cols_list, InDf, OutDf,max_value=[]):
    try:
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
    except Exception as e:
            print('Failed at insert_incrIds', e)

# joins multiple dataframes
def multi_df_merge(MainDF, key_cols, join_tables, how='left'):
    try:
        for keyCol,joinDF in zip(key_cols,join_tables):
            col_id = keyCol.replace('GUID','ID')
            if not joinDF.empty:
                joinDF = joinDF[['GUID',col_id]]
                joinDF = joinDF.rename(columns={'GUID':keyCol})
                MainDF = pd.merge(MainDF,joinDF,on = keyCol,how=how)
        return MainDF
    except Exception as e:
            print('Failed merge data', e)


def PortfolioProject(F_PP_DF, TableName):
    try:
        F_PP_DF = PP_DF[['INITIATIVE_GUID','PORTFOLIO_GUID','PROJECT_GUID','PROGRAM_OPTION_GUID','SIMULATION_GUID']]

        # check if the filtered portfolio projects dataframe has any data else skip
        if not F_PP_DF.empty:
            key_cols = ['INITIATIVE_GUID','PORTFOLIO_GUID','PROJECT_GUID','PROGRAM_OPTION_GUID','SIMULATION_GUID']
            join_tables = [INITIATIVE_DF,PORTFOLIO_DF,PROJECT_DF,PROGRAM_OPTION_DF,SIMULATION_DF]
            F_PP_DF = multi_df_merge(F_PP_DF, key_cols, join_tables)


            PP_CONC_DF = (pd.concat([PORTFOLIO_PROJECTS_DF,F_PP_DF])
                                .sort_values(by=['PORTFOLIO_PROJECTS_ID'],ignore_index=True)
                               .drop_duplicates(subset=key_cols,keep='first').reset_index(drop=True)).fillna(0)

            PP_CONC_DF['PORTFOLIO_PROJECTS_ID'] =PP_CONC_DF['PORTFOLIO_PROJECTS_ID'].astype(int)
            PP_CONC_DF = PP_CONC_DF.loc[PP_CONC_DF['PORTFOLIO_PROJECTS_ID'].isin([0])].drop('PORTFOLIO_PROJECTS_ID',axis=1)

            if not any (pd.concat([PORTFOLIO_PROJECTS_DF[key_cols],PP_CONC_DF[key_cols]]).duplicated()):

                if PORTFOLIO_PROJECTS_DF.empty:
                    # adding incremental id user generated in ascending order
                    Cols_list = ['PORTFOLIO_PROJECTS_ID']
                    insert_incrIds(Cols_list, PORTFOLIO_PROJECTS_DF, PP_CONC_DF,max_value=[100000])
                    print('The inupt table:' + row['Input_Table']+ ' is Empty')

                if not PORTFOLIO_PROJECTS_DF.empty:
                    # adding incremental id user generated in ascending order
                    Cols_list = ['PORTFOLIO_PROJECTS_ID']
                    insert_incrIds(Cols_list, PORTFOLIO_PROJECTS_DF, PP_CONC_DF)
                    print('The inupt table:' + row['Input_Table']+ ' is not empty')



                PORT_PROJ_OUT_DF = (pd.concat([PORTFOLIO_PROJECTS_DF,PP_CONC_DF]).reset_index(drop=True))
                ListOfIDCol = ['PORTFOLIO_PROJECTS_ID','INITIATIVE_ID','PORTFOLIO_ID','PROJECT_ID','PROGRAM_OPTION_ID','SIMULATION_ID']
                PORT_PROJ_OUT_DF[ListOfIDCol] =PORT_PROJ_OUT_DF[ListOfIDCol].fillna(0.0).apply(np.int64)

                return PORT_PROJ_OUT_DF
            else:
                print("Data already loaded in "+ TableName + " Table")

        else:
            print("Completed writing data to "+ TableName + " Table")

    except Exception as e:
        print('Failed while writing data to ' + TableName + " Table", e)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
JsonFileName = 'manifest.json'


# read the manifest json
JsonData = load_file_from_zip(JsonFileName,CAPTARIO_S3_BUCKET)


if JsonData:

    # itterate through the config table, which provides table wise details
    for i, row in Config_Table_DF.iterrows():

        try:
            # look for PORTFOLIO_PROJECTS table
            if row['Input_Table'] == 'PORTFOLIO_PROJECTS':

                # reading data from table
                PORTFOLIO_PROJECTS_DF = dataiku.Dataset(row['Input_Table']).get_dataframe()

                if not JsonData['SimulationId'] in set(PORTFOLIO_PROJECTS_DF['SIMULATION_GUID']):

                    if len(JsonData['ChildSimulations'])!=0:
                        # using custom function flatten json upto ChildSimulations level
                        PP_DF = Json_normalise_by_position(JsonData, ['ChildSimulations'])

                        # check if these columns present in manifest json
                        port_proj_ColsList = ['SimulationId','ModelId','ChildSimulations_ModelId','ChildSimulations_ParentId']
                        if all(item in PP_DF.columns for item in port_proj_ColsList):

                            PP_DF['INITIATIVE_GUID'] = PP_DF['ModelURL'].str.split('/').str[2]
                            PP_DF = (PP_DF.rename(columns={'SimulationId':'SIMULATION_GUID','ModelId':'PORTFOLIO_GUID',
                                                          'ChildSimulations_ModelId':'PROGRAM_OPTION_GUID',
                                                           'ChildSimulations_ParentId':'PROJECT_GUID'}))
                            PORT_PROJ_OUT_DF = PortfolioProject(PP_DF, row['Output_Table'])

                            # loading data to table
                            PORT_PROJ_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PORT_PROJ_OUT_DF)
                        else:
                            print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")

                    else:
                        # flatten json
                        PP_DF = pd.json_normalize(JsonData)
                        # check if these columns present in manifest json
                        port_proj_ColsList = ['SimulationId','ModelId','ParentId']
                        if all(item in PP_DF.columns for item in port_proj_ColsList):

                            PP_DF['INITIATIVE_GUID'] = PP_DF['ModelURL'].str.split('/').str[2]
                            PP_DF = (PP_DF.rename(columns={'SimulationId':'SIMULATION_GUID','ModelId':'PROGRAM_OPTION_GUID'
                                                          , 'ParentId':'PROJECT_GUID'}))
                            PP_DF['PORTFOLIO_GUID'] = PP_DF['PROJECT_GUID']

                            PORT_PROJ_OUT_DF = PortfolioProject(PP_DF, row['Output_Table'])

                            # loading data to table
                            PORT_PROJ_OUT = dataiku.Dataset(row['Output_Table']).write_with_schema(PORT_PROJ_OUT_DF)
                        else:
                            print("Required attribute not present in Manifest.json file for "+ row['Output_Table'] + " Table")
                else:
                    print("This SimulationId: "+JsonData['SimulationId']+" is already Processed for "+ row['Output_Table'] + " Table")
        except Exception as e:
            print('Failed while writing data to ' + row['Output_Table'] + " Table", e)
            
else:
    print("No manifest.json present in S3 Bucket")