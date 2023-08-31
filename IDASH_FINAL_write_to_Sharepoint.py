# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import json
import io
from io import BytesIO
import re
from datetime import datetime, date


#Project Libs
from spfunctions import push_JSON_to_sharepoint , read_Lists_from_SP_to_DSS, write_files_to_SPOnline
from IDASHFunctions import write_to_folder

# first day of the month
first_day_of_month = date.today().replace(day=1)

# Read recipe inputs
IDASH_JSON_FILES_FOLDER = dataiku.Folder("IDASH_JSON_FILES_FOLDER")
IDASH_JSON_FILES_FOLDER_info = IDASH_JSON_FILES_FOLDER.get_info()

DevJsonFiles = IDASH_JSON_FILES_FOLDER.list_paths_in_partition()

# Read recipe inputs
PROD_IDASH_JSON_FILES_FOLDER = dataiku.Folder("PROD_IDASH_JSON_FILES_FOLDER")
PROD_IDASH_JSON_FILES_FOLDER_info = PROD_IDASH_JSON_FILES_FOLDER.get_info()

ProdJsonFiles = PROD_IDASH_JSON_FILES_FOLDER.list_paths_in_partition()

# SharePoint Config Data
SharePoint_Config = dataiku.Dataset("SharePoint_Config")
SP_Config_df = SharePoint_Config.get_dataframe()

# GETTING PROJECT VARIABLES
## CREDENTIALS TO ACCESS THE SHAREPOINT SITE
user = dataiku.get_custom_variables()["SHAREPOINT_ACCOUNT"]
password = dataiku.get_custom_variables()["SHAREPOINT_ACCOUNT_PWD"]

NEWSFLOW_SHAREPOINT_SITE = dataiku.get_custom_variables()["NEWSFLOW_SHAREPOINT_SITE"]
NEWSFLOW_LIBRARY_PATH = dataiku.get_custom_variables()["NEWSFLOW_LIBRARY_PATH"]

DEV_NEWSFLOW_SHAREPOINT_SITE = dataiku.get_custom_variables()["DEV_NEWSFLOW_SHAREPOINT_SITE"]
DEV_NEWSFLOW_LIBRARY_PATH = dataiku.get_custom_variables()["DEV_NEWSFLOW_LIBRARY_PATH"]

PROD_SHAREPOINT_SITE = dataiku.get_custom_variables()["PROD_SHAREPOINT_SITE"]
PROD_LIBRARY_PATH = dataiku.get_custom_variables()["PROD_LIBRARY_PATH"]

DEV_SHAREPOINT_SITE = dataiku.get_custom_variables()["DEV_SHAREPOINT_SITE"]
DEV_LIBRARY_PATH = dataiku.get_custom_variables()["DEV_LIBRARY_PATH"]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Publishing Json files to SharePoint Online and ecfd 2013 Site

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def dfCol_toList(df,Cond_Col,Filterd_Col='Json_File_Name'):
    if df[Cond_Col].notnull().any():
        JsonList = df.loc[df[Cond_Col].str.lower()=='yes',Filterd_Col].tolist()
        return JsonList

def read_and_clean_json(json_file, folder):
    with folder.get_download_stream(json_file) as f:
        Json_Data = f.read().decode('utf8')

        return Json_Data

def write_DevSharePoint(JsonFiles, JsonList, OutputType):
    JsonFiles = [re.sub(r'/', '', file) for file in JsonFiles]
    Monthly_Snap = '/Monthly_Snapshot_of_JsonFiles'
    if OutputType == 'Dev_SharePoint_2013':
        FList = list(set(JsonFiles).intersection(JsonList))
        for JFile in FList:
            Json_data = read_and_clean_json(JFile, IDASH_JSON_FILES_FOLDER)

            #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
            resultMain = push_JSON_to_sharepoint(DEV_SHAREPOINT_SITE, user, password, DEV_LIBRARY_PATH,
                                                     Json_data, JFile)
            print('Completed writing the file '+JFile+' to IDASH Dev 2013 SharePoint')

            if date.today() == first_day_of_month:
                DateJFile = str(first_day_of_month)+'_'+JFile
                #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
                resultMain = push_JSON_to_sharepoint(DEV_SHAREPOINT_SITE, user, password, DEV_LIBRARY_PATH+Monthly_Snap,
                                                     Json_data, DateJFile)
                print('Completed writing the file '+DateJFile+' to IDASH Dev 2013 SharePoint')


    elif OutputType=='Dev_SharePoint_Online':
        FList = list(set(JsonFiles).intersection(JsonList))
        for JFile in FList:
            Json_data = read_and_clean_json(JFile, IDASH_JSON_FILES_FOLDER)

            # push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
            write_to_folder(Json_data,['DEV_IDASH_SHAREPOINT_ONLINE'],JFile)

            print('Completed writing the file '+JFile+' to IDASH Dev Online SharePoint')

            if date.today() == first_day_of_month:
                DateJFile = str(first_day_of_month)+'_'+JFile
                # push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
                write_to_folder(Json_data,['DEV_IDASH_SHAREPOINT_ONLINE_MSNAP'],DateJFile)

                print('Completed writing the file '+DateJFile+' to IDASH Dev Online SharePoint')
    
    elif OutputType =='Dev_NewsFlow_SharePoint':
        FList = list(set(JsonFiles).intersection(JsonList))
        for JFile in FList:
            Json_data = read_and_clean_json(JFile, IDASH_JSON_FILES_FOLDER)

            #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
            resultMain = push_JSON_to_sharepoint(DEV_NEWSFLOW_SHAREPOINT_SITE, user, password, DEV_NEWSFLOW_LIBRARY_PATH,
                                                 Json_data, JFile)
            print('Completed writing the file '+JFile+' to DEV NewsFlow 2013 SharePoint')

            if date.today() == first_day_of_month:
                DateJFile = str(first_day_of_month)+'_'+JFile
                #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
                resultMain = push_JSON_to_sharepoint(DEV_NEWSFLOW_SHAREPOINT_SITE, user, password, DEV_NEWSFLOW_LIBRARY_PATH+Monthly_Snap,
                                                 Json_data, DateJFile)
                print('Completed writing the file '+DateJFile+' to DEV NewsFlow 2013 SharePoint')

    else:
        print('No files to write to SharePoint')


def write_ProdSharePoint(JsonFiles, JsonList, OutputType):
    JsonFiles = [re.sub(r'/', '', file) for file in JsonFiles]
    Monthly_Snap = '/Monthly_Snapshot_of_JsonFiles'
    if OutputType == 'Prod_SharePoint_2013':
        FList = list(set(JsonFiles).intersection(JsonList))
        for JFile in FList:
            Json_data = read_and_clean_json(JFile, PROD_IDASH_JSON_FILES_FOLDER)

            #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
            resultMain = push_JSON_to_sharepoint(PROD_SHAREPOINT_SITE, user, password, PROD_LIBRARY_PATH,
                                                     Json_data, JFile)
            print('Completed writing the file '+JFile+' to IDASH Prod 2013 SharePoint')

            if date.today() == first_day_of_month:
                DateJFile = str(first_day_of_month)+'_'+JFile
                #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
                resultMain = push_JSON_to_sharepoint(PROD_SHAREPOINT_SITE, user, password, PROD_LIBRARY_PATH+Monthly_Snap,
                                                     Json_data, DateJFile)
                print('Completed writing the file '+DateJFile+' to IDASH Prod 2013 SharePoint')

    # The Prod Sharepoint Online
    elif OutputType=='Prod_SharePoint_Online':
        FList = list(set(JsonFiles).intersection(JsonList))
        for JFile in FList:
            Json_data = read_and_clean_json(JFile, PROD_IDASH_JSON_FILES_FOLDER)

            # push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
            write_to_folder(Json_data,['PROD_IDASH_SHAREPOINT_ONLINE'],JFile)

            print('Completed writing the file '+JFile+' to IDASH Prod Online SharePoint')

            if date.today() == first_day_of_month:
                DateJFile = str(first_day_of_month)+'_'+JFile
                # push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
                write_to_folder(Json_data,['PROD_IDASH_SP_ONLINE_MSNAP'],DateJFile)

                print('Completed writing the file '+DateJFile+' to IDASH Prod Online SharePoint')

    elif OutputType =='NewsFlow_SharePoint':
        FList = list(set(JsonFiles).intersection(JsonList))
        for JFile in FList:
            Json_data = read_and_clean_json(JFile, PROD_IDASH_JSON_FILES_FOLDER)

            #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
            resultMain = push_JSON_to_sharepoint(NEWSFLOW_SHAREPOINT_SITE, user, password, NEWSFLOW_LIBRARY_PATH,
                                                 Json_data, JFile)
            print('Completed writing the file '+JFile+' to NewsFlow 2013 SharePoint')

            if date.today() == first_day_of_month:
                DateJFile = str(first_day_of_month)+'_'+JFile
                #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
                resultMain = push_JSON_to_sharepoint(NEWSFLOW_SHAREPOINT_SITE, user, password, NEWSFLOW_LIBRARY_PATH+Monthly_Snap,
                                                 Json_data, DateJFile)
                print('Completed writing the file '+DateJFile+' to NewsFlow 2013 SharePoint')

    else:
        print('No files to write to SharePoint')





F_SP_Config_df = SP_Config_df.copy()
F_SP_Config_df.dropna(how='all', axis=1, inplace=True)
SharePoint_List = list(F_SP_Config_df.filter(like='SharePoint').columns)

for SP_Site in SharePoint_List:
    print(SP_Site)
    write_DevSharePoint(DevJsonFiles, dfCol_toList(SP_Config_df,SP_Site), SP_Site)
    write_ProdSharePoint(ProdJsonFiles, dfCol_toList(SP_Config_df,SP_Site), SP_Site)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# site_url = "https://pfizer.sharepoint.com/"
# OLS = "sites/PortfolioDev/idashTest"
# SP_DOC_LIBRARY = 'assets'
# SP_DOC_LIBRARY_FOLDER = 'data'
# write_files_to_SPOnline(site_url, OLS, SP_DOC_LIBRARY, SP_DOC_LIBRARY_FOLDER, 'Test_File_1212', Json_data)
