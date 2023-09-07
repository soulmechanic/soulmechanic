# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
# from SharePoint_Functions import read_SPList
from datetime import date
from datetime import datetime, timedelta
import functools
import json
from WRD_FUNCTIONS import write_to_folder, jsonify, FilterByCode, gen_dict_by_key, gen_list_milestones, prep_for_JSON


# Read recipe inputs
WRD_DASHBOARD_ESI = dataiku.Dataset("WRD_DASHBOARD_STG")
WRD_DASHBOARD_ESI_df = WRD_DASHBOARD_ESI.get_dataframe()

WRD_SPONLINE_CONTRACT_LIST_df = dataiku.Dataset("WRD_SPONLINE_CONTRACT_LIST").get_dataframe(columns=['FEAResearchCode'],na_values=False)
WRD_SPONLINE_CONTRACT_LIST = WRD_SPONLINE_CONTRACT_LIST_df['FEAResearchCode'].to_list()


#PROD_CONTRACT_WRD_SPLIST_DATA = dataiku.Dataset("PROD_CONTRACT_WRD_SPLIST_DATA")
#ProdFEAResearchCode_DF = PROD_CONTRACT_WRD_SPLIST_DATA.get_dataframe()





try:
    '''
    commeted the read SP list as this being generated externally outside the python using default plugin
    Read SP list for getting specific FEAResearch Codes
    FEAResearchCode_DF = read_SPList('https://pfizer.sharepoint.com/','sites/ESIBDDashboard-dev/','Contract')
    FEAResearchCode_DF = (pd.DataFrame(FEAResearchCode_DF['FEAResearchCode'][FEAResearchCode_DF['FEAResearchCode']!=""])
                          .reset_index(drop=True))

    filtering conditions as following:
    1) Based on the Research code there would be a check if there is a project code available.
    2) If no Project code created yet then milestones will be fetched based on Research code.
    3) If Project code is available then milestones will be fetched based on Project Code.

    '''
#     F_WRD_DASHBOARD_ESI_df = FilterByCode(WRD_DASHBOARD_ESI_df,FEAResearchCode_DF)
    F_WRD_DASHBOARD_ESI_df = WRD_DASHBOARD_ESI_df[(WRD_DASHBOARD_ESI_df['Code'].isin(WRD_SPONLINE_CONTRACT_LIST)) |
                                           (WRD_DASHBOARD_ESI_df['DiscoveryFinanceCode'].isin(WRD_SPONLINE_CONTRACT_LIST))]

    # Renaming the columns according as specified in json
    F_WRD_DASHBOARD_ESI_df = F_WRD_DASHBOARD_ESI_df.rename(columns={'MILESTONE_NAME': 'Milestone',
                                                                   'MILESTONE_DISPLAY_NAME':'name',
                                                                   'ACTIVITY_CODE':'core',
                                                                   'DUPLICATE_MILESTONE_DESCRIPTOR':'desc',
                                                                   'PLAN_FINISH':'date',
                                                                   'PCT_COMPLETE':'pcnt',
                                                                   'REGION':'region',
                                                                   'GEM_CURRENT':'gem'}).applymap(prep_for_JSON)
    F_WRD_DASHBOARD_ESI_df[['core','pcnt']] = F_WRD_DASHBOARD_ESI_df[['core','pcnt']].fillna(0.0).astype('int64')

    # generating dictionary of milestone info for each code
    Miles_Values_DF = gen_dict_by_key(F_WRD_DASHBOARD_ESI_df)

    # generating list of milestone names for each code
    Miles_list_DF = gen_list_milestones(F_WRD_DASHBOARD_ESI_df)

    # merging all the dataframes generated previously
    FF_WRD_DASHBOARD_ESI_df = F_WRD_DASHBOARD_ESI_df[['Code','Compound_Name','DiscoveryFinanceCode']].drop_duplicates()
    Final_WRD_DASHBOARD_ESI_df = (functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'),
                                               [FF_WRD_DASHBOARD_ESI_df,Miles_Values_DF, Miles_list_DF]))


    # generating json from dataframe
    WRD_Dashboard_json = jsonify(Final_WRD_DASHBOARD_ESI_df,'PYTHON_RECIPE_WRD_DASHBOARD',
                              'Candidate')

     # publishing json file to S3 folder
    filenameMAIN = 'TEST_WRD_Dashboard_DSS.txt'
    folders = ['DEV_WRD_DASHBOARD_SPONLINE_FOLDER']#,'PROD_WRD_SP_ONLINE']
    write_to_folder(WRD_Dashboard_json,folders,filenameMAIN)
    print('Completed writing WRD Dashboard Json file to SharePoint DocLibrary')
except Exception as e:
    print('Unable to write Json to SP folder:', '->', str(e))
