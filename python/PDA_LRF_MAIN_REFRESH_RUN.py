import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
from datetime import date
from datetime import datetime, timedelta
from dataiku.scenario import Trigger
from SharePointFunctions import read_SPList_2013
curr_date = pd.Timestamp('today').strftime("%Y-%m-%d %H:%M:%S")

SPLists= ['Programs', 'ProgramDetails']

p = dataiku.Project() 
variables = p.get_variables() 

# Read recipe inputs
lrf_SP2013_CONFIG = dataiku.Dataset("LRF_SPLIST_2013_CONFIG")
config_df = lrf_SP2013_CONFIG.get_dataframe()
config_df = config_df[config_df.SharePoint_List_Name.isin(SPLists)]

# reading username and password from global variables
SP_user = dataiku.get_custom_variables()['Username']
SP_password = dataiku.get_custom_variables()['Password']

# itterate through each row of config data
for index, row in config_df.iterrows():
    
    if row['SharePoint_List_Name'] =='Programs': 
        # using custom built function to read sharepoint list and convert to a dataframe
        Programs_SPL_DF = read_SPList_2013(row['SharePoint_Site'], SP_user, SP_password, row['SharePoint_List_Name'])

    elif row['SharePoint_List_Name'] =='ProgramDetails': 
        # using custom built function to read sharepoint list and convert to a dataframe
        ProgramDetails_SPL_DF = read_SPList_2013(row['SharePoint_Site'], SP_user, SP_password, row['SharePoint_List_Name'])


t = Trigger()

Programs_Count = int(variables["standard"]["Programs_Count"])
ProgramDetails_Count = int(variables["standard"]["ProgramDetails_Count"])
Programs_new_count = len(Programs_SPL_DF)
ProgramDetails_new_count = len(ProgramDetails_SPL_DF)

# set variable for last time the data changes checked
variables["standard"]["Last_Checked"] = curr_date 
p.set_variables(variables) 

if (Programs_new_count != Programs_Count) or (ProgramDetails_new_count != ProgramDetails_Count):
    variables["standard"]["Programs_Count"] = Programs_new_count 
    variables["standard"]["ProgramDetails_Count"] = ProgramDetails_new_count 
    p.set_variables(variables)    
    t.fire()
