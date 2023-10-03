
# coding: utf-8

# In[1]:


# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from SharePointFunctions import auth_sharepoint, clear_SPList, add_items_SPList, drop_cols, read_SPList, read_SPList_By_View

curr_datetime = pd.Timestamp('today').strftime("%Y-%m-%d %H:%M:%S")

# Read recipe inputs
Covid_CI_Formatted_Excel = dataiku.Dataset("Covid_CI_Formatted_Excel")
Covid_CI_Formatted_Excel_df = Covid_CI_Formatted_Excel.get_dataframe()

# Read recipe inputs
COVID_CI_Config = dataiku.Dataset("COVID_CI_Config")
COVID_CI_Config_df = COVID_CI_Config.get_dataframe()


# In[2]:


# -------------------------------------------------------------------------------- 
def write_sharepoint_data_to_Snowflake(config_df):
    #reading the config data and drop empty rows
    config_df = config_df.copy()
    try:
        # itterate through each row of config data
        for index, row in config_df.iterrows():
            # using custom built function to read sharepoint list with views and convert to a dataframe
            SPL_DF = read_SPList(row['SharePoint_Site'], row['OLS'],row['SharePoint_List_Name'])
            # remove special character
            SPL_DF.columns = SPL_DF.columns.str.replace('_x0020', '')
            SPL_DF['Run_Date'] = curr_datetime
            SPL_DF = (SPL_DF.drop(['Last_Modified.','Created_Date.','ParentUniqueId','_ComplianceTagWrittenTime.','SMLastModifiedDate.'],axis=1)
                      .rename(columns={'Title':row['Title_Column_Name']}).reset_index(drop=True))

            # writing each sharepoint list converted dataframe to snowflake table according to table names in config
            output_to_snowflake = dataiku.Dataset(row['Snowflake_Table_Name'])

            output_to_snowflake.write_with_schema(SPL_DF)
        print("{0} out {1} Snowflake tables created".format(index + 1, config_df.shape[0]))
        return
    except Exception as e:
        print("write_sharepoint_data_to_Snowflake, error:", e)
        pass

# -------------------------------------------------------------------------------- 
write_sharepoint_data_to_Snowflake(COVID_CI_Config_df)


# In[3]:


# # Intial used for Formatting the excel file.

# Covid_CI_DF = Covid_CI_Formatted_Excel_df.melt(id_vars=["PRIMARY_GROUPING_L1", "PRIMARY_GROUPING_L2", "PRIMARY GROUPING_L3"],
#         var_name="Details",
#         value_name="Value")

# Covid_CI_DF[['COMPOUND_TYPE', 'MANUFACTURER', 'BRAND']] = Covid_CI_DF['Details'].str.split('|', expand=True)
# Covid_CI_DF

# Covid_CI_DF = Covid_CI_DF[['MANUFACTURER','BRAND','COMPOUND_TYPE','PRIMARY_GROUPING_L1','PRIMARY_GROUPING_L2','PRIMARY GROUPING_L3','Value']]

