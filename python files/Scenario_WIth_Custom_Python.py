
# coding: utf-8

# In[1]:


# -*- coding: utf-8 -*-
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
from datetime import date
from datetime import datetime, timedelta
from dataiku.scenario import Trigger
from SharePointFunctions import read_SPList_2013
from ScenarioFunctions import wait_until_scenario_ends


# In[2]:


# import time

# def wait_until_scenario_ends(scenario_id):
    
#     client = dataiku.api_client()
#     project = client.get_project(dataiku.default_project_key())
#     scenario = project.get_scenario(scenario_id)

#     while True:
#         status = scenario.get_status().get_raw()
#         is_running = status['running']
#         any_not_complete = False
#         if is_running:
#             any_not_complete = True

#         if any_not_complete:
#             print("scenario is still running...")

#         else:
#             print("scenario is completed")
#             return True
    
#         # Wait a bit before checking again
#         time.sleep(30)
        
if wait_until_scenario_ends("PDA_LRF_MAIN_REFRESH_RUN"):
    print('done')


# In[ ]:


# Read recipe inputs
dummy = dataiku.Dataset("dummy")
dummy_df = dummy.get_dataframe()


# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a Pandas dataframe
# NB: DSS also supports other kinds of APIs for reading and writing data. Please see doc.

dummy_test_df = dummy_df # For this sample code, simply copy input to output


# Write recipe outputs
dummy_test = dataiku.Dataset("dummy_test")
dummy_test.write_with_schema(dummy_test_df)

