import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
from dataiku.scenario import Trigger
from datetime import date
from datetime import datetime, timedelta
from spfunctions import read_SPList_2013

SP_URL = 'http://ecfd13.pfizer.com/sites/ATCC/ERD/'
# 86400
# reading username and password from global variables
SP_user = dataiku.get_custom_variables()['SHAREPOINT_ACCOUNT']
SP_password = dataiku.get_custom_variables()['SHAREPOINT_ACCOUNT_PWD']

SPL_DF = read_SPList_2013(SP_URL, SP_user, SP_password,'Annual_OP_Plan_Schedule')

curr_date = pd.Timestamp('today').strftime("%Y-%m-%d")

t = Trigger()

ANNUAL_OP_PLAN_DS = dataiku.Dataset('ANNUAL_OP_PLAN')
PList = ANNUAL_OP_PLAN_DS.list_partitions()
if not any(item == curr_date for item in PList):

    if not SPL_DF[(SPL_DF['Scenario_Run_Date']==curr_date) & (SPL_DF['Active']=='Yes')].empty: # your condition here
        t.fire()
