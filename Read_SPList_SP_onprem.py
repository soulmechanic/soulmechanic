# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import requests
from requests_ntlm import HttpNtlmAuth
from shareplum import Site
from shareplum.site import Version


def read_Lists_from_SP_to_DSS(url, user, password, list_name):
    #read lists from sharepoint lists
    try:
        Auth=HttpNtlmAuth(user,password)
        site = Site(url, auth=Auth)
        sp_list = site.List(list_name)
        data = sp_list.GetListItems('All Items');
        data_df = pd.DataFrame(data)
        return data_df
    except Exception as e:
        return 'error in reading from SharePoint: ' + e


user = 'SRVAMR-EPIC'
password = 'xNXCVL8rrUP7BbuPWrb9sM030'
SP_url = 'http://ecfd13.pfizer.com/sites/EPIC-dev'
SP_list_name = 'Country_Region_List'


country_region_df = read_Lists_from_SP_to_DSS(SP_url, user, password, SP_list_name)

country_region_df