import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import sys
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
import json
import requests
from requests_ntlm import HttpNtlmAuth
from shareplum import Site
from shareplum.site import Version
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.http.request_options import RequestOptions
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.listitems.caml.caml_query import CamlQuery
from office365.sharepoint.listitems.listitem import ListItem
from office365.sharepoint.fields.field_creation_information import FieldCreationInformation
from office365.sharepoint.fields.field_multi_user_value import FieldMultiUserValue
from office365.sharepoint.fields.field_type import FieldType
from office365.sharepoint.fields.field_user_value import FieldUserValue
from office365.sharepoint.lists.list_creation_information import ListCreationInformation
from office365.sharepoint.lists.list_template_type import ListTemplateType

def push_JSON_to_sharepoint(base_url,user,pwd,library_path,JSON_data,filename):
    #push JSON to sharepoint
    #TODO: type check inputs 
    try: 
        sharePointUrl = base_url
        Auth=HttpNtlmAuth(user,pwd)
        #Get header values
        tSite=sharePointUrl+'/_api/contextinfo'
        headers= {'accept': 'application/json;odata=verbose'}
        r=requests.post(tSite, auth=Auth, headers=headers)
        form_digest_value = r.json()['d']['GetContextWebInformation']['FormDigestValue']

        #Update header values 
        updated_headers = {
                "Accept":"application/json;odata=verbose",
                "Content-Type":"application/json;odata=verbose",
                 "X-RequestDigest" : form_digest_value
                 }

        requestUrl = base_url + "/_api/Web/GetFolderByServerRelativeUrl('"+ library_path + "')/Files/add(overwrite=true,url='" + filename + "')" 
        tmpstr = JSON_data.encode('utf-8') 
        response=requests.put(requestUrl,data=JSON_data.encode('utf-8'),auth=Auth,headers=updated_headers)

        return response
    except Exception as e:
        return 'error in Sharepoint Upload: ' + e
    
def get_JSON_from_sharepoint(base_url_src,user_src,pwd_src,library_path_src,filename_src):
    try:
        #load From Sharepoint 2013 using requests lib
        sharePointUrl = base_url_src
        Auth_src=HttpNtlmAuth(user_src,pwd_src)
        #Get header values
        Site_src=sharePointUrl+'/_api/contextinfo'
        headers_src= {'accept': 'application/json;odata=verbose'}
        r_src=requests.post(Site_src, auth=Auth_src, headers=headers_src)
        form_digest_value_src = r_src.json()['d']['GetContextWebInformation']['FormDigestValue']

        #Update header values
        updated_headers_src = {
                "Accept":"application/json;odata=verbose",
                "Content-Type":"application/json;odata=verbose",
                 "X-RequestDigest" : form_digest_value_src
                 }

        requestUrl_src = base_url_src + "/_api/Web/GetFolderByServerRelativeUrl('"+ library_path_src + "')/Files('" + filename_src + "')/$value"

        response=requests.get(requestUrl_src,auth=Auth_src,headers=updated_headers_src)
        return response.json()
    
    except Exception as e:
        return 'error in Sharepoint Upload: ' + e
    

def read_Lists_from_SP_to_DSS(url, user, password, list_name, sp_view):
    #read lists from sharepoint lists
    try:
        Auth=HttpNtlmAuth(user,password)
        site = Site(url, auth=Auth)
        sp_list = site.List(list_name)
        #sp_list = list_name.GetListItems('')
        data = sp_list.GetListItems(sp_view);
        data_df = pd.DataFrame(data)
        return data_df
    except Exception as e:
        return 'error in reading from SharePoint: ' + e
    
    

def SPList(dsite,splist):
    ###READ DOWN SHAREPOINT LIST CONTENT AND STORE AS DATAFRAME
    #dynamic read URL is built from site and list inputs
    lSite="http://"+dsite+"_api/lists/getbytitle('{}')/items?$top=5000".format(str(splist))
    #send request with NTLM auth and headers
    r=requests.get(lSite, auth=Auth, headers=Headers)
    #disect json to get results into dataframe
    results = r.json()["d"]["results"]
    df=json_normalize(results)
    df=pd.DataFrame(data=df)
    return df

# read sharepoint list from SharePoint 2013
def read_SPList_2013(url, user, password, list_name):
    #read lists from sharepoint lists
    try:
        Auth=HttpNtlmAuth(user,password)
        site = Site(url, auth=Auth)
        sp_list = site.List(list_name)
        data = sp_list.GetListItems('All Items')
        data_df = pd.DataFrame(data)
        return data_df
    except Exception as e:
        return 'error in reading from SharePoint: ' + e
    


def auth_sharepoint_cert(site_url, OLS):
    try:
        ## Switching to Cerificate based auth, usename/password deprecated
        ## to run this logic, the user needs to have set up an app key/secret in their DSS Profile 'other credentials' list
        ##https://dss-amer-dev.pfizer.com/profile/account/
        ## see https://pfizer.sharepoint.com/sites/AnalyticsWorkspaces/SitePages/Connect-a-SharePoint-List-with-Dataiku-DSS-using-Python.aspx
        site_url = site_url ## example ** "https://pfizer.sharepoint.com/" ** #sharepoint tenant url
        OLS = OLS ## ** 'sites/PortfolioDev/scratch'** #name of the site
        appkey = dataiku.get_custom_variables()["APPKEY"]    
        client = dataiku.api_client()
        auth_info = client.get_auth_info(with_secrets=True)
        
        appSecret = None
        #loop through user creds to find matching key
        for credential in auth_info["secrets"]:
            
            if credential["key"] == appkey:
                
                appSecret = credential["value"]
                
                break

        if not appSecret:
            raise Exception("appSecret not found")
        

        client_credentials = ClientCredential(appkey,appSecret)
       
        ctx = ClientContext(site_url+OLS).with_credentials(client_credentials)
        return ctx
    except Exception as e:
        print('auth_sharepoint_cert failed, error:', e)
        pass
    


def write_files_to_SPOnline(site_url, OLS, SP_DOC_LIBRARY, SP_DOC_LIBRARY_FOLDER, FileName, Json_file):
    try:
        ctx = auth_sharepoint_cert(site_url, OLS)    

        SP_DOC_LIBRARY = SP_DOC_LIBRARY
        SP_DOC_LIBRARY_FOLDER = SP_DOC_LIBRARY_FOLDER

        # Access the SharePoint folder or create the folder if it doesn’t exist
        sp_folder_path ="/{0}/{1}".format(SP_DOC_LIBRARY,SP_DOC_LIBRARY_FOLDER)
        sp_folder  =  ctx.web.ensure_folder_path(sp_folder_path).execute_query()

        # Upload the file to the folder in the SharePoint library
        sp_file = sp_folder.upload_file(FileName, Json_file.encode('utf-8'))
        ctx.execute_query()
    except Exception as e:
        print('Writing file to SharePoint online failed due to error:', e)

# def DSS_TO_SP_FOLDER(url, user, password, list_name, sp_view):
#     #read lists from sharepoint lists
#     try:
#         Auth=HttpNtlmAuth(user,password)
#         site = Site(url, auth=Auth)
#         folder = site.Folder('Shared Documents/This Folder')
# 		folder.upload_file('Hello', 'new.txt')
#         # sp_list = site.List(list_name)
#         # #sp_list = list_name.GetListItems('')
#         # data = sp_list.GetListItems(sp_view);
#         # data_df = pd.DataFrame(data)
#         # return data_df
#     except Exception as e:
#         return 'error in reading from SharePoint: ' + e
         
