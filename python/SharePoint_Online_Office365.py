import dataiku
from dataiku import pandasutils as pdu
import pyarrow.parquet as pq
import pandas as pd, numpy as np
import json
import io
from io import BytesIO
import re
from zipfile import ZipFile
import zipfile
from pandas.io.json import json_normalize
import xlrd
from openpyxl import load_workbook
from string import ascii_lowercase
import time
##from SharePointFunctions import auth_sharepoint, clear_SPList

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



def auth_sharepoint(site_url, OLS, xUsername, xPassword):
    try:
        ## supplying the credential for accessing the SharePoint site.
        ## Credentials are save in Global Variables for this project.
        site_url = site_url ## example ** "https://pfizer.sharepoint.com/" ** #sharepoint tenant url
        OLS = OLS ## ** 'sites/PortfolioDev/scratch'** #name of the site

        # xUsername=dataiku.get_custom_variables()["Username"]
        # xPassword=dataiku.get_custom_variables()["Password"]

        ctx = ClientContext(site_url+OLS).with_credentials(UserCredential(xUsername, xPassword))
        return ctx
    except Exception as e:
        print('auth_sharepoint failed, error:', e)
        



    
    
    
def drop_cols(df,allowDefaultColumns):    
    try:
        hidden_cols_list = ['ContentTypeId', '_ModerationComments', 'LinkTitleNoMenu', 'LinkTitle', 'LinkTitle2', 'File_x0020_Type', 'HTML_x0020_File_x0020_Type.File_x0020_Type.mapall', 'HTML_x0020_File_x0020_Type.File_x0020_Type.mapcon', 'HTML_x0020_File_x0020_Type.File_x0020_Type.mapico', 'ComplianceAssetId', 'ID', 'ContentType', 'Modified', 'Modified.', 'Created', 'Created.', 'Author', 'Author.id', 'Author.title', 'Author.span', 'Author.email', 'Author.sip', 'Author.jobTitle', 'Author.department', 'Author.picture', 'Editor', 'Editor.id', 'Editor.title', 'Editor.span', 'Editor.email', 'Editor.sip', 'Editor.jobTitle', 'Editor.department', 'Editor.picture', '_HasCopyDestinations', '_HasCopyDestinations.value', '_CopySource', 'owshiddenversion', 'WorkflowVersion', '_UIVersion', '_UIVersionString', 'Attachments', '_ModerationStatus', '_ModerationStatus.', 'SelectTitle', 'InstanceID', 'Order', 'Order.', 'GUID', 'WorkflowInstanceID', 'FileRef', 'FileRef.urlencode', 'FileRef.urlencodeasurl', 'FileRef.urlencoding', 'FileRef.scriptencodeasurl', 'FileDirRef', 'Last_x0020_Modified', 'Created_x0020_Date', 'Created_x0020_Date.ifnew', 'FSObjType', 'SortBehavior', 'PermMask', 'PrincipalCount', 'FileLeafRef', 'FileLeafRef.Name', 'FileLeafRef.Suffix', 'UniqueId', 'SyncClientId', 'ProgId', 'ScopeId', 'HTML_x0020_File_x0020_Type', '_EditMenuTableStart', '_EditMenuTableStart2', '_EditMenuTableEnd', 'LinkFilenameNoMenu', 'LinkFilename', 'LinkFilename2', 'DocIcon', 'ServerUrl', 'EncodedAbsUrl', 'BaseName', 'MetaInfo', 'MetaInfo.', '_Level', '_IsCurrentVersion', '_IsCurrentVersion.value', 'ItemChildCount', 'FolderChildCount', 'Restricted', 'OriginatorId', 'NoExecute', 'ContentVersion', '_ComplianceFlags', '_ComplianceTag', '_ComplianceTagWrittenTime', '_ComplianceTagUserId', '_IsRecord', 'AccessPolicy', '_VirusStatus', '_VirusVendorID', '_VirusInfo', 'AppAuthor', 'AppEditor', 'SMTotalSize', 'SMLastModifiedDate', 'SMTotalFileStreamSize', 'SMTotalFileCount', '_CommentFlags', '_CommentCount']
        drop_cols_list = list(set(hidden_cols_list) - set(allowDefaultColumns))
        df = df.drop(drop_cols_list,axis=1)
        return df
    except Exception as e:
        print('drop_cols, error:', e)
       
    
# Read Sharepoint list
def create_query():
    qry = CamlQuery()
    qry.ViewXml = f"""<Where><IsNotNull><FieldRef Name="Title" /></IsNotNull></Where>"""
    return qry

def read_SPList(site_url, OLS, SPList, xUsername, xPassword, allowDefaultColumns=[]):
    try:
        ctx = auth_sharepoint(site_url, OLS, xUsername, xPassword)
        target_list = ctx.web.lists.get_by_title(SPList)
        list_qry = create_query()
        result = target_list.render_list_data(list_qry.ViewXml).execute_query()
        data = json.loads(result.value)
        rows = data.get("Row", [])

        _df = pd.DataFrame(rows)
        _df = drop_cols(_df,allowDefaultColumns)
        return _df
    except Exception as e:
        print('reading SharePoint list, error:', e)
        







