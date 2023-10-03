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
from html.parser import HTMLParser
from io import StringIO
import html
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
from office365.sharepoint.files.file import File



def auth_sharepoint(site_url, OLS):
    try:
        ## supplying the credential for accessing the SharePoint site.
        ## Credentials are save in Global Variables for this project.
        site_url = site_url ## example ** "https://pfizer.sharepoint.com/" ** #sharepoint tenant url
        OLS = OLS ## ** 'sites/PortfolioDev/scratch'** #name of the site

        xUsername=dataiku.get_custom_variables()["Username"]
        xPassword=dataiku.get_custom_variables()["Password"]

        ctx = ClientContext(site_url+OLS).with_credentials(UserCredential(xUsername, xPassword))
        return ctx
    except Exception as e:
        print('auth_sharepoint failed, error:', e)
        pass

    
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

# function to clear existing sharepoint list
def clear_SPList(site_url, OLS, SPList):
    # clearing the sharepoint list data based on condition
    ctx = auth_sharepoint(site_url, OLS)
    target_list = ctx.web.lists.get_by_title(SPList)
    items = target_list.items.get().execute_query()
    count = len(items)
    print(count)
    while (count>1):
        for item in items:  # type: ListItem
            item.delete_object()
        ctx.execute_batch()
        count = len(items)
    print("Items left after clearing SharePoint List: {0}".format(len(items)))
    
    
    
def add_items_SPList(SP_Site_Url,ols,SPList,dict_):
    # clearing data in sharepoint list
    clear_SPList(SP_Site_Url, ols, SPList)
    time.sleep(6)
    
    ## adding data to sharepoint list from a dataframe
    sp_list_ctx = auth_sharepoint(SP_Site_Url,ols)
    target_list = sp_list_ctx.web.lists.get_by_title(SPList)
    count = 0
    for row in dict_:
        items_added = target_list.add_item(row)
        #count = count + 1
    sp_list_ctx.execute_batch()
    print(" {0} items created".format(count))
    
    
    
def drop_cols(df,allowDefaultColumns):    
    try:
        hidden_cols_list = ['ServerRedirectedEmbedUrl','_ColorHex', '_ColorTag', '_Emoji','ServerRedirectedEmbedUri','Id','FileSystemObjectType','OData__UIVersionString',
                            'EditorId','AuthorId','ContentTypeId', '_ModerationComments', 'LinkTitleNoMenu', 'LinkTitle', 'LinkTitle2',
                            'File_x0020_Type', 'HTML_x0020_File_x0020_Type.File_x0020_Type.mapall',
                            'HTML_x0020_File_x0020_Type.File_x0020_Type.mapcon', 'HTML_x0020_File_x0020_Type.File_x0020_Type.mapico',
                            'ComplianceAssetId', 'ID', 'ContentType', 'Modified', 'Modified.', 'Created', 'Created.', 'Author', 'Author.id',
                            'Author.title', 'Author.span', 'Author.email', 'Author.sip', 'Author.jobTitle', 'Author.department',
                            'Author.picture', 'Editor', 'Editor.id', 'Editor.title', 'Editor.span', 'Editor.email', 'Editor.sip',
                            'Editor.jobTitle', 'Editor.department', 'Editor.picture', '_HasCopyDestinations', '_HasCopyDestinations.value',
                            '_CopySource', 'owshiddenversion', 'WorkflowVersion', '_UIVersion', '_UIVersionString', 'Attachments',
                            '_ModerationStatus', '_ModerationStatus.', 'SelectTitle', 'InstanceID', 'Order', 'Order.', 'GUID',
                            'WorkflowInstanceID', 'FileRef', 'FileRef.urlencode', 'FileRef.urlencodeasurl', 'FileRef.urlencoding',
                            'FileRef.scriptencodeasurl', 'FileDirRef', 'Last_x0020_Modified', 'Created_x0020_Date', 'Created_x0020_Date.ifnew',
                            'FSObjType', 'SortBehavior', 'PermMask', 'PrincipalCount', 'FileLeafRef', 'FileLeafRef.Name', 'FileLeafRef.Suffix',
                            'UniqueId', 'SyncClientId', 'ProgId', 'ScopeId', 'HTML_x0020_File_x0020_Type', '_EditMenuTableStart',
                            '_EditMenuTableStart2', '_EditMenuTableEnd', 'LinkFilenameNoMenu', 'LinkFilename', 'LinkFilename2', 'DocIcon',
                            'ServerUrl', 'EncodedAbsUrl', 'BaseName', 'MetaInfo', 'MetaInfo.', '_Level', '_IsCurrentVersion',
                            '_IsCurrentVersion.value', 'ItemChildCount', 'FolderChildCount', 'Restricted', 'OriginatorId', 'NoExecute',
                            'ContentVersion', '_ComplianceFlags', '_ComplianceTag', '_ComplianceTagWrittenTime', '_ComplianceTagUserId',
                            '_IsRecord', 'AccessPolicy', '_VirusStatus', '_VirusVendorID', '_VirusInfo', 'AppAuthor', 'AppEditor',
                            'SMTotalSize', 'SMLastModifiedDate', 'SMTotalFileStreamSize', 'SMTotalFileCount', '_CommentFlags', '_CommentCount',
                           'ParentUniqueId', 'OData__ColorTag']
        
        drop_cols_list = list(set(hidden_cols_list) - set(allowDefaultColumns))
        df = df.drop(drop_cols_list,axis=1, errors='ignore')
        return df
    except Exception as e:
        print('drop_cols, error:', e)
        pass
    
# Read Sharepoint list
def create_query():
    qry = CamlQuery()
    qry.ViewXml = f"""<Where><IsNotNull><FieldRef Name="PROJECT_CODE" /></IsNotNull></Where>"""
    return qry

def read_SPList(site_url, OLS, SPList,allowDefaultColumns=[]):
    try:
        ctx = auth_sharepoint_cert(site_url, OLS)    #auth_sharepoint_cert
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
        pass
    

def read_SPList_By_View(site_url, OLS, SPList, allowDefaultColumns=[]):
    try:
    
        ctx = auth_sharepoint_cert(site_url, OLS)   #auth_sharepoint_cert
        list_object = ctx.web.lists.get_by_title(SPList)

        # 1. First request to retrieve views
        view_items = list_object.views.get_by_title("Folder View").get_items()
        ctx.load(view_items)
        ctx.execute_query()

        my_list = []
        for _item in view_items:
            _data = _item.properties
            my_list.append(_data)
        _df = pd.DataFrame(data=my_list)
        _df = drop_cols(_df,allowDefaultColumns)
        return _df
    except Exception as e:
        print('reading SharePoint list, error:', e)
        
# Functions for extracting AEC data

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()

def unescape(rawText):
    s = MLStripper()
    s.feed(html.unescape(rawText))
    return s.get_data()
        
def read_AECList(site_url, OLS, SPList):
    try:
        ctx = auth_sharepoint_cert(site_url, OLS) #auth_sharepoint_cert
        target_list = ctx.web.lists.get_by_title(SPList)

        result = target_list.render_list_data(f"""<View Scope="RecursiveAll"><Query><Where><IsNotNull><FieldRef Name="PROJECT_CODE" /></IsNotNull></Where></Query></View>""").execute_query()
        aecData = json.loads(result.value).get("Row", [])

        #AEC_COLUMN_LIST=['THERAPEUTIC_AREA','RESEARCH_UNIT','PROJECT_CODE','PROJECT_NAME', 'Phase','PRE_POST_POC','CAPABILITIES_ALIGNMENT',
        #                 'UNMET_NEED','BREAKTHROUGH', 'FIRST_MOVER_ADVANTAGE', 'SCIENTIFIC_CONFIDENCE','PEAK','PeakCommentary','TRDC',
        #                 'TRDCCommentary', 'CAPABILITIES_ALIGNMENT_COMMENTAR', 'UNMET_NEED_COMMENTARY','BREAKTHROUGH_COMMENTARY',
        #                 'FIRST_MOVER_ADVANTAGE_COMMENTARY','SCIENTIFIC_CONFIDENCE_COMMENTARY', 'NOTES','Modified','Editor.title']
        
        AEC_COLUMN_LIST=['THERAPEUTIC_AREA','RESEARCH_UNIT','PROJECT_CODE','PROJECT_NAME', 'Phase','PRE_POST_POC', 'CATEGORY_BUILDING_BLOCK',
                         'CAPABILITIES_ALIGNMENT_COMMENTAR', 'UNMET_NEED', 'UNMET_NEED_COMMENTARY','BREAKTHROUGH', 'BREAKTHROUGH_COMMENTARY',
                         'FIRST_MOVER_ADVANTAGE', 'FIRST_MOVER_ADVANTAGE_COMMENTARY','SCIENTIFIC_CONFIDENCE', 'SCIENTIFIC_CONFIDENCE_COMMENTARY',
                         'PEAK','PeakCommentary', 'TRDC', 'TRDCCommentary', 'CAPABILITIES_ALIGNMENT_COMMENTAR', 'NOTES','Modified',
                         'Editor.title']

        AEC_df = pd.DataFrame(aecData)[AEC_COLUMN_LIST]
        AEC_df.rename(columns={'PROJECT_CODE': 'CANDIDATE_CODE','Modified': 'AEC_DATE_MODIFIED','Editor.title': 'AEC_EDITOR','CAPABILITIES_ALIGNMENT_COMMENTAR':'CATEGORY_STRATEGY_COMMENTARY', 'PeakCommentary': 'PEAK_COMMENTARY', 'TRDCCommentary': 'TRDC_COMMENTARY','PRE_POST_POC': 'PPR'}, inplace = True)
        AEC_df['CAPABILITIES_ALIGNMENT_COMMENTARY']  =   AEC_df.apply(lambda x:unescape(x.CAPABILITIES_ALIGNMENT_COMMENTARY) , axis=1)
        AEC_df['UNMET_NEED_COMMENTARY']  =   AEC_df.apply(lambda x:unescape(x.UNMET_NEED_COMMENTARY) , axis=1)
        AEC_df['BREAKTHROUGH_COMMENTARY']  =   AEC_df.apply(lambda x:unescape(x.BREAKTHROUGH_COMMENTARY) , axis=1)
        AEC_df['FIRST_MOVER_ADVANTAGE_COMMENTARY']  =   AEC_df.apply(lambda x:unescape(x.FIRST_MOVER_ADVANTAGE_COMMENTARY) , axis=1)
        AEC_df['SCIENTIFIC_CONFIDENCE_COMMENTARY']  =   AEC_df.apply(lambda x:unescape(x.SCIENTIFIC_CONFIDENCE_COMMENTARY) , axis=1)
        AEC_df['PEAK_COMMENTARY']  =   AEC_df.apply(lambda x:unescape(x.PEAK_COMMENTARY) , axis=1)
        AEC_df['TRDC_COMMENTARY']  =   AEC_df.apply(lambda x:unescape(x.TRDC_COMMENTARY) , axis=1)
        AEC_df['NOTES']  =   AEC_df.apply(lambda x:unescape(x.NOTES) , axis=1)
        return AEC_df
    except Exception as e:
        print('reading SharePoint list, error:', e)
        

def read_doc_lib_files(site_url, OLS, doc_library, folder = None):
    
    """
    This function lets users to read csv or xlsx files from SharePoint document library folder as a pandas dataframe.
    
    Parameters:
    1) site_url - URL of the site
        Eg.: https://pfizer.sharepoint.com/sites/ABC/ABC_SUB/default.aspx , in this link 'https://pfizer.sharepoint.com/' is site_url  
    2) OLS - other part of url which includes the site and subsite (if exists)
        Eg.: https://pfizer.sharepoint.com/sites/ABC/ABC_SUB/default.aspx , in this link 'sites/ABC/ABC_SUB/' is OLS
    3) doc_library - Name of the document library. In most cases, 'Shared Document'. Or you can click on 'Document' on the left panel of
        SharePoint site
    4) folder - Folder name in which the csv or xlsx files are stored.
    """
    #ctx = auth_sharepoint(site_url, OLS)         #FOR USERNAME, PASSWORD
    ctx = auth_sharepoint_cert(site_url, OLS)   #For APP Key
    
    if folder == None or str(folder) == 'nan':
        folder = ''
        
    sp_doc_library_ = doc_library
    folder_ = folder
    target_folder_url = f'{sp_doc_library_}/{folder_}'
    
    root_folder = ctx.web.get_folder_by_server_relative_url(target_folder_url)
    root_folder.expand(["Files", "Folders"]).get().execute_query()
    
    for x in root_folder.files:

        file_response = File.open_binary(ctx, x.properties['ServerRelativeUrl'])
        if x.properties['Name'].endswith(".csv"):
            df = pd.read_csv(BytesIO(file_response.content))
        elif x.properties['Name'].endswith(".xlsx"):
            df = pd.read_excel(BytesIO(file_response.content), engine='openpyxl')
            
    return df