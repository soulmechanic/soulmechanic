def reorder_dataframe_columns(snowflake_connection, table_name, schema, df):
    """ Reorder columns in a Pandas DataFrame based on the order of columns in a Snowflake table.

        :param snowflake_connection: A valid Snowflake connection object
        :param table_name: The name of the Snowflake table to use as reference
        :param schema: The name of the Snowflake schema to use as refrence
        :param df: The Pandas DataFrame to reorder columns
        :return: A new Pandas DataFrame with reordered columns
        """
    try:
        # Get the column names from the Snowflake table
        result = snowflake_connection.cursor().execute(f"DESCRIBE TABLE {schema}.{table_name}")
        columns = [row[0] for row in result]
        
        df.columns =  [x.upper() for x in df.columns]
        

        # Check if any columns are missing in the DataFrame
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"The following columns are missing in the DataFrame: {missing_columns}")

        # Reorder the columns in the DataFrame
        df = df[columns]

        return df
    except Exception as e:
        raise ValueError(f"An error occurred while reordering columns: {e}")



def archive_files_from_folder(source_folder, target_folder, archive_subfolders):
    
    source_folder = Folder(source_folder)
    target_folder = Folder(target_folder)
       
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Locate file in source S3 folder which is older than current date
    files = source_folder.list_paths_in_partition()
    
    previous_day_files = [file for file in files if datetime.fromtimestamp(source_folder.get_path_details(file)['lastModified']/1000).strftime('%Y-%m-%d') > current_date]
    
    for subfolder_name in archive_subfolders:
        list_of_csv_files = [file for file in previous_day_files if subfolder_name in file and file.endswith(".csv") ]
        
        # Move that located file to target S3 folder
        zip_file_name = f'{current_date}_{subfolder_name}.zip'
        
        # Create a new zip file
        zip_file = zipfile.ZipFile(zip_file_name, "w")

        # Add each CSV file to the zip file
        for file in list_of_csv_files:
            if file.endswith(".csv"):
                with source_folder.get_download_stream(file) as stream:
                    csv_file_name = os.path.basename(file)
                    zip_file.writestr(csv_file_name, stream.read())

        # Close the zip file
        zip_file.close()

        with open(zip_file_name, "rb") as f:
            target_folder.upload_stream('/'+subfolder_name+'/'+zip_file_name, f)
            
        # List the contents of the folder
        contents = target_folder.list_paths_in_partition()
        
        x=0
        # Check if the zip file exists in target folder before deleting files in source folder
        if '/'+subfolder_name+'/'+zip_file_name in contents:
            for to_be_deleted_csv_file in list_of_csv_files:
                source_folder.clear_path(to_be_deleted_csv_file)
            print("File exists")
        else:
            print("File does not exist")
    return 

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import snowflake.connector
from datetime import datetime
import pytz
from pytz import timezone
import re

def connect_to_snowflake(schemaname):
    from snowflake.connector import connect
    # connect to snowflake database and send out connection object
    # for use in loading data to snowflake tables or
    # selecting data from snowflake tables
    user_secrets = dataiku.api_client().get_auth_info(with_secrets=True)["secrets"]
    Snowflake_Account = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Account")['value']
    Snowflake_User = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_User")['value']
    Snowflake_Password = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Password")['value']
    Snowflake_Role = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Role")['value']
    Snowflake_DB = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_DB")['value']
    Snowflake_Warehouse = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Warehouse")['value']

    conn = snowflake.connector.connect(
        user=Snowflake_User,
        password=Snowflake_Password,
        account=Snowflake_Account,
        role=Snowflake_Role,
        warehouse = Snowflake_Warehouse,
        database = Snowflake_DB,
        schema = schemaname
    )

    conn.cursor()
    cur = conn.cursor()

    return conn

def execute_sqlstmt(conn,sqlstmt):
    #this function is to execute adhoc sql statement in the SQL Connection
    # this will return dataframe for select statement
    # for other DML statements it will send out status
    # ensure your sql statements does not get very large volumn of data
    conn.cursor()
    cur = conn.cursor()
    try:
        sqlstmt = sqlstmt
        if (sqlstmt.startswith("SELECT")):
            dataframe = pd.read_sql(sqlstmt,conn)
            return dataframe
        else:
            cur.execute(sqlstmt)

    finally:
        cur.close()

def update_process_table(conn,SchemaName,TableName, IdValue,EndTimeValue,Status,ErrorLog):
    try:
        sqlstmt = "UPDATE " + SchemaName+"."+TableName + " SET FINISHED_ON = '"\
        +str(EndTimeValue) +"'"+ ", STATUS = '" +str(Status) +"'" + ", LOG_FILE_NAME = '"\
        +str(ErrorLog) +"' WHERE PROCESS_ID = '"+str(IdValue)+ "'"
        
        execute_sqlstmt(conn,sqlstmt)

    except Exception as e:
        print('update stmt failed',str(e))
        
def write_snowflake(dataset,df):
    #Used By Logging scenario for Writing
    snowflake_destination = dataiku.Dataset(dataset)
    snowflake_destination.spec_item["appendMode"] = True
    with snowflake_destination.get_writer() as writer:
        writer.write_dataframe(df)
        
        
def start_scenario_process_log(scenario_name,process_name,PARAMETERS, project_details='Default'):
    try:
    
       # get client and Scenario details
        client = dataiku.api_client()
        if project_details=='Default':
            Project = client.get_default_project()
        else:
            Project = client.get_project(project_details)

        scenario = Project.get_scenario(scenario_name)
        
        project_variables = Project.get_variables()

        current_run = scenario.get_current_run()
        current_run_details = current_run.get_details()
        process_id = current_run_details['scenarioRun']['runId']
        # commenting this line to take scenario name from parameters
        # process_name = current_run_details['scenarioRun']['trigger']['scenarioId']
        process_name = process_name
        process_user_name = current_run_details['scenarioRun']['runAsUser']['realUserLogin']
        process_trigger_name = current_run_details['scenarioRun']['trigger']['trigger']['name']
        process_triggered_by = process_trigger_name+" - "+process_user_name
        process_start_time = current_run.start_time
        process_start_time = process_start_time.astimezone(timezone('US/Eastern'))
        process_end_time = pd.NaT
        initial_status = "start"
        error_msg = ''
        process_id_num = process_id.replace("-","")
        process_data = [[process_id_num,process_name,process_triggered_by,PARAMETERS,process_start_time,process_end_time,initial_status,error_msg]]

        process_df = pd.DataFrame(process_data,columns=['PROCESS_ID','PROCESS_NAME','TRIGGERED_BY','PARAMETERS','STARTED_ON','FINISHED_ON','STATUS','LOG_FILE_NAME'])
        
        scenario_process_id_variable = scenario_name+"_SCENARIO_PROCESS_ID"
        
        project_variables["local"][scenario_process_id_variable] = process_id_num
        Project.set_variables(project_variables)


        return process_df,process_id_num,str(process_start_time)
    except Exception as e:
        print('start_scenario_process_log failed',str(e))

def end_scenario_process_log(scenario_name,SchemaName,TableName ,project_details='Default'):
    try:
       # get client and Scenario details
        client = dataiku.api_client()
        if project_details=='Default':
            Project = client.get_default_project()
        else:
            Project = client.get_project(project_details)

        scenario = Project.get_scenario(scenario_name)
        
        project_variables = Project.get_variables()
        
        scenario_process_id_variable = scenario_name+"_SCENARIO_PROCESS_ID"
        
        process_id = project_variables["local"][scenario_process_id_variable]

        last_run = scenario.get_run(process_id)
        final_status = last_run.outcome
        process_end_time = last_run.end_time

        error_msg='NA'
        #Error printing
        if last_run.outcome == "FAILED":
            last_run_details = last_run.get_details()
            if last_run_details.first_error_details is None:
                error_msg = "Error message unavailable"
            else:
                error_msg= last_run_details.first_error_details.get('message').replace("'","") 
                
        error_msg = re.sub(r"[^a-zA-Z0-9]+", ' ', error_msg)   
        
        conn=connect_to_snowflake(SchemaName)
        update_process_table(conn,SchemaName,TableName, process_id,process_end_time,final_status,error_msg)
        
        project_variables["local"][scenario_process_id_variable] = ""
        Project.set_variables(project_variables)

        return process_end_time,final_status,error_msg,process_id
    except Exception as e:
        print('end_scenario_process_log failed',str(e))


from dataiku.scenario import Scenario
import dataiku
import pandas as pd

def extract_elements(json_obj, keys):
    result = {}
    
    def extract(json_obj, keys, result):
        if isinstance(json_obj, dict):
            for key, value in json_obj.items():
                if key in keys:
                    result[key] = value
                else:
                    extract(value, keys, result)
        elif isinstance(json_obj, list):
            for item in json_obj:
                extract(item, keys, result)
    
    extract(json_obj, keys, result)
    return result


def run_scenario_from_point_failure(scenario_name, scenario_first_step, project_details='Default'):
    '''
    Runs a scenario if failed or aborted from step which it failed.
    
    param scenario_name: A valid scenario name
    param scenario_first_step:  A valid first step name of the scenario.
    param project_details: if project_details is default it will get the default project name and if mentioned it will run in that particular project.
    
    '''
        
    try:
        # get client and Scenario details
        client = dataiku.api_client()
        if project_details=='Default':
            project = client.get_default_project()
        else:
            project = client.get_project(project_details)

        scenario = project.get_scenario(scenario_name)
        project_variables = project.get_variables()
        last_run = scenario.get_last_finished_run()
        
        # check if the last finished scenario run outcome is 'FAILED' or 'ABORTED' only then perform next steps
        if last_run.outcome in ['FAILED','ABORTED']:
            last_run_details = last_run.get_details()
            scenario_last_step = last_run_details.last_step
            failed_step = extract_elements(scenario_last_step, 'step')
            last_failed_step_name = failed_step['step']['name']
            scenario_end_step = scenario_name+"_SCENARIO_START_FROM_STEP"
            project_variables["local"][scenario_end_step] = {last_failed_step_name: 1}
            project.set_variables(project_variables)
            
        # else if the last finished scenario run outcome is not 'FAILED' or 'ABORTED' only then perform next steps
        else:
            scenario_end_step = scenario_name+"_SCENARIO_START_FROM_STEP"
            project_variables["local"][scenario_end_step] = {scenario_first_step: 1}
            project.set_variables(project_variables)
            
    except Exception as e:
        print(f"An error occurred while funtion run_scenario_from_point_failure: {e}")


import dataiku
from dataiku import pandasutils as pdu
import pandas as pd, numpy as np
import json
import io
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
        print('appkey:',appkey)
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
        hidden_cols_list = ['ServerRedirectedEmbedUrl','ServerRedirectedEmbedUri','Id','FileSystemObjectType','OData__UIVersionString',
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
                           'ParentUniqueId']
        
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
        ctx = auth_sharepoint_cert(site_url, OLS)
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
    
        ctx = auth_sharepoint_cert(site_url, OLS)
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






            
            
    
