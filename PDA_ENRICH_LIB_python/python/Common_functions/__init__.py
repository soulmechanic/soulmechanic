import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import time
import logging
import multiprocessing
from PROCESS_LIST_FUNC import execute_sqlstmt, connect_to_snowflake
import re
from dataiku import Folder
from datetime import datetime
import zipfile
import os





def write_snowflake(dataset,df):
    #Used By Logging scenario for Writing
    snowflake_destination = dataiku.Dataset(dataset)
    snowflake_destination.spec_item["appendMode"] = True
    with snowflake_destination.get_writer() as writer:
        writer.write_dataframe(df)  
    
        
def num_inplace(df):
    """Automatically detect and convert (in place!) each
    dataframe column of datatype 'object' to a datetime just
    when ALL of its non-NaN values can be successfully parsed
    by pd.to_datetime().  Also returns a ref. to df for
    convenient use in an expression.
    """
    from pandas.errors import ParserError
    for c in df.columns[df.dtypes=='object']: #don't cnvt num
        try:
            df[c]= pd.to_numeric(df[c])
            #print(df[c])
        except (ParserError,ValueError): #Can't cnvrt some
            pass # ...so leave whole column as-is unconverted
    return df        

def dt_inplace(df):
    """Automatically detect and convert (in place!) each
    dataframe column of datatype 'object' to a datetime just
    when ALL of its non-NaN values can be successfully parsed
    by pd.to_datetime().  Also returns a ref. to df for
    convenient use in an expression.
    """
    from pandas.errors import ParserError
    for c in df.columns[df.dtypes=='object']: #don't cnvt num
        try:
            df[c]=pd.to_datetime(df[c])
            #print(df[c])
        except (ParserError,ValueError): #Can't cnvrt some
            pass # ...so leave whole column as-is unconverted
    return df

def data_type_conversion(df):
    df= num_inplace(df)
    df= dt_inplace(df)
    return df
    
def table_structure(df,dataset):
    df_table= data_type_conversion(df)
    res = df_table[0:0]
    # Write recipe outputs
    OUTPUT_TABLE = dataiku.Dataset(dataset)
    OUTPUT_TABLE.write_with_schema(res)
    


def check_table_exist(conn,table_name):
    from snowflake.connector import connect
    cur = conn.cursor()
    sql = "SHOW TABLES  LIKE " + "'" + table_name + "'"
    print('sql:',sql)
    cur.execute(sql)
    result = cur.fetchone()
    print('result:',result)
    
    if result:
       cur.close() 
       return 1
    else :
       cur.close() 
       return 0
    
def get_coded_user(appKey):
    ##Replacing legacy auth model with App ID/Key
    client = dataiku.api_client()
    auth_info = client.get_auth_info(with_secrets=True)

    #appKey = 'codedUser'
    #appSecret = None
    for credential in auth_info["secrets"]:
        if credential["key"] == appKey:
            appSecret = credential["value"]
            break
    return appSecret
    if not appSecret:
        return 0    
    
def get_snowflake_connection(SchemaName):
    from snowflake.connector import connect
    user_secrets = dataiku.api_client().get_auth_info(with_secrets=True)["secrets"]
    if not user_secrets:
        return 0 
    #Snowflake connection variables
    Snowflake_Account = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Account")['value']
    Snowflake_User = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_User")['value']
    Snowflake_Password = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Password")['value']
    Snowflake_Role = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Role")['value']
    Snowflake_DB = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_DB")['value']
    Snowflake_Warehouse = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Warehouse")['value']

    # -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
    schema = SchemaName
    try:
        conn = connect(
            user= Snowflake_User,
            password= Snowflake_Password,
            account= Snowflake_Account,
            role= Snowflake_Role,
            warehouse = Snowflake_Warehouse,
            database = Snowflake_DB,
            schema = schema,
        )
        return conn
    except Exception as e:
        print('Error while connecting to Snowflake', e)
        return 0
            
#def write_csv_to_S3(folder_path, file_name, dataframe):
#    """
#    This function lets user write dataframe into S3 as csv.
#    User must mention the folder path, the csv file name and the dataframe that needs to be written as csv
#    """
#    try:
#        folder_path.upload_stream(file_name,dataframe.to_csv(index=False).encode("utf-8"))
#        return 1
#    except Exception as e:
#        print(e)
#        return 0
    
def write_csv_to_S3(s3_folder_path, s3_folder_name, file_name, dataframe, separator):
    """
    This function lets user write dataframe into S3 as csv.
    User must mention the S3 folder path, the csv file name and the dataframe that needs to be written as csv
    Parameters:
    1) s3_folder_path - Destination folder path, make sure you select correct child folders in the setting section of
        Dataiku managed folders. In this case it will be "Enrich"
    2) s3_folder_name - Name of the folders inside Enrich folder. In this case this will be,
        a) Flat File b) Portfolio c) Portfolio Snapshot Mapping d) Profile Probabilities e) Profit and Loss f) Project
        g) Snapshot
    3) file_name - Name of the file with extension. E.g.: 'file_name.csv'
    4) dataframe - Name of the DataFrame which has to be written as csv
    """
    try:
        file_path  = s3_folder_name + '/' + file_name
        s3_folder_path.upload_stream(file_path, dataframe.to_csv(index=False,sep= separator).encode("utf-8"))
        return 1
    except Exception as e:
        raise Exception("write_csv_to_S3:Unable to write on s3 Enrich folder:",str(file_name,e))
       # return 0
    
def capture_stg_error_records(conn,process_id,load_type,table_name,table_name_stg,query_id):
    from snowflake.connector import connect
    try:
        cur = conn.cursor()
        sql_del = "DELETE FROM " + table_name + " WHERE LOAD_TYPE='" + load_type + "'"
        cur.execute(sql_del)
        Query = """INSERT INTO VAW_AMER_DEV_PUB.PDAENRICH.<table_name> 
                       select <process_id>,
                       <load_type>,
                       error,
                       file,
                       line,
                       b.column_name,
                       rejected_record
                       from
                       table(validate(VAW_AMER_DEV_PUB.PDAENRICH.<table_name_stg>, 
                           job_id => '<query_id>' )) b; """
        Query = Query.replace('<table_name>',table_name)
        Query = Query.replace('<table_name_stg>',table_name_stg)
        Query = Query.replace('<process_id>',process_id)
        Query = Query.replace('<load_type>',"'" + load_type + "'")
        Query = Query.replace('<query_id>',query_id)
        print(Query)
        cur.execute(Query)
        conn.commit()
        print("Insert completed")
        cnsql = 'select count(*) from VAW_AMER_DEV_PUB.PDAENRICH.' + table_name;
        print("cnsql:",cnsql)
        #cnsql = cnsql.replace('<table_name>',table_name)
        cur.execute(cnsql)
        cnt = cur.fetchone()
        cnt = int(cnt[0])
        if cnt > 0:
           sql_trunc = 'TRUNCATE TABLE ' + table_name_stg
           print("sql_trunc:",sql_trunc)
           cur.execute(sql_trunc)
        cur.close()     
        return cnt
    except Exception as e:
        raise Exception("Issue in capturing error records",e)
        return 0  
def load_s3_to_stg(conn,table_name,stage_name,file_format,pattern,on_error_option):
    from snowflake.connector import connect
    try:
        cur = conn.cursor()
        #sql_trunc = 'TRUNCATE TABLE ' + table_name
        #cur.execute(sql_trunc)
        Query= ''' COPY INTO VAW_AMER_DEV_PUB.PDAENRICH.<table_name>
                   FROM @VAW_AMER_DEV_PUB.PDAENRICH.<stage_name>
                   File_format = (FORMAT_NAME =VAW_AMER_DEV_PUB.PDAENRICH.<file_format>)
                   pattern = '<pattern>'
                   on_error = '<on_error_option>'; '''
            
        Query = Query.replace('<table_name>',table_name)
        Query = Query.replace('<stage_name>',stage_name)
        Query = Query.replace('<file_format>',file_format)
        Query = Query.replace('<pattern>',pattern)
        Query = Query.replace('<on_error_option>',on_error_option)
        print(Query)
        cur.execute(Query)
        get_last_query_id = 'select last_query_id();'
        cur.execute(get_last_query_id)
        query_id = cur.fetchone()
        print('queryid:'+ str(query_id))
        query_id = str(query_id[0])
        conn.commit()
        cur.close()     
        return query_id
    except Exception as e:
        print('excepton',e)
        return 0       
    
def process_id_derive(DSS_Project,DSS_Scenario):
    """Based on parameter passed DSS project and Scenario, 
    It will check the Scenario status and if it is running then it will fetch the 
    Current process_Id for the Scenario.
    """
    client = dataiku.api_client()
    try:
        Project = client.get_project(DSS_Project)
        scenario = Project.get_scenario(DSS_Scenario)
        current_run = scenario.get_current_run()
        if current_run != None:
            current_run_details = current_run.get_details()
            process_id = current_run_details['scenarioRun']['runId']
            return process_id
    except Exception as e:
        raise Exception("Issue in getting job process id")

        
        
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

def s3_archival(src_folder_id, dest_folder_id):
    
    """
    This function lets users to archive the folder data. It gets a backup of the S3 source folder
    to an archive folder and then clear the source folder.
    
    Parameters:
    1) src_folder_id - ID of the source folder, make sure you provide the correct ID to prevent any accidents. One can get this detail from the
        folder url.
        Eg.: https://dss-amer-dev.pfizer.com/projects/PDAENRICHHISTORICAL/managedfolder/DWRABCLm/view/ , in this link "DWRABCLm" is the ID.
    2) dest_folder_id - ID of the backup folder. Extract ID in the similar way as above.    
    """
    
    try:
        src_folder = dataiku.Folder(src_folder_id)
        dest_folder = dataiku.Folder(dest_folder_id)   

        #List of paths from partition
        paths = src_folder.list_paths_in_partition()
        for x in range(len(paths)):

            #check whether the file is CSV file
            if (re.search('(.*?).csv' , paths[x])) and 'archive' not in paths[x].lower():
                with src_folder.get_download_stream(paths[x]) as f:
                    #print(paths[x])
                    dest_folder.upload_stream(paths[x], f)
                    src_folder.clear_path(paths[x])
                    
        print(str(src_folder.get_info()['name']) + " folder is successfully archived and backep up in " + 
              str(dest_folder.get_info()['name']) + " folder.")
        return 1            
    except Exception as e:
        raise ValueError(f"An error occurred while file archival: {e}")
        
def check_s3_file_exists(folder_id):    
    try:
        src_folder = dataiku.Folder(folder_id)
        x=0
        #List of paths from partition
        paths = file_path.list_paths_in_partition()
        for paths[x] in paths:

            #check whether the file is CSV file
            if(re.search('(.*?).csv' , paths[x])):
                 print("File exists in :" ,file_path)
                 return 1
            else :
                 return 0
                    
    except Exception as e:
        raise ValueError(f"An error occurred while checking file existence: {e}")
        
def set_status_process_list(conn,TableName,ProcessId,EndTimeValue,Status,log_file_name,process_name):
    try:
        cur = conn.cursor()
        
        sqlstmt = "UPDATE " +TableName + " SET FINISHED_ON = '"\
        +str(EndTimeValue) + "'" + ", STATUS = '" + str(Status) + "'" + ", LOG_FILE_NAME = '" + str(log_file_name) + "'"\
        + " WHERE PROCESS_ID = '"+str(ProcessId)+ "'"
        #\
        #+ " AND PROCESS_NAME = '"+str(process_name)+ "'"
        
        print("sqlstmt:"+ sqlstmt)
        
        cur.execute(sqlstmt)

    except Exception as e:
        raise ValueError(f"Update status process list failed: {e}")
        
def set_snapshot_run_details(conn,TableName,ProcessId,StartTimeValue,EndTimeValue):
    try:
        cur = conn.cursor()
        
        sqlstmt = "UPDATE " +TableName + " SET FINISHED_ON = '"\
        +str(EndTimeValue) +"'"+ ", STARTED_ON = '" +str(StartTimeValue) + "'"+ ", PROCESS_ID = '" +str(ProcessId) + "'"
        
        print("update stmt:",sqlstmt)
        
        cur.execute(sqlstmt)

    except Exception as e:
        raise ValueError(f"Update snapshot run details failed: {e}")        
        
        
def archive_files_from_folder(source_folder, target_folder, archive_subfolders, override = False):
    
    source_folder = Folder(source_folder)
    target_folder = Folder(target_folder)
       
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Locate file in source S3 folder which is older than current date
    files = source_folder.list_paths_in_partition()
    
    previous_day_files = [file for file in files if datetime.fromtimestamp(source_folder.get_path_details(file)['lastModified']/1000).strftime('%Y-%m-%d') >= current_date]
    
    

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
        
        # List the contents of the folder
        contents = target_folder.list_paths_in_partition()
        
        if not '/'+subfolder_name+'/'+zip_file_name in contents:
            with open(zip_file_name, "rb") as f:
                target_folder.upload_stream('/'+subfolder_name+'/'+zip_file_name, f)       

                #x=0
                # Check if the zip file exists in target folder before deleting files in source folder
                #if '/'+subfolder_name+'/'+zip_file_name in contents:

                for to_be_deleted_csv_file in list_of_csv_files:
                    print("*************************************"+to_be_deleted_csv_file+"*************************************")
                    source_folder.clear_path(to_be_deleted_csv_file)
        elif override:
            with open(zip_file_name, "rb") as f:
                target_folder.upload_stream('/'+subfolder_name+'/'+zip_file_name, f)       



                for to_be_deleted_csv_file in list_of_csv_files:
                    print("*************************************"+to_be_deleted_csv_file+"*************************************")
                    source_folder.clear_path(to_be_deleted_csv_file)

        else:
            print(f"The Zip {zip_file_name} already exist")
    return 
   