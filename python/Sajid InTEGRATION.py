
# coding: utf-8

# In[ ]:



import dataiku
import pandas as pd, numpy as np
from pandas.io.json import json_normalize
from dataiku import pandasutils as pdu
from nexus_common import write_snowflake
from snowflake.connector.pandas_tools import write_pandas
from dataiku.core.sql import SQLExecutor2
from snowflake.connector import connect
from io import BytesIO
from pandas import DataFrame
import zipfile
from zipfile import ZipFile
from ast import literal_eval

import re
import io
import json
import sys
import math
import gc



# Read recipe inputs
CAPTARIO_S3_BUCKET = dataiku.Folder("wD8wbeOz")


#List of paths from partition
paths = CAPTARIO_S3_BUCKET.list_paths_in_partition()

#CAPTARIO_S3_BUCKET_info = CAPTARIO_S3_BUCKET.get_info()


# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.


# In[ ]:


def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])


# In[ ]:


def Load_account_files(child_id,modelType,list_of_file_names,z,fileName, Source, project_id= None):
    if(modelType=='ProgramOption'):
        folder_path= 'inputs/'
    else:

        folder_path= 'child-simulations/'+ child_id + '/inputs/'

    file = [x for x in list_of_file_names if re.search(folder_path + fileName + '.json',x)]


    appended_data = []
    try:

        for elem in file:
            if elem.startswith(folder_path) and elem.endswith(fileName + '.json'):
                out = z.open(elem)
                data = json.load(out)

                if(fileName == 'accounts'):
                    df_account= pd.json_normalize(data)
                    #print(df_account)
                    #read only selected columns
                    df_account = df_account[['Name','IsRevenue','IsCost','IsDevelopment']]
                    df_account['Code'] = df_account['Name'].str.split().agg("".join)
                    df_account["SUB_CODE"] = None
                    df_account["AC_TYPE"] = None
                    appended_data.append(df_account)
                else:
                    assup_df= pd.json_normalize(data)
                    #filter for assumption where AttributeValues is null or empty[]
                    assup_filter_df= assup_df[(assup_df.AttributeValues.str.len()<1) | (assup_df.AttributeValues.isnull())]
                    assup_filter_df=assup_filter_df[['AssumptionKey','Name','IsTimeAssumption','IsModelExpression','Param1','Param2','Param3','Param4']]


                    #Read nested json
                    df_account= pd.json_normalize(data,'AttributeValues',['AssumptionKey','Name','IsTimeAssumption','IsModelExpression','Param1','Param2','Param3','Param4'],record_prefix='_')
                    df_account = df_account[df_account['_Name'] != 'Name']
                    df= df_account.pivot(index =['AssumptionKey','Name'], columns='_Name', values='_Value')
                    df=df.reset_index()
                    df_account = pd.merge(df_account[['AssumptionKey','Name','IsTimeAssumption','IsModelExpression','Param1','Param2','Param3','Param4']],df,on=['AssumptionKey','Name'], how='left')
                    #df_account = df_account.reset_index(level=0)

                    frames = [df_account, assup_filter_df]
                    df_account=pd.concat(frames)

                    df_account['Project_Id']= project_id
                    #df_account = df_account[['DP_NAME','DP_GROUP','DP_REGION','DP_PHASE','DP_SCENARIO','TYPE']]
                    appended_data.append(df_account)

        return appended_data
    except Exception as e:
        print('Failed while performing transformation in Load_account_files function', e)


# In[ ]:


# generate incremental user generated ids to be assigned to GUIDs
def insert_incrIds(Cols_list, InDf, OutDf,max_value=[]):
    try:

        # iterate through columns in the list of columns specified
        for col in Cols_list:

            # if the max ids mentioned
            if max_value:
                for M_value in max_value:
                    M_value = int(M_value)

                    # adding incremental id user generated in ascending order
                    OutDf.insert(0, col, range(M_value, M_value + len(OutDf)))

            # if no max ids are mentioned
            elif InDf.empty:
                MAXID= 1000
            else:
                MAXID = int(InDf[col].max())

            print(len(OutDf))
            print(MAXID)

            # adding incremental id user generated in ascending order
            OutDf.insert(0, col, range(MAXID, MAXID + len(OutDf)))

        return

    except Exception as e:
        print('Failed while incrementing the id column', e)


# In[ ]:


def max_id_find(df,id_col):
    try:
        if df.empty:
            max_tbl=0
        else:
            max_tbl= df[id_col].max()

        return max_tbl
    except Exception as e:
        print('Error while finding row count from dataframe', e)


# In[ ]:


## Create list which stores all the calculated result for system timeseries and system values
def load_calculated_result_parquet_file(modelType,result_folder,list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,proj_id,program_option_id):

    if(modelType=='ProgramOption'):
        folder_path= 'results/' + result_folder
    else:
        folder_path= 'child-simulations/' + child_id + '/results/' + result_folder

    file = [x for x in list_of_file_names if re.search(folder_path,x)]
    x=0

    appended_data = []

    try:

        # Iterate over the list of file names in given list & get parquet file
        for elem in file:
            if elem.startswith(folder_path) and elem.endswith('.parquet'):
                #class_name= (elem.rsplit('/',1)[-1]).split('.')[0]
                type=(elem.rsplit('/',1)[-1]).split('.')[0]

                # Read parquet file
                out = z.open(elem)
                data = out.read()

                #read parquet file to pandas dataframe
                df_sys_series = pd.read_parquet(BytesIO(data), engine='pyarrow')
                df_sys_series.columns=[*df_sys_series.columns[:-1],'Value']

                if 'timeseries' in result_folder:
                    if not 'IterationIndex' in df_sys_series.columns:
                        df_sys_series['IterationIndex'] = 0
                    df_sys_series[['SIMULATION_ID','INITIATIVE_ID','PORTFOLIO_ID','PROJECT_ID','PROGRAM_OPTION_ID','DP_NAME']] = pd.DataFrame([[simulation_id,                                                                                               initiative_id,                                                                                               portfolio_id,                                                                                               proj_id,                                                                                               program_option_id,                                                                                               type]],                                                                                             index=df_sys_series.index)

                    df_sys_series=df_sys_series[['SIMULATION_ID','INITIATIVE_ID','PORTFOLIO_ID','PROJECT_ID','PROGRAM_OPTION_ID','DP_NAME','IterationIndex','TimeBucket','Value']]

                    #print(df_sys_series.shape[0])
                    appended_data.append(df_sys_series)

                    #print(df_sys_series.shape[0])



                else:
                    if not 'IterationIndex' in df_sys_series.columns:
                        df_sys_series['IterationIndex'] = 0
                    #df_sys_series.drop(['IterationIndex'], axis=1)
                    df_sys_series[['SIMULATION_ID','INITIATIVE_ID','PORTFOLIO_ID','PROJECT_ID','PROGRAM_OPTION_ID','DP_NAME']] = pd.DataFrame([[simulation_id,                                                                                                                                                initiative_id,                                                                                                                                                portfolio_id,                                                                                                                                                proj_id,                                                                                                                                                program_option_id,                                                                                                                                                type]],                                                                                                                                              index=df_sys_series.index)

                    df_sys_series=df_sys_series[['SIMULATION_ID','INITIATIVE_ID','PORTFOLIO_ID','PROJECT_ID','PROGRAM_OPTION_ID','DP_NAME','IterationIndex','Value']]
                    appended_data.append(df_sys_series)


        return appended_data
    except Exception as e:
        print('Error while reading the data from ' + result_folder, e)


# In[ ]:


def snowflake_conn(schema,query):
    user_secrets = dataiku.api_client().get_auth_info(with_secrets=True)["secrets"]
    Snowflake_Account = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Account")['value']
    Snowflake_User = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_User")['value']
    Snowflake_Password = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Password")['value']
    Snowflake_Role = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Role")['value']
    Snowflake_DB = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_DB")['value']
    Snowflake_Warehouse = next(a_dict for a_dict in user_secrets if a_dict["key"] == "Snowflake_Warehouse")['value']


    #table_name = table_name
    schema = schema
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
        cur_qry= conn.cursor().execute(query)
        return cur_qry
    except Exception as e:
        raise Exception('Error while connecting to Snowflake', e)


# In[ ]:


def delete_stg_tables(table_name):
    try:
        query= "DROP TABLE {}".format(table_name)
        snowflake_conn("PDACAPTARIO",query)
    except Exception as e:
        print('Error while deleting table' + table_name, e)


# In[ ]:


def account_transform(df_account):
    try:
        #df_account= pd.concat([df_account,assup_acc_df], ignore_index= True)
        df_account['Name']= df_account['Name'].str.title()
        df_account['Code'] = df_account['Name'].str.split().agg("".join)
        #df_account.fillna({'SUB_CODE':'NA', 'AC_TYPE':'NA', 'IsRevenue':'NA', 'IsDevelopment':'NA','IsCost':'NA'}, inplace=True)

        df_account = (df_account.sort_values(by=['Code'], ignore_index=True)
                      .drop_duplicates(subset=['Name'],keep='first'))



        #Get data from ACCOUNT snowflake table
        ACCOUNT_TABLE_df = dataiku.Dataset("ACCOUNT_INPUT").get_dataframe()
        ACCOUNT_TABLE_df.fillna({'SUB_CODE':'NA', 'AC_TYPE':'NA'}, inplace=True)

        if not ACCOUNT_TABLE_df.empty:
            df_account = df_account.merge(ACCOUNT_TABLE_df[['NAME','IS_REVENUE','IS_DEVELOPMENT','IS_COST']], left_on="Name", right_on="NAME", how="left").drop(columns = ['IsRevenue','IsCost','IsDevelopment','NAME'])
        else:
            df_account.rename(columns={'IsRevenue': 'IS_REVENUE', 'IsCost': 'IS_COST', 'IsDevelopment': 'IS_DEVELOPMENT'}, inplace=True)



        #Rename dataframe columns
        df_account.rename(columns={'Code': 'AC_CODE', 'Name': 'NAME'}, inplace=True)
        df_account.fillna({'SUB_CODE':'NA', 'AC_TYPE':'NA','IS_COST':False,'IS_DEVELOPMENT': False,'IS_REVENUE':False}, inplace=True)


        df_common = df_account.merge(ACCOUNT_TABLE_df, how = 'inner' ,indicator=False)

        df_account= df_account[(~df_account.AC_CODE.isin(df_common.AC_CODE))]

        #increment id by auto increment
        Cols_list = ['ACCOUNT_ID']
        insert_incrIds(Cols_list,ACCOUNT_TABLE_df,df_account)

        #find max of Datapoint ID from existing table and dataframe
        max_df= df_account['ACCOUNT_ID'].max()
        max_tbl = max_id_find(ACCOUNT_TABLE_df,'ACCOUNT_ID')

        if not df_account.empty:
            #call function to load incremental data
            write_snowflake('ACCOUNT',df_account)
    #load_incremental_data(df_account,'ACCOUNT',max_df,max_tbl,'ACCOUNT_ID')
    except Exception as e:
        print('Error while performing transformation for Accounts table', e)


# In[ ]:


def assumption_dp_transform(df_account1,final_sys_timeseries_df,final_sys_values_df,final_custom_timeseries_df,final_custom_values_df):

    try:
        #print(df_account1)
        #Create a df to get unique set of datapoints from output timeseries
        dp_timeseries_df = pd.DataFrame(sorted(final_sys_timeseries_df['DP_NAME'].unique()),columns =['Name'])
        #dp_timeseries_df['SOURCE'] = 'System Timeseries'

        #Create a df to get unique set of datapoints from output values
        dp_values_df = pd.DataFrame(sorted(final_sys_values_df['DP_NAME'].unique()),columns =['Name'])

        #Create a df to get unique set of datapoints from Custom timeseries
        custom_timeseries_df = pd.DataFrame(sorted(final_custom_timeseries_df['DP_NAME'].unique()),columns =['Name'])
        #custom_timeseries_df['SOURCE'] = 'Custom Timeseries'

        #Create a df to get unique set of datapoints from Custom values
        custom_values_df = pd.DataFrame(sorted(final_custom_values_df['DP_NAME'].unique()),columns =['Name'])
        #custom_values_df['SOURCE'] = 'Custom Values'

        #Write recipe outputs
        stg_custom_dp = dataiku.Dataset("STG_CUSTOM_DATAPOINTS")
        stg_custom_dp.write_with_schema(custom_timeseries_df)

        #Write recipe outputs
        stg_custom_dp_values = dataiku.Dataset("STG_CUSTOM_VALUES_DP")
        stg_custom_dp_values.write_with_schema(custom_values_df)


        query= """SELECT cst.Name,reg.Name REGION, phs.Name PHASE, sch.Name SCENARIO
                FROM PDACAPTARIO.STG_CUSTOM_DATAPOINTS cst
                LEFT JOIN "VAW_US_RS_DEV_DB"."PDACAPTARIO"."REGION" reg
                ON CONTAINS(cst.Name,reg.Name)
                LEFT JOIN "VAW_US_RS_DEV_DB"."PDACAPTARIO"."PHASES" phs
                ON CONTAINS(cst.Name,phs.Name)
                LEFT JOIN "VAW_US_RS_DEV_DB"."PDACAPTARIO"."SCENARIO" sch
                ON CONTAINS(cst.Name,sch.Code)"""
        rows= snowflake_conn('PDACAPTARIO',query)

        df_custom_timeseries = DataFrame(rows.fetch_pandas_all())


        '''
            Custom values data from custom-values folder
        '''

        query= """SELECT cst.Name,reg.Name REGION, phs.Name PHASE
                 FROM PDACAPTARIO.STG_CUSTOM_VALUES_DP cst
                 LEFT JOIN "VAW_US_RS_DEV_DB"."PDACAPTARIO"."REGION" reg
                 ON CONTAINS(cst.Name,reg.Name)
                 LEFT JOIN "VAW_US_RS_DEV_DB"."PDACAPTARIO"."PHASES" phs
                 ON CONTAINS(cst.Name,phs.Name)"""
        rows= snowflake_conn('PDACAPTARIO',query)

        #custom values timeseries
        df_custom_values = DataFrame(rows.fetch_pandas_all())

        #Rename dataframe columns
        df_custom_timeseries.rename(columns={'NAME': 'Name','REGION':'Region','PHASE':'Phase','SCENARIO':'Scenario'}, inplace=True)
        df_custom_values.rename(columns={'NAME': 'Name','REGION':'Region','PHASE':'Phase'}, inplace=True)

        #Get data from Assumption_datapoint snowflake table
        ASSUMPTION_DATAPOINT_df = dataiku.Dataset("ASSUMPTION_DATAPOINT_INPUT").get_dataframe()

        #Get data from ACCOUNT snowflake table
        ACCOUNT_TABLE_df = dataiku.Dataset("ACCOUNT_INPUT").get_dataframe()

        #Drop duplicate values
        df_assup_dp= df_account1.drop(['AssumptionKey','Param1','Param2','Param3','Param4','Project_Id','Form ID','Sub Account','IsTimeAssumption','IsModelExpression'], axis=1)
        #df_assup_dp = df_assup_dp[df_assup_dp['Name_x'] == 'NA'] 'Name_y',


        #df_assup_dp=df_assup_dp[df_assup_dp.Name.notnull()]
        df_assup_dp = (df_assup_dp.drop_duplicates())

        #merge assumpton datapoints and system timeseries datapoints
        df_assup_dp= pd.concat([df_assup_dp,dp_timeseries_df,dp_values_df,df_custom_timeseries,df_custom_values], ignore_index=True)

        #Join with Account table to find Account_Id
        df_assup_dp['account_lower'] = df_assup_dp['Account'].str.lower()
        ACCOUNT_TABLE_df['account_nm_lower'] = ACCOUNT_TABLE_df['NAME'].str.lower()
    #     df_assup_dp = df_assup_dp.merge(ACCOUNT_TABLE_df[['ACCOUNT_ID','account_nm_lower']], left_on="account_lower", right_on="account_nm_lower", how="left")
    #     print(df_assup_dp)

        df_assup_dp = df_assup_dp.merge(ACCOUNT_TABLE_df[['ACCOUNT_ID','account_nm_lower']], left_on="account_lower", right_on="account_nm_lower", how="left").drop(columns = ['account_nm_lower','account_lower','Account'])


        df_assup_dp = (df_assup_dp.drop_duplicates())
        #Rename column
        df_assup_dp.rename(columns={'Name': 'DP_NAME', 'Phase':'DP_PHASE', 'Region':'DP_REGION','Scenario':'DP_SCENARIO','Type':'DP_TYPE'}, inplace=True)
        df_assup_dp['DP_GROUP'] = df_assup_dp['DP_NAME']
        #df_assup_dp['DP_TYPE'] = df_assup_dp['DP_TYPE'].fillna(df_assup_dp['DP_SCENARIO'])

        df_assup_dp.fillna({'ACCOUNT_ID':999, 'DP_PHASE':'All', 'DP_REGION':'Global', 'DP_SCENARIO':'All','DP_TYPE':'NA','DP_NAME':'NA','DP_GROUP':'NA'}, inplace=True)
        ASSUMPTION_DATAPOINT_df.fillna({'ACCOUNT_ID':999, 'DP_PHASE':'All', 'DP_REGION':'Global', 'DP_SCENARIO':'All','DP_TYPE':'NA','DP_NAME':'NA','DP_GROUP':'NA'}, inplace=True)

        df_common = df_assup_dp.merge(ASSUMPTION_DATAPOINT_df, how = 'inner' ,indicator=False)

        df_assup_dp= df_assup_dp[(~df_assup_dp.DP_NAME.isin(df_common.DP_NAME))]



        #increment id by auto increment
        Cols_list = ['DATAPOINT_ID']
        insert_incrIds(Cols_list,ASSUMPTION_DATAPOINT_df,df_assup_dp)

        #Derive clean reporting name from DP_NAME clean column and add flag to identify risk and non-risk adjusted data
        df_assup_dp['REPORTING_DP_NAME'] = df_assup_dp.DP_NAME
        df_assup_dp['IS_RISK_ADJUSTED'] = np.where(df_assup_dp.DP_NAME.str.contains('Unadj'), False, True)


        #find distinct region from dataframe
        for ele in list(df_assup_dp.DP_REGION.unique()):
            df_assup_dp['REPORTING_DP_NAME'] = df_assup_dp['REPORTING_DP_NAME'].str.replace(ele,'')

        #find distinct Scenaio from dataframe
        for ele1 in list(df_assup_dp.DP_SCENARIO.unique()):
            df_assup_dp['REPORTING_DP_NAME'] = df_assup_dp['REPORTING_DP_NAME'].str.replace(ele1,'')

        #find distinct Phase from dataframe
        for ele1 in list(df_assup_dp.DP_PHASE.unique()):
            df_assup_dp['REPORTING_DP_NAME'] = df_assup_dp['REPORTING_DP_NAME'].str.replace(ele1,'')

        df_assup_dp['REPORTING_DP_NAME'] = df_assup_dp['REPORTING_DP_NAME'].map(lambda x:x.replace('_',''))
        df_assup_dp['REPORTING_DP_NAME'] = df_assup_dp['REPORTING_DP_NAME'].map(lambda x:x.replace('Unadj',''))


        #Change order by columns in the dataframe
        df_assup_dp= df_assup_dp[['DATAPOINT_ID','REPORTING_DP_NAME','DP_NAME','DP_REGION','DP_PHASE','DP_SCENARIO','DP_GROUP','DP_TYPE','ACCOUNT_ID','IS_RISK_ADJUSTED']]

        df_assup_dp= df_assup_dp.drop_duplicates(subset=['DP_NAME', 'DP_REGION','DP_PHASE','DP_SCENARIO'], keep='last')

        max_df= df_assup_dp['DATAPOINT_ID'].max()
        max_tbl = max_id_find(ASSUMPTION_DATAPOINT_df,'DATAPOINT_ID')

        if not df_assup_dp.empty:
            #call function to load incremental data
            write_snowflake('ASSUMPTION_DATAPOINT',df_assup_dp)
        #load_incremental_data(acc_OUT_DF1,'ASSUMPTION_DATAPOINT',max_df,max_tbl,'DATAPOINT_ID')
    except Exception as e:
        print('Error while performing transformation for Assumption_datapoints table', e)


# In[ ]:


def project_assumption_transform(df_account1):

    try:
        df_assup_proj= df_account1.drop(['Form ID','Sub Account','Type'], axis=1)
        #df_assup_proj= df_account1
        df_assup_proj.fillna({'Phase':'All', 'Region':'Global', 'Scenario':'All'}, inplace=True)
        #df_assup_proj.fillna('NA', inplace=True)
        #print(df_assup_proj)
        #.drop(['Form ID','Phase','Region','Scenario','Sub Account','Type'], axis=1)

        #Get data from PORTFOLIO_PROJECTS snowflake table
        PROJECTS_df = dataiku.Dataset("PROJECT").get_dataframe()

        #Get data from ASSUMPTION_DATAPOINT snowflake table
        ASSUMPTION_DATAPOINT_df = dataiku.Dataset("ASSUMPTION_DATAPOINT_INPUT").get_dataframe()

        #account table
#         ACCOUNT_df = dataiku.Dataset("ACCOUNT").get_dataframe()

#         ASSUMPTION_DATAPOINT_df= ASSUMPTION_DATAPOINT_df.merge(ACCOUNT_df[['ACCOUNT_ID','NAME']],left_on="ACCOUNT_ID", right_on="ACCOUNT_ID",how= "left")


        #join Portfolio_Project table to find numeric project code
        df_assup_proj = df_assup_proj.merge(PROJECTS_df[['GUID','PROJECT_ID']], left_on="Project_Id", right_on="GUID", how="left").drop(columns = ['Project_Id','GUID'])

        #assup_df[(assup_df.AttributeValues.str.len()<1)

#         ASSUMPTION_DATAPOINT_df['account_lower_1'] = ASSUMPTION_DATAPOINT_df['NAME'].str.lower()
#         df_assup_proj['account_lower_2'] = df_assup_proj['Account'].str.lower()


        df_assup_proj.fillna({'DP_NAME':'NA','DP_REGION':'NA','DP_PHASE':'NA','DP_SCENARIO':'NA','Name':'NA','Param1':'NA','Param2':'NA','Param3':'NA','Param4':'NA','Name_y':'NA','Name':'NA'}, inplace=True)
        #ASSUMPTION_DATAPOINT_df.fillna({'account_lower_1':'NA'}, inplace=True)

        df_assup_proj = df_assup_proj.merge(ASSUMPTION_DATAPOINT_df[['DP_NAME','DATAPOINT_ID','DP_REGION','DP_PHASE','DP_SCENARIO']], left_on=["Name","Region","Phase","Scenario"], right_on=["DP_NAME","DP_REGION","DP_PHASE","DP_SCENARIO"], how="left").drop(columns = ['DP_REGION','DP_PHASE','DP_SCENARIO','Phase','Region','Scenario'])
        #print(df_assup_proj)
        df_assup_proj.fillna({'DP_NAME':'NA','DP_REGION':'NA','DP_PHASE':'NA','DP_SCENARIO':'NA','Name':'NA','Param1':'NA','Param2':'NA','Param3':'NA','Param4':'NA','Name_y':'NA','Name':'NA'}, inplace=True)

        df_assup_proj = (df_assup_proj.drop_duplicates())


        Cols_list = ['ASSUMPTION_ID']
        assp_proj_list = df_assup_proj['PROJECT_ID'].unique()
        #str(df_assup_proj['PROJECT_ID']).unique()
        combined_assup_prj = ",".join( map(str, assp_proj_list) )
        query= "DELETE FROM PROJECT_ASSUMPTIONS WHERE PROJECT_ID IN (%s)" % (combined_assup_prj)
        snowflake_conn('PDACAPTARIO',query)

        PROJECT_ASSUMPTION_df = dataiku.Dataset("PROJECT_ASSUMPTIONS_INPUT").get_dataframe()
        insert_incrIds(Cols_list,PROJECT_ASSUMPTION_df,df_assup_proj)
        df_assup_proj.rename(columns={'AssumptionKey': 'GUID', 'IsTimeAssumption': 'IS_TIME_ASSUMPTION', 'IsModelExpression':'IS_MODEL_ASSUMPTION', 'Param1':'PARAM1','Param2':'PARAM2','Param3':'PARAM3','Param4':'PARAM4'}, inplace=True)
        df_assup_proj= df_assup_proj[['ASSUMPTION_ID','GUID','PROJECT_ID','DATAPOINT_ID','PARAM1','PARAM2','PARAM3','PARAM4','IS_TIME_ASSUMPTION','IS_MODEL_ASSUMPTION']]

        #Append data to Output_Timeseries Snowflake table
        write_snowflake('PROJECT_ASSUMPTIONS',df_assup_proj)
    except Exception as e:
        print('Error while performing transformation for Project_Assuumptions table', e)


# In[ ]:


def custom_dp_transform(out_df,custom_df,dataset,type):

    try:

        #Get data from PORTFOLIO_PROJECTS snowflake table
        PORTFOLIO_PROJECTS_df = dataiku.Dataset("PORTFOLIO_PROJECTS").get_dataframe()

        #Get data from ASSUMPTION_DATAPOINT snowflake table
        ASSUMPTION_DATAPOINT_df = dataiku.Dataset("ASSUMPTION_DATAPOINT_INPUT").get_dataframe()
        try:

            if type == 'Custom-timeseries':
                #Conact system timeseries and custom timeseries
                df=pd.concat([out_df,custom_df], ignore_index=True)
                #Rename columns
                df.rename(columns={'IterationIndex':'ITERATION_INDEX', 'Value':'VALUE','TimeBucket':'TIME_BUCKET'}, inplace=True)
                #Join with PORTFOLIO_PROJECT table to find PORTFOLIO_PROJECT_ID
                df = df.merge(PORTFOLIO_PROJECTS_df[['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_PROJECTS_ID']], left_on=["PROJECT_ID","PORTFOLIO_ID","SIMULATION_ID","INITIATIVE_ID","PROGRAM_OPTION_ID"], right_on=["PROJECT_GUID","PORTFOLIO_GUID","SIMULATION_GUID","INITIATIVE_GUID","PROGRAM_OPTION_GUID"], how="left").drop(columns = ['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_ID','PROJECT_ID','SIMULATION_ID','INITIATIVE_ID','PROGRAM_OPTION_ID'])

                #Join with ASSUMPTION_DATAPOINT table to find DATAPOINT_ID
                df = df.merge(ASSUMPTION_DATAPOINT_df[['DP_NAME','DATAPOINT_ID']], left_on=["DP_NAME"], right_on=["DP_NAME"], how="left")

                #Change the sequence of columns according Snowflake table structure
                df = df[['PORTFOLIO_PROJECTS_ID','DATAPOINT_ID','DP_NAME','ITERATION_INDEX','TIME_BUCKET','VALUE']]

                #find unique list of PORTFOLIO_PROJECTS_ID so that we can avoid duplicates
                port_proj_id_list = df['PORTFOLIO_PROJECTS_ID'].unique()
                combined_port_prj = ",".join( map(str, port_proj_id_list))

                #Delete the portfolio_project id if it is already there
                query= "DELETE FROM OUTPUT_TIMESERIES WHERE PORTFOLIO_PROJECTS_ID IN (%s)" % (combined_port_prj)
    #             snowflake_conn('PDACAPTARIO',query)

        except Exception as e:
            print(f'Error while performing transformation for {type}', e)

        try:

            if type == 'Custom-values':
                #Conact system values and custom values
                df=pd.concat([out_df,custom_df], ignore_index=True)


                #rename column in custom values
                df.rename(columns={'IterationIndex':'ITERATION_INDEX', 'Value':'VALUE'}, inplace=True)

                #Join with PORTFOLIO_PROJECT table to find PORTFOLIO_PROJECT_ID
                df = df.merge(PORTFOLIO_PROJECTS_df[['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_PROJECTS_ID']], left_on=["PROJECT_ID","PORTFOLIO_ID","SIMULATION_ID","INITIATIVE_ID","PROGRAM_OPTION_ID"], right_on=["PROJECT_GUID","PORTFOLIO_GUID","SIMULATION_GUID","INITIATIVE_GUID","PROGRAM_OPTION_GUID"], how="left").drop(columns = ['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_ID','PROJECT_ID','SIMULATION_ID','INITIATIVE_ID','PROGRAM_OPTION_ID'])

                #Join with ASSUMPTION_DATAPOINT table to find DATAPOINT_ID
                df = df.merge(ASSUMPTION_DATAPOINT_df[['DP_NAME','DATAPOINT_ID']], left_on=["DP_NAME"], right_on=["DP_NAME"], how="left")

                #Change the sequence of columns according Snowflake table structure
                df = df[['PORTFOLIO_PROJECTS_ID','DATAPOINT_ID','DP_NAME','ITERATION_INDEX','VALUE']]

                #find unique list of PORTFOLIO_PROJECTS_ID so that we can avoid duplicates
                port_proj_id_list = df['PORTFOLIO_PROJECTS_ID'].unique()
                combined_port_prj = ",".join( map(str, port_proj_id_list))

                #Delete the portfolio_project id if it is already there
                query= "DELETE FROM OUTPUT_VALUES WHERE PORTFOLIO_PROJECTS_ID IN (%s)" % (combined_port_prj)
    #             snowflake_conn('PDACAPTARIO',query)
        except Exception as e:
            print(f'Error while performing transformation for {type}', e)

        try:

            if type == 'User-timeseries':
                #Conact system timeseries and custom timeseries
                df = out_df.copy()
                #Rename columns
                df.rename(columns={'IterationIndex':'ITERATION_INDEX', 'Value':'VALUE','TimeBucket':'TIME_BUCKET'}, inplace=True)
                #Join with PORTFOLIO_PROJECT table to find PORTFOLIO_PROJECT_ID
                df = (df.merge(PORTFOLIO_PROJECTS_df[['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_PROJECTS_ID']],left_on=["PROJECT_ID","PORTFOLIO_ID","SIMULATION_ID","INITIATIVE_ID","PROGRAM_OPTION_ID"],right_on=["PROJECT_GUID","PORTFOLIO_GUID","SIMULATION_GUID","INITIATIVE_GUID","PROGRAM_OPTION_GUID"], how="left").drop(columns = ['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_ID','PROJECT_ID','SIMULATION_ID','INITIATIVE_ID','PROGRAM_OPTION_ID']))

                final_sim_assumps__IS_TIMESERIES_df=final_sim_assumps_df[final_sim_assumps_df['IS_TIMESERIES']==True]

                #Join with ASSUMPTION_DATAPOINT table to find DATAPOINT_ID
                df = (df.merge(final_sim_assumps__IS_TIMESERIES_df[['ASSUMPTION_KEY','DP_NAME','DATAPOINT_ID']], left_on=["DP_NAME"],right_on=["ASSUMPTION_KEY"], how="left").drop(columns=['ASSUMPTION_KEY','DP_NAME_x']).rename(columns={'DP_NAME_y':'DP_NAME'}))

                #Change the sequence of columns according Snowflake table structure
                df = df[['PORTFOLIO_PROJECTS_ID','DATAPOINT_ID','DP_NAME','TIME_BUCKET','ITERATION_INDEX','VALUE']]

                #find unique list of PORTFOLIO_PROJECTS_ID so that we can avoid duplicates
                port_proj_id_list = df['PORTFOLIO_PROJECTS_ID'].unique()
                combined_port_prj = ",".join( map(str, port_proj_id_list))

                #Delete the portfolio_project id if it is already there
                query= "DELETE FROM USER_TIMESERIES WHERE PORTFOLIO_PROJECTS_ID IN (%s)" % (combined_port_prj)
    #             snowflake_conn('PDACAPTARIO',query)
        except Exception as e:
            print(f'Error while performing transformation for {type}', e)

        try:

            if type == 'User-values':
                #Conact system timeseries and custom timeseries
                df = out_df.copy()
                #rename column in custom values
                df.rename(columns={'IterationIndex':'ITERATION_INDEX', 'Value':'VALUE'}, inplace=True)

                #Join with PORTFOLIO_PROJECT table to find PORTFOLIO_PROJECT_ID
                df = df.merge(PORTFOLIO_PROJECTS_df[['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_PROJECTS_ID']], left_on=["PROJECT_ID","PORTFOLIO_ID","SIMULATION_ID","INITIATIVE_ID","PROGRAM_OPTION_ID"], right_on=["PROJECT_GUID","PORTFOLIO_GUID","SIMULATION_GUID","INITIATIVE_GUID","PROGRAM_OPTION_GUID"], how="left").drop(columns = ['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_ID','PROJECT_ID','SIMULATION_ID','INITIATIVE_ID','PROGRAM_OPTION_ID'])

                final_sim_assumps__ISNOT_TIMESERIES_df=final_sim_assumps_df[final_sim_assumps_df['IS_TIMESERIES']==False]

                #Join with ASSUMPTION_DATAPOINT table to find DATAPOINT_ID
                df = (df.merge(final_sim_assumps__ISNOT_TIMESERIES_df[['ASSUMPTION_KEY','DP_NAME','DATAPOINT_ID']], left_on=["DP_NAME"],right_on=["ASSUMPTION_KEY"], how="left").drop(columns=['ASSUMPTION_KEY','DP_NAME_x']).rename(columns={'DP_NAME_y':'DP_NAME'}))

                #Change the sequence of columns according Snowflake table structure
                df = df[['PORTFOLIO_PROJECTS_ID','DATAPOINT_ID','DP_NAME','ITERATION_INDEX','VALUE']]

                #find unique list of PORTFOLIO_PROJECTS_ID so that we can avoid duplicates
                port_proj_id_list = df['PORTFOLIO_PROJECTS_ID'].unique()
                combined_port_prj = ",".join( map(str, port_proj_id_list))

                #Delete the portfolio_project id if it is already there
                query= "DELETE FROM USER_VALUES WHERE PORTFOLIO_PROJECTS_ID IN (%s)" % (combined_port_prj)
    #             snowflake_conn('PDACAPTARIO',query)

        except Exception as e:
            print(f'Error while performing transformation for {type}', e)


        try:

            if type == 'Sim-Assumptions':
                #Conact system timeseries and custom timeseries
                df = out_df.copy()

                #Join with PORTFOLIO_PROJECT table to find PORTFOLIO_PROJECT_ID
                df = df.merge(PORTFOLIO_PROJECTS_df[['PORTFOLIO_GUID','PROJECT_GUID','SIMULATION_GUID','INITIATIVE_GUID','PROGRAM_OPTION_GUID','PORTFOLIO_PROJECTS_ID']], on=["PROJECT_GUID","PORTFOLIO_GUID","SIMULATION_GUID","INITIATIVE_GUID","PROGRAM_OPTION_GUID"], how="left")#.drop(columns = ['PORTFOLIO_ID','PROJECT_ID','SIMULATION_ID','INITIATIVE_ID','PROGRAM_OPTION_ID'])

                #Change the sequence of columns according Snowflake table structure
                df = df[['DP_ACCOUNT', 'ASSUMPTION_KEY', 'DP_FORMID', 'DP_GROUP', 'DP_PHASE', 'DP_REGION', 'DP_SCENARIO', 'DP_SUB_ACCOUNT', 'DP_TYPE', 'INITIATIVE_GUID', 'PORTFOLIO_GUID', 'PROJECT_GUID', 'PROGRAM_OPTION_GUID', 'SIMULATION_GUID', 'CHILD_SIMULATION_GUID', 'DP_NAME', 'DP_PARAM1', 'IS_TIMESERIES', 'IS_MODEL_EXPRESSION', 'PORTFOLIO_PROJECTS_ID', 'DATAPOINT_ID']]

                #find unique list of PORTFOLIO_PROJECTS_ID so that we can avoid duplicates
                port_proj_id_list = df['PORTFOLIO_PROJECTS_ID'].unique()
                combined_port_prj = ",".join( map(str, port_proj_id_list))

                #Delete the portfolio_project id if it is already there
                query= "DELETE FROM USER_VALUES WHERE PORTFOLIO_PROJECTS_ID IN (%s)" % (combined_port_prj)
    #             snowflake_conn('PDACAPTARIO',query)

        except Exception as e:
            print(f'Error while performing transformation for {type}', e)
        #Append data to Output_Timeseries Snowflake table
#         write_snowflake(dataset,df)
        print(type, ": ",convert_size(sys.getsizeof(df)))
    except Exception as e:
        print('Error while performing custom_dp_transform main', e)


# In[ ]:


def Load_assumption_files(child_id,modelType,list_of_file_names,z,fileName, project_id, portfolio_id,initiative_id,program_option_id,simulation_id):
    
    #Get data from ASSUMPTION_DATAPOINT snowflake table
    ASSUMPTION_DATAPOINT_df = dataiku.Dataset("ASSUMPTION_DATAPOINT_INPUT").get_dataframe()
    
    if(modelType=='ProgramOption'):
        folder_path= 'inputs/'
#         folder_path= 'child-simulations/'+ child_id + '/inputs/'
    else:
        folder_path= 'child-simulations/'+ child_id + '/inputs/'

    file = [x for x in list_of_file_names if re.search(folder_path + fileName + '.json',x)]

    assump_dfs = []

    attribute_value_transformation = [[{'Name': 'Form ID', 'Value': 'Missing'}, {'Name': 'Region', 'Value': 'Global'}, {'Name': 'Scenario', 'Value': 'All'}]]
    for elem in file:
        if elem.startswith(folder_path) and elem.endswith(fileName + '.json'):
            out = z.open(elem)
            data = json.load(out)
            df = pd.json_normalize(data)
            # Choose only selected columns and re-name then for futher processing
            df_assump_reduced = (df[["AssumptionKey","Name","Param1","Param2","Param3","Param4","IsTimeAssumption",
                                           "IsModelExpression"]].rename(columns={'AssumptionKey':'ASSUMPTION_KEY','Name':'DP_NAME',
                                                                  'Param1':'DP_PARAM1','Param2':'DP_PARAM2',
                                                                  'Param3':'DP_PARAM3','Param4':'DP_PARAM4',
                                                                  'IsTimeAssumption':'IS_TIMESERIES',
                                                                  'IsModelExpression':'IS_MODEL_EXPRESSION'}))
            df_assump_reduced[['SIMULATION_GUID','INITIATIVE_GUID','PORTFOLIO_GUID','PROJECT_GUID',
                               'PROGRAM_OPTION_GUID','CHILD_SIMULATION_GUID']] = pd.DataFrame([[simulation_id,\
                                                                           initiative_id,\
                                                                           portfolio_id,\
                                                                           project_id,\
                                                                           program_option_id,\
                                                                           child_id]],\
                                                                         index=df_assump_reduced.index)

            df_attrs = df[['AssumptionKey','AttributeValues']]
            pd.options.mode.chained_assignment = None
            df_attrs.loc[(df_attrs['AttributeValues'].isnull()) | (df_attrs['AttributeValues'].str.len() < 1) ,"AttributeValues"]=attribute_value_transformation

            df_temp = df_attrs.set_index('AssumptionKey')
            list_df = df_temp.AttributeValues.astype(str).apply(literal_eval)

            exploded = list_df[list_df.str.len() > 0].copy().explode()
            final = pd.DataFrame(list(exploded), index=exploded.index).reset_index()

            df_attrs = df_attrs.merge(final,on='AssumptionKey').pivot(index = 'AssumptionKey', columns='Name', values='Value').reset_index().rename(columns={'AssumptionKey':'ASSUMPTION_KEY', 'Region':'DP_REGION','Phase':'DP_PHASE', 'Form ID':'DP_FORMID','Scenario':'DP_SCENARIO','Type':'DP_TYPE','Sub Account':'DP_SUB_ACCOUNT','Account':'DP_ACCOUNT','Name':'DP_GROUP'})

            df_assump_final = df_attrs.merge(df_assump_reduced,on='ASSUMPTION_KEY', how='left')
            df_assump_final.fillna({ 'DP_PHASE':'All', 'DP_REGION':'Global', 'DP_SCENARIO':'All','DP_TYPE':'NA','DP_GROUP':'Missing','DP_FORMID':'NA','DP_ACCOUNT':'Missing','DP_SUB_ACCOUNT':'Missing'}, inplace=True)
            df_assump_final = df_assump_final.merge(ASSUMPTION_DATAPOINT_df, on=['DP_NAME','DP_REGION','DP_PHASE','DP_SCENARIO'], how='left')

            # Apply integer format for float variables
            df_assump_final['DATAPOINT_ID'] = df_assump_final['DATAPOINT_ID'].fillna(0).apply(np.int64)
            assump_dfs.append(df_assump_final)
    return assump_dfs


# In[ ]:


#%%time
x=0

#Iterate through all the paths/files from source folder
for paths[x] in paths:
    #check whether the file is zip file
    if(re.search('(.*?).zip' , paths[x])):


        """
                Read Zip file without extraction
        """

        with CAPTARIO_S3_BUCKET.get_download_stream(paths[x]) as f:
            z = zipfile.ZipFile(io.BytesIO(f.read()))
            list_of_file_names = z.namelist()

            file = [x for x in list_of_file_names if re.search('manifest.json',x)]
            out = z.open(file[0])
            JsonFile = json.load(out)
            mode = JsonFile['Mode']
            modelType = JsonFile['ModelType']

            if (mode == 'Calculation' or mode == 'MonteCarloSimulation') and modelType== 'Portfolio':
                simulation_id = JsonFile['SimulationId']
                model_url= JsonFile['ModelURL']
                initiative_id = model_url.rsplit('/', 4)[1]
                portfolio_id = JsonFile['ModelId']

                #Declare list
                list1=[]
                list2=[]
                list3=[]
                list4=[]
                list5=[]
                list6=[]
                list7=[]
                list8=[]
                list9=[]
                final_account = []
                final_assumption =[]
                final_sys_timeseries=[]
                final_sys_values=[]
                final_custom_timeseries=[]
                final_custom_values=[]
                final_user_timeseries=[]
                final_user_values=[]
                final_sim_assumps=[]
                if len(JsonFile['ChildSimulations']) != 0:


                    #Iterate through all child simulations from manifest.json file
                    for i in range(len(JsonFile['ChildSimulations'])):
                        child_id = JsonFile['ChildSimulations'][i]['SimulationId']
                        project_id = JsonFile['ChildSimulations'][i]['ParentId']
                        program_option_id = JsonFile['ChildSimulations'][i]['ModelId']


                        #load account.json file
                        list1 = Load_account_files(child_id,modelType,list_of_file_names,z,'accounts','Account')
                        combined_account=pd.concat(list1)
                        final_account.append(combined_account)

                        #Load assumption.json data
                        list2 = Load_account_files(child_id,modelType,list_of_file_names,z,'assumptions','Assumption',project_id)
                        combined_assump=pd.concat(list2)
                        final_assumption.append(combined_assump)

                        #read output timeseries parquet data
                        list_3 = load_calculated_result_parquet_file(modelType,'system-timeseries/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)
                        combined_sys_timeseries=pd.concat(list_3)
                        final_sys_timeseries.append(combined_sys_timeseries)

                        #read output values parquet data
                        list_6 = load_calculated_result_parquet_file(modelType,'system-values/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)
                        combined_sys_values=pd.concat(list_6)
                        final_sys_values.append(combined_sys_values)

                        #Append Custom timeseries data to list
                        list_4 = load_calculated_result_parquet_file(modelType,'custom-timeseries/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)
                        combined_custom_timeseries=pd.concat(list_4)
                        final_custom_timeseries.append(combined_custom_timeseries)

                        #Append Custom values data to list
                        list5 = load_calculated_result_parquet_file(modelType,'custom-values/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)
                        combined_custom_values=pd.concat(list5)
                        final_custom_values.append(combined_custom_values)
                        
                        #read user timeseries parquet data
                        list_7 = load_calculated_result_parquet_file(modelType,'user-timeseries/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)
                        combined_user_timeseries=pd.concat(list_7)
                        final_user_timeseries.append(combined_user_timeseries)

                        #read user values parquet data
                        list_8 = load_calculated_result_parquet_file(modelType,'user-values/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)
                        combined_user_values=pd.concat(list_8)
                        final_user_values.append(combined_user_values)

                        #read sim assumptions values json data
                        list_9=Load_assumption_files(child_id,modelType,list_of_file_names,z,'assumptions',project_id,portfolio_id,initiative_id,program_option_id,simulation_id)
                        combined_sim_assumps=pd.concat(list_9)
                        final_sim_assumps.append(combined_sim_assumps)



            else:
                simulation_id = JsonFile['SimulationId']
                portfolio_id = JsonFile['ParentId']
                model_url= JsonFile['ModelURL']

                initiative_id = model_url.rsplit('/', 5)[1]
                child_id = JsonFile['SimulationId']
                project_id = JsonFile['ParentId']
                proj_name= JsonFile['ParentName']
                program_option_id = JsonFile['ModelId']


                #load account.json file
                final_account = Load_account_files(child_id,modelType,list_of_file_names,z,'accounts','Account')

                #Load assumption.json data
                final_assumption = Load_account_files(child_id,modelType,list_of_file_names,z,'assumptions','Assumption',project_id)

                #read output timeseries parquet data
                final_sys_timeseries = load_calculated_result_parquet_file(modelType,'system-timeseries/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)

                #read output values parquet data
                final_sys_values = load_calculated_result_parquet_file(modelType,'system-values/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)

                #Append Custom timeseries data to list
                final_custom_timeseries = load_calculated_result_parquet_file(modelType,'custom-timeseries/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)

                #Append Custom values data to list
                final_custom_values = load_calculated_result_parquet_file(modelType,'custom-values/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)
                
                #Append user timeseries data to list
                final_user_timeseries = load_calculated_result_parquet_file(modelType,'user-timeseries/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)
                
                #Append Custom values data to list    
                final_user_values = load_calculated_result_parquet_file(modelType,'user-values/',list_of_file_names,z,child_id,simulation_id,initiative_id,portfolio_id,project_id,program_option_id)

                #Append sim assumptions values json data
                final_sim_assumps=Load_assumption_files(child_id,modelType,list_of_file_names,z,'assumptions',project_id,portfolio_id,initiative_id,program_option_id,simulation_id)
                       
                
                
            ##Concat assumption data for all projects and store it in df_account1 dataframe
            df_account1= pd.concat(final_assumption, ignore_index= True)
            df_account1= (df_account1.drop_duplicates())
            assup_acc_df = pd.DataFrame(sorted(df_account1['Account'].dropna().unique()),columns =['Name'])



            #Concat account data for all projects and store it in df_account dataframe
            df_account= pd.concat(final_account, ignore_index= True)
            df_account= pd.concat([df_account,assup_acc_df], ignore_index= True)
            account_transform(df_account)

            #Dataframe to hold all the system timeseries data on Project level
            final_sys_timeseries_df= pd.concat(final_sys_timeseries, ignore_index= True)

            #Dataframe to hold all the system values data on Project level
            final_sys_values_df= pd.concat(final_sys_values, ignore_index= True)

            #Dataframe to hold all the custom timeseries data on Project level
            final_custom_timeseries_df= pd.concat(final_custom_timeseries, ignore_index= True)

            #Dataframe to hold all the custom values data on Project level
            final_custom_values_df= pd.concat(final_custom_values, ignore_index= True)

            #load data to Assumption_datapoint table
            assumption_dp_transform(df_account1,final_sys_timeseries_df,final_sys_values_df,final_custom_timeseries_df,final_custom_values_df)

            #load data to Project_Assumptions table
            project_assumption_transform(df_account1)
            
            #Dataframe to hold all the user timeseries data on Project level
            final_user_timeseries_df= pd.concat(final_user_timeseries, ignore_index= True)

            #Dataframe to hold all the user values data on Project level
            final_user_values_df= pd.concat(final_user_values, ignore_index= True)

            #Dataframe to hold all the simulation assumptions data on Project level
            final_sim_assumps_df= pd.concat(final_sim_assumps, ignore_index= True)

            '''
            Read Parquet file from System timeseries from raw data extract and do below transformation before
            loading in to Snowflake "OUTPUT_TIMESERIES' table
            '''
            custom_dp_transform(final_sys_timeseries_df,final_custom_timeseries_df,'OUTPUT_TIMESERIES','Custom-timeseries')

            '''
            Read Assumptions.json from raw data extract and do below transformation before
            loading in to output_values Snowflake table
            '''

            custom_dp_transform(final_sys_values_df,final_custom_values_df,'OUTPUT_VALUES_RK','Custom-values')
            
            
            custom_dp_transform(final_user_timeseries_df,None,'USER_TIMESERIES','User-timeseries')

            custom_dp_transform(final_user_values_df,None,'USER_VALUES','User-values')

            custom_dp_transform(final_sim_assumps_df,None,'SIMULATION_ASSUMPTIONS','Sim-Assumptions')




            '''
            Staging Table cleanup
            '''
            delete_stg_tables('STG_CUSTOM_VALUES_DP')
            delete_stg_tables('STG_CUSTOM_DATAPOINTS')

