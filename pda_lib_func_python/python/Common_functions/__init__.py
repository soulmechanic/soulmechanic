import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import time
import logging
import multiprocessing


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
    df = num_inplace(df)
    df = dt_inplace(df)
    df = dt_inplace(df)
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
            
