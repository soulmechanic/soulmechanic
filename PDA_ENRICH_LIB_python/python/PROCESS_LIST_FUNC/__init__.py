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
        print(current_run_details)
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
        project_variables["local"]["Actual_Process_id"] = process_id
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