# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import sys
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime, timedelta
import datetime
import json
import requests
from requests_ntlm import HttpNtlmAuth
from IPORTFunctions import write_to_folder


#Recieves name value as input and formats the names if the value is not empty
#Ex : Painter,Carla -> Carla Painter
#Ex : Moore,Danielle;Painter,Carla -> Danielle Moore;Carla Painter
def Format_Name(name_value):
    if pd.isna(name_value) or name_value.strip() == '' :
        return name_value
    elif name_value.find(';') > 0:
        names_list = []
        frmt_name_lst = []
        names_list=name_value.split(';')
        for each_name in names_list:
            temp_lst = []
            temp_lst = each_name.rsplit(',',1)
            temp_lst.reverse()
            frmt_name_lst.append(' '.join(map(str.strip,temp_lst)))
        return ';'.join(frmt_name_lst)
    elif name_value.find(';') < 0:
        temp_lst = []
        temp_lst = name_value.rsplit(',',1)
        temp_lst.reverse()
        frmt_name = ' '.join(map(str.strip,temp_lst))
        return frmt_name

def Replace_Special_Char_for_JSON(input_df):
    input_df = input_df.replace(to_replace=chr(92)+chr(91), value=chr(40),regex=True) #[ -> (
    input_df = input_df.replace(to_replace=chr(92)+chr(93), value=chr(41),regex=True) #] -> )
    input_df = input_df.replace(to_replace=chr(92)+chr(92), value=chr(92)+chr(92),regex=True) #\ -> \\
    input_df = input_df.replace(to_replace=chr(92)+chr(9), value=' ',regex=True) # \t -> ' '
    return input_df

if __name__== "__main__":
    # Read recipe inputs
    iport_dataset = dataiku.Dataset("PULL_DISC_CAND_PROJ_DATA_DEV")
    (names, dtypes, parse_date_columns) = iport_dataset._get_dataframe_schema()
    with iport_dataset._stream() as dku_output:
        iport2Candidate_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    handle_int_dtypes={'Project_Status_Date_Excel':int,'Project_Phase_Date_Excel':int,'Project_Created_Date_Excel':int}
    for col,dtype in handle_int_dtypes.items():
        iport2Candidate_df[col]=iport2Candidate_df[col].replace(to_replace='',value=0)
        iport2Candidate_df[col]=iport2Candidate_df[col].astype(dtype)

    mile_dataset = dataiku.Dataset("PULL_PROJ_CODE_MILESTONES")
    (names, dtypes, parse_date_columns) = mile_dataset._get_dataframe_schema()
    with mile_dataset._stream() as dku_output:
        proj_CODE_MILESTONES_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    sub_dataset = dataiku.Dataset("PULL_SUBMISSIONS_DATA")
    (names, dtypes, parse_date_columns) = sub_dataset._get_dataframe_schema()
    with sub_dataset._stream() as dku_output:
        submissions_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    iport_dataset = dataiku.Dataset("PULL_IPORT_DETAIL_TAB_PSS")
    (names, dtypes, parse_date_columns) = iport_dataset._get_dataframe_schema()
    with iport_dataset._stream() as dku_output:
        pharmsci_attr_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    ##Adding Asset Team Dataset -12/14/2020
    roles_dataset = dataiku.Dataset("Candidate_Role")
    (names, dtypes, parse_date_columns) = roles_dataset._get_dataframe_schema()
    with roles_dataset._stream() as dku_output:
        asset_team_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    #Since the above read processes are converting few numeric columns(which containts both NULLs and NUMERIC values) into STRING.
    int_dtypes = {'Project_Status_Date_Excel':int,'Project_Phase_Date_Excel':int,'Project_Created_Date_Excel':int,
                 'dateExcel':int,'dateNum':int,'gemExcel':int,'gemNum':int,'PharmSci_Last_Published_Excel':int}

    #Sort input dataset using proj_code values
    sorted_df = proj_CODE_MILESTONES_df.sort_values(by=["Project_Code"])
    #Handle special characters for JSON format
    sorted_df = Replace_Special_Char_for_JSON(sorted_df)
    pharmsci_attr_df = Replace_Special_Char_for_JSON(pharmsci_attr_df)

    #Replace values in Candidate_Task_Core_Code_Short_Desc
    sorted_df["Candidate_Task_Core_Code_Short_Desc"] = sorted_df["Candidate_Task_Core_Code_Short_Desc"].replace('ChinaSubmission','China Submission')
    sorted_df["Candidate_Task_Core_Code_Short_Desc"] = sorted_df["Candidate_Task_Core_Code_Short_Desc"].replace('Pos. ESoE','Pos ESoE')
    sorted_df["Candidate_Task_Core_Code_Short_Desc"] = sorted_df["Candidate_Task_Core_Code_Short_Desc"].replace('Cmp Strt IDd','Compound Selected')

    #Re format names
    iport2Candidate_df['Project_Manager']=iport2Candidate_df.apply(lambda x:Format_Name(x['Project_Manager']), axis=1)
    iport2Candidate_df['Project_Planner']=iport2Candidate_df.apply(lambda x:Format_Name(x['Project_Planner']), axis=1)
    iport2Candidate_df['Project_Leader']=iport2Candidate_df.apply(lambda x:Format_Name(x['Project_Leader']), axis=1)

    #Rename column names
    sorted_df = sorted_df.rename(columns={'Candidate_Task_Core_Code_Short_Desc':'milestone','Candidate_Task_Core_Code':'core','Candidate_Task_Duplicate_Milestone_Descriptor':'desc','Candidate_Task_End_Date':'date','Candidate_Task_Pcnt_Comp':'pcnt','Candidate_Task_Current_GEM':'gem'})

    #Start Building Milestones dataset
    #Iterate through all milestones in each project and create a list of dictionaries for each milestone in a project
    uniq_proj_code = list(sorted_df["Project_Code"].unique())
    final_milestones_lst=[]
    for each_proj_code in uniq_proj_code:
        final_dict={}
        filtered_df = sorted_df[sorted_df["Project_Code"]==each_proj_code].iloc[:,1:].copy()
        #filtered_df["date"]=filtered_df["date"].astype(str)+' 0:0:0'
        dict_list = filtered_df.to_dict(orient='records')
        milestones_list = []
        for each_dict in dict_list:
            temp_dict_mile={}
            temp_dict_mile={k:v for k,v in each_dict.items() if v!='' and str(v)!='nan'}
            for col,dtype in int_dtypes.items():
                if col in temp_dict_mile.keys():
                    temp_dict_mile[col]=dtype(temp_dict_mile[col])
            milestones_list.append(temp_dict_mile)
        final_dict["Project_Code"]=each_proj_code
        final_dict["Milestones"]=milestones_list
        final_milestones_lst.append(final_dict)

    final_milestones_df = pd.DataFrame(final_milestones_lst)
    #End Milestones code

    ##CODE TO APPEND ASSET TEAM TO JSON FILE - 12/14/2020;Stallon
    proj_code_list=list(asset_team_df['Code'].unique())
    final_asset_team_list=[]
    final_columns={"Role_Name":"role","Display_Name":"name"}
    for each_code in proj_code_list:
        sliced_asset_team_df=asset_team_df[asset_team_df['Code']==each_code].iloc[:,1:].copy()
        roles_dict=sliced_asset_team_df.to_dict(orient="records")
        final_roles_dict={}
        final_roles_list=[]
        for each_role in roles_dict:
            temp_dict={}
            temp_dict = {final_columns[k]:v for k,v in each_role.items() if k in final_columns.keys() and (v !='' or str(v)!='nan')}
            #temp_dict = {k:v for k,v in each_role.items() if v !='' or str(v)!='nan'}
            final_roles_list.append(temp_dict)
        final_roles_dict['Project_Code']=each_code
        final_roles_dict['Asset_Team']=final_roles_list
        final_asset_team_list.append(final_roles_dict)
    final_asset_df=pd.DataFrame(final_asset_team_list)
    ## END OF ASSET TEAM CODE

    #Commenting submissions code to supress this data from target file, 06/09/2020
    #Start building Submissions code
    #submissions_df = submissions_df.rename(columns={"Submission_Candidate_Code" : "Project_Code"})
    #sub_sorted_df = submissions_df.sort_values(by=["Project_Code"])
    #Handle special characters for JSON format
    #sub_sorted_df = Replace_Special_Char_for_JSON(sub_sorted_df)
    #Below conversions is to handle date values as they were converting to timestamp('mm-dd-yyyy 00:00:00') format
    #sub_sorted_df["Submission_Start"]=sub_sorted_df["Submission_Start"].astype(str)
    #sub_sorted_df["Submission_Finish"]=sub_sorted_df["Submission_Finish"].astype(str)
    #sub_sorted_df["Submission_Act_Start"]=sub_sorted_df["Submission_Act_Start"].astype(str)
    #sub_sorted_df["Submission_Created"]=sub_sorted_df["Submission_Created"].astype(str)
    #sub_sorted_df["Submission_Modified"]=sub_sorted_df["Submission_Modified"].astype(str)

    #Loop through each submissions_candidate_code
    #Produce a list of dictionries for each cand_code records
    #uniq_subm_cand_code = list(sub_sorted_df["Project_Code"].unique())
    #final_sub_df=pd.DataFrame()
    #for each_subm_cand_code in uniq_subm_cand_code:
    #    temp_sub_dict={}
    #    filtered_df = sub_sorted_df[sub_sorted_df["Project_Code"]==each_subm_cand_code].iloc[:,1:].copy()
    #    dict_list = filtered_df.to_dict(orient='records')
    #    submissions_list = []
    #    for each_dict in dict_list:
    #        new_dict={}
    #        new_dict={k:v for k,v in each_dict.items() if v!='' and str(v)!='nan'}
    #        submissions_list.append(new_dict)
    #    temp_sub_dict["Project_Code"]=each_subm_cand_code
    #    temp_sub_dict["Submissions"]=submissions_list
    #    final_sub_df = final_sub_df.append(temp_sub_dict,ignore_index=True)

    iport_sorted_df = iport2Candidate_df.sort_values(by=["Project_Code"])
    iport_milestones_join_df = pd.merge(iport_sorted_df,final_milestones_df,on="Project_Code",how="left")
    #iport_submission_join_df = pd.merge(iport_milestones_join_df,final_sub_df, on="Project_Code",how="left")
    #final_join_df = pd.merge(iport_submission_join_df,pharmsci_attr_df, on="Project_Code",how="left")
    final_join_df = pd.merge(iport_milestones_join_df,pharmsci_attr_df, on="Project_Code",how="left")
    ## Updated for asset_team - 12/14/2020
    final_join_with_asset_df=pd.merge(final_join_df,final_asset_df, on="Project_Code",how="left" )
    final_dict_lst = final_join_with_asset_df.to_dict(orient='records')
    ## End of update
    #final_dict_lst = final_join_df.to_dict(orient='records')
    final_list=[]
    for each_dict in final_dict_lst:
        new_dict={}
        new_dict={k:v for k,v in each_dict.items() if v!='' and str(v)!='nan'}
        for col,dtype in int_dtypes.items():
            if col in new_dict.keys():
                new_dict[col]=dtype(new_dict[col])
        final_list.append(new_dict)
    candidates_json = json.dumps(final_list,indent=1,ensure_ascii=False,default=str)
    candidates_json = candidates_json.replace('\n','\r\n')
    final_formatted_data = ('{"Metadata" :{"run_date": "'+date.today().strftime('%d-%b-%Y')+'","run_date_time":"'+datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')+'"},"Candidate":'+candidates_json+'}')

#######################################################################################################################

 # publishing json file to S3 folder
filenameMAIN = 'PortfolioData.txt'
folders = ['IPORT_PROJECTS_DATA_S3_FOLDER']
write_to_folder(final_formatted_data,folders,filenameMAIN)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# #################### ***********PROD SP 2013 PUSH Json************** ###################

#     #Below code to post data onto sharepoint site
#     sharePointUrl = dataiku.get_custom_variables()['sharepoint_url']
#     Auth=HttpNtlmAuth(dataiku.get_custom_variables()['DSS_ACCOUNT'],dataiku.get_custom_variables()['DSS_ACCOUNT_PWD'])

#     #Get header values
#     tSite=sharePointUrl+'/_api/contextinfo'
#     headers= {'accept': 'application/json;odata=verbose'}
#     r=requests.post(tSite, auth=Auth, headers=headers)
#     form_digest_value = r.json()['d']['GetContextWebInformation']['FormDigestValue']

#     #Update header values
#     updated_headers = {
#             "Accept":"application/json;odata=verbose",
#             "Content-Type":"application/json;odata=verbose",
#              "X-RequestDigest" : form_digest_value
#              }

#     requestUrl = dataiku.get_custom_variables()['POST_PortfolioData_URL']
#     ### Commented below code writing json file untill the scenario runs and testing completes ###
#     # r=requests.put(requestUrl,data=final_formatted_data.encode('utf-8'),auth=Auth,headers=updated_headers)



#################### ***********DEV SP 2013 PUSH Json************** ###################


# #Below code to post data onto sharepoint site
# sharePointUrl = dataiku.get_custom_variables()['sharepoint_url_dev']
# Auth=HttpNtlmAuth(dataiku.get_custom_variables()['DSS_ACCOUNT'],dataiku.get_custom_variables()['DSS_ACCOUNT_PWD'])

# #Get header values
# tSite=sharePointUrl+'/_api/contextinfo'
# headers= {'accept': 'application/json;odata=verbose'}
# r=requests.post(tSite, auth=Auth, headers=headers)
# form_digest_value = r.json()['d']['GetContextWebInformation']['FormDigestValue']

# #Update header values
# updated_headers = {
#         "Accept":"application/json;odata=verbose",
#         "Content-Type":"application/json;odata=verbose",
#          "X-RequestDigest" : form_digest_value
#          }

# devRequestUrl = dataiku.get_custom_variables()['POST_PortfolioData_URL_dev']
# ### Temp Implementation ###
# r=requests.put(devRequestUrl,data=final_formatted_data.encode('utf-8'),auth=Auth,headers=updated_headers)


# Write recipe outputs
#portfolio_data = dataiku.Dataset("PROJECTS_DATA")
#portfolio_data.write_with_schema(final_join_with_asset_df)  ##Updated for asset_team on 12/14/2020
#portfolio_data.write_with_schema(final_join_df)
