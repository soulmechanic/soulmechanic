# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import sys
import pandas as pd
import numpy as np
from datetime import date, datetime
import datetime
import json
import requests
from requests_ntlm import HttpNtlmAuth
from IPORTFunctions import write_to_folder, jsonify

#Recieves series as input and formats the names if the series is not empty
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

#Iterate through each project and build list of dictionaries, one for each milestone, for every project code
def Group_by_Proj_Code(input_df,int_dtypes):
    uniq_proj_code = list(input_df["Project_Code"].unique())
    group_by_final_df=pd.DataFrame()
    for each_proj_code in uniq_proj_code:
        final_dict={}
        filtered_df = input_df[input_df["Project_Code"]==each_proj_code].iloc[:,1:].copy()
        dict_list = filtered_df.to_dict(orient='records')
        group_by_list = []
        for each_dict in dict_list:
            formatted_dict={}
            formatted_dict={k:v for k,v in each_dict.items() if v!='' and str(v)!='nan' }
            for col,dtype in int_dtypes.items():
                if col in formatted_dict.keys():
                    formatted_dict[col]=dtype(formatted_dict[col])
            group_by_list.append(formatted_dict)
        final_dict["Project_Code"]=each_proj_code
        final_dict["Milestones/Submissions"]=group_by_list
        group_by_final_df = group_by_final_df.append(final_dict,ignore_index=True)
    return group_by_final_df

def Replace_Special_Char_for_JSON(input_df):
    #input_df = input_df.replace(to_replace=chr(92)+chr(91), value=chr(40),regex=True)
    #input_df = input_df.replace(to_replace=chr(92)+chr(93), value=chr(41),regex=True)
    input_df = input_df.replace(to_replace=chr(92)+chr(92), value=chr(92)+chr(92),regex=True)
    input_df = input_df.replace(to_replace=chr(92)+chr(9), value=' ',regex=True)
    return input_df

if __name__== "__main__":
    mile_dataset = dataiku.Dataset("PULL_PROJ_CODE_MILESTONES")
    (names, dtypes, parse_date_columns) = mile_dataset._get_dataframe_schema()
    with mile_dataset._stream() as dku_output:
        proj_code_milestones_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    sub_dataset = dataiku.Dataset("PULL_SUBMISSIONS_DATA")
    (names, dtypes, parse_date_columns) = sub_dataset._get_dataframe_schema()
    with sub_dataset._stream() as dku_output:
        proj_code_submissions_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    drpd_dataset = dataiku.Dataset("PULL_DROPPED_PROJ_DATA")
    (names, dtypes, parse_date_columns) = drpd_dataset._get_dataframe_schema()
    with drpd_dataset._stream() as dku_output:
        dropped_projects_data_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    iport_dataset = dataiku.Dataset("PULL_IPORT_DETAIL_TAB_PSS")
    (names, dtypes, parse_date_columns) = iport_dataset._get_dataframe_schema()
    with iport_dataset._stream() as dku_output:
        pharmsci_attr_data_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    ##Adding Asset Team Dataset -12/14/2020
    roles_dataset = dataiku.Dataset("Candidate_Role")
    (names, dtypes, parse_date_columns) = roles_dataset._get_dataframe_schema()
    with roles_dataset._stream() as dku_output:
        asset_team_df = pd.read_table(dku_output, names=names, keep_default_na=False)

    #Since the above read processes are converting few numeric columns(which containts NULL and NUMERIC values) into STRING.
    int_dtypes = {'dateExcel':int,'dateNum':int,'gemExcel':int,'gemNum':int,
                  'PharmSci_Last_Published_Excel':int,'Project_Status_Date_Excel':int,'Project_RIP_Date_Excel':int,
                 'Project_Phase_Date_Excel':int,'Project_Created_Date_Excel':int}

    #Handle special characters for JSON format
    proj_code_milestones_df = Replace_Special_Char_for_JSON(proj_code_milestones_df)
    #Commenting submissions data code to supress this data from target file 06/09/2020
    #proj_code_submissions_df = Replace_Special_Char_for_JSON(proj_code_submissions_df)
    dropped_projects_data_df = Replace_Special_Char_for_JSON(dropped_projects_data_df)
    pharmsci_attr_data_df = Replace_Special_Char_for_JSON(pharmsci_attr_data_df)

    #Format names
    dropped_projects_data_df['Compound_Type'] = dropped_projects_data_df['Compound_Type'].replace('','Unknown / TBD')
    dropped_projects_data_df['Project_SPOA']=dropped_projects_data_df.apply(lambda x : Format_Name(x['Project_SPOA']), axis=1)
    dropped_projects_data_df['Project_Manager']=dropped_projects_data_df.apply(lambda x : Format_Name(x['Project_Manager']), axis=1)
    dropped_projects_data_df['Project_Planner']=dropped_projects_data_df.apply(lambda x : Format_Name(x['Project_Planner']), axis=1)
    dropped_projects_data_df['Project_Leader']=dropped_projects_data_df.apply(lambda x : Format_Name(x['Project_Leader']), axis=1)

    #Sort input dataset using proj_code values
    milestones_df_sort = proj_code_milestones_df.sort_values(by=["Project_Code"])

    #Rename column names
    milestones_df_sort = milestones_df_sort.rename(columns={'Candidate_Task_Core_Code_Short_Desc':'milestone','Candidate_Task_Core_Code':'core','Candidate_Task_Duplicate_Milestone_Descriptor':'desc','Candidate_Task_End_Date':'date','Candidate_Task_Pcnt_Comp':'pcnt','Candidate_Task_Current_GEM':'gem'})

    #Group by milestones records on project_code
    final_milestones_df = Group_by_Proj_Code(milestones_df_sort,int_dtypes)
    final_milestones_df = final_milestones_df.rename(columns = {'Milestones/Submissions':'Milestones'})

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

    Dropped_Candidates_and_Products = pd.merge(dropped_projects_data_df,final_milestones_df,on='Project_Code',how='left')

    #Commenting the submissions data code to supress this data from target file, 06/09/2020
    #Group by submissions records on project_code
    #submissions_df_sort = proj_code_submissions_df.sort_values(by=["Submission_Candidate_Code"])
    #submissions_df_sort = submissions_df_sort.rename(columns={"Submission_Candidate_Code" : "Project_Code"})
    #final_submissions_df = Group_by_Proj_Code(submissions_df_sort,int_dtypes)
    #final_submissions_df = final_submissions_df.rename(columns={'Milestones/Submissions':'Submissions'})

    #merge_cand_and_submissions_df = pd.merge(Dropped_Candidates_and_Products,final_submissions_df, on="Project_Code", how="left")

    #Dropped_Candidates_and_Products = pd.merge(merge_cand_and_submissions_df,pharmsci_attr_data_df, on="Project_Code", how="left")
    #end
    Dropped_Candidates_and_Products = pd.merge(Dropped_Candidates_and_Products,pharmsci_attr_data_df, on="Project_Code", how="left")
    ## Updated for asset_team - 12/14/2020
    Dropped_Candidates_and_Products_with_asset=pd.merge(Dropped_Candidates_and_Products,final_asset_df, on="Project_Code",how="left")
    Dropped_Cand_and_Prod_dict = Dropped_Candidates_and_Products_with_asset.to_dict(orient='records')
    ## End of update
    #Dropped_Cand_and_Prod_dict = Dropped_Candidates_and_Products.to_dict(orient='records')

    #Convert each row into a dictionary and remove keys which have empty values
    final_list = []
    for each_item in Dropped_Cand_and_Prod_dict:
        temp_dict = {}
        temp_dict={k:v for k,v in each_item.items() if v!='' and str(v)!='nan'}
        for col,dtype in int_dtypes.items():
            if col in temp_dict.keys():
                temp_dict[col]=dtype(temp_dict[col])
        final_list.append(temp_dict)
    dropped_cand_prod_json = json.dumps(final_list,indent=1,ensure_ascii=False,default=str)
    dropped_cand_prod_json = dropped_cand_prod_json.replace('\n','\r\n')
    dropped_cand_prod_output = '{"Metadata" :{"run_date": "'+date.today().strftime('%d-%b-%Y')+'","run_date_time":"'+datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')+'"},"Candidate_Dropped":'+dropped_cand_prod_json+'}'



#######################################################################################################################

 # publishing json file to S3 folder
filenameMAIN = 'DroppedData.txt'
folders = ['IPORT_DROPPED_CAND_AND_PROJS_S3_FOLDER']
write_to_folder(dropped_cand_prod_output,folders,filenameMAIN)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# #################### ***********PROD SP 2013 PUSH Json************** ###################


#     #Begin post data onto sharepoint site
#     sharePointUrl = dataiku.get_custom_variables()['sharepoint_url']
#     Auth=HttpNtlmAuth(dataiku.get_custom_variables()['DSS_ACCOUNT'],dataiku.get_custom_variables()['DSS_ACCOUNT_PWD'])

#     #Get headers
#     tSite=sharePointUrl+'/_api/contextinfo'
#     headers= {'accept': 'application/json;odata=verbose'}
#     r=requests.post(tSite, auth=Auth, headers=headers)
#     form_digest_value = r.json()['d']['GetContextWebInformation']['FormDigestValue']

#     updated_headers = {
#             "Accept":"application/json;odata=verbose",
#             "Content-Type":"application/json;odata=verbose",
#              "X-RequestDigest" : form_digest_value
#              }

#     requestUrl = dataiku.get_custom_variables()['POST_DroppedData_URL']
#     ### Commented below code writing json file untill the scenario runs and testing completes ###
#     #r=requests.put(requestUrl,data=dropped_cand_prod_output.encode('utf-8'),auth=Auth,headers=updated_headers)

#################### ***********DEV SP 2013 PUSH Json************** ###################

# #Begin post data onto sharepoint site
# sharePointUrl = dataiku.get_custom_variables()['sharepoint_url_dev']
# Auth=HttpNtlmAuth(dataiku.get_custom_variables()['DSS_ACCOUNT'],dataiku.get_custom_variables()['DSS_ACCOUNT_PWD'])

# #Get headers
# tSite=sharePointUrl+'/_api/contextinfo'
# headers= {'accept': 'application/json;odata=verbose'}
# r=requests.post(tSite, auth=Auth, headers=headers)
# form_digest_value = r.json()['d']['GetContextWebInformation']['FormDigestValue']

# updated_headers = {
#         "Accept":"application/json;odata=verbose",
#         "Content-Type":"application/json;odata=verbose",
#          "X-RequestDigest" : form_digest_value
#          }

# devRequestUrl = dataiku.get_custom_variables()['POST_DroppedData_URL_dev']
# ### Temp Implementation ###
# r=requests.put(devRequestUrl,data=dropped_cand_prod_output.encode('utf-8'),auth=Auth,headers=updated_headers)

# # Updated to include asset_team - 12/14/2020
# dropped_Candidates_and_Products_df = Dropped_Candidates_and_Products_with_asset # For this sample code, simply copy input to output
# #dropped_Candidates_and_Products_df = Dropped_Candidates_and_Products
# Dropped_Candidates_and_Products = dataiku.Dataset("DROPPED_CAND_AND_PROJS")
# Dropped_Candidates_and_Products.write_with_schema(dropped_Candidates_and_Products_df)
