# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

from datetime import date, datetime
import json
import requests
from requests_ntlm import HttpNtlmAuth
from IPORTFunctions import write_to_folder, jsonify

#Recieves series as input and formats the names if the series is not empty
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

#Appends escape character to special characters like \b,\n,\r,\,\t,[,],"
def Replace_Special_Char_for_JSON(input_df):
    input_df = input_df.replace(to_replace=chr(92)+chr(9), value=' ',regex=True) #\t
    input_df = input_df.replace(to_replace=chr(92)+chr(91), value='(',regex=True) #[
    input_df = input_df.replace(to_replace=chr(92)+chr(93), value=')',regex=True) #]
    return input_df

# Function to replace float or str datatype values in a dict or list with int data type values
class Decoder(json.JSONDecoder):
    def decode(self, s):
        result = super().decode(s)
        return self._decode(result)

    def _decode(self, o):
        if isinstance(o, str) or isinstance(o, float):
            try:
                return int(o)
            except ValueError:
                return o
        elif isinstance(o, dict):
            return {k: self._decode(v) for k, v in o.items()}
        elif isinstance(o, list):
            return [self._decode(v) for v in o]
        else:
            return o

# Read recipe inputs
STUDY_75PCNT_ENROLLMENT_DATA_df = dataiku.Dataset("STUDY_75PCNT_ENROLLMENT_DATA").get_dataframe()

stdy_dataset = dataiku.Dataset("PULL_STUDY_DATA_DEV_MAIN")
(names, dtypes, parse_date_columns) = stdy_dataset._get_dataframe_schema()
with stdy_dataset._stream() as dku_output:
    study_data_df = pd.read_table(dku_output, names=names, keep_default_na=False)

study_data_df = study_data_df.merge(STUDY_75PCNT_ENROLLMENT_DATA_df[['Study_Number','Study_Seventy_Five_Pcnt_Randomized_Date',
                                                                     'Study_Pcnt_Comp_SeventyFive_Pcnt_Enrollment']],
                                    on='Study_Number',how='left')


#The stream method above would read all columns as OBJECT datatype
#Below code would handle integer,float datatype columns
int_dtypes = {'Study_Subj_Entered_Enrolled':int,'Study_Subj_Entered_Randomized':int,'Study_Tot_Subj_Active':int,
              'Study_Tot_Subj_Complete_Study':int,'Study_Tot_Subj_Disc':int,'Study_Tot_Sites_Enrolled':int,'Study_Tot_Sites_Active':int,
              'Study_Planned_No_Subjects':int, 'Study_Planned_No_Centers':int, 'Study_Target_No_Completers':int,
              'Study_Planned_Dur_Subj_Entered':int, 'Study_Planned_Dur_Subj_Hours':int, 'Study_Planned_Dur_Subj_Days': float,
              'Study_Project_Plan_System_ID':int,'Study_Status_Assess_Date_Excel':int}

study_data_frmt_df = Replace_Special_Char_for_JSON(study_data_df)

# Split milestone columns from 103 into a new dataframe, including the new CSTL role - updated 12/14/2020
study_data_master_df = study_data_frmt_df.iloc[:,:103]
study_data_child_df = study_data_frmt_df.iloc[:,103:].fillna('')


study_data_master_df['Study_CPM'] = study_data_master_df.apply(lambda x : Format_Name(x['Study_CPM']),axis = 1)
study_data_master_df['Study_Project_Planner'] = study_data_master_df.apply(lambda x : Format_Name(x['Study_Project_Planner']),axis = 1)
study_data_master_df['Study_Clinician'] = study_data_master_df.apply(lambda x : Format_Name(x['Study_Clinician']),axis = 1)

# List of milestones along with their percentage complete and source attributes in same order
Milestones = ['Study_Outline_Date','Study_Finalize_Lock_Core_Protocol_Date','Study_Final_Approved_Protocol_Date','Study_Work_Order_BL_Date','Study_SAP_Start_Date','Study_SAP_Finish_Date','Study_CRF_Approved_Date','Study_Database_Ready_Date','Study_First_Drug_Shipped_Date','Study_Fifty_Pcnt_Sites_Active_Date','Study_FSFV_Date','Study_Twenty_Five_Pcnt_Subjects_Randomized_Date','Study_FSFD_Date','Study_Fifty_Pcnt_Subjects_Randomized_Date','Study_LSFV_Date','Study_LSLV_Date','Study_Database_Release_Date','Study_DB_Release_Suppl_Date','Study_Primary_Completion_Date','Study_TLR_Date','Study_Final_Tables_Date','Study_Draft_CSR_Date','Study_Final_CSR_Min_Date','Study_Final_CSR_Max_Date','Study_Final_Suppl_CSR_Date','Study_Seventy_Five_Pcnt_Randomized_Date']
Milestones_pcnt_cmplt = ['Study_Pcnt_Comp_Study_Outline','Study_Pcnt_Comp_Finalize_Lock_Core_Protocol','Study_Pcnt_Comp_FAP','Pcnt_Comp_Study_Work_Order_BL','','Study_Pcnt_Comp_SAP','Study_Pcnt_Comp_CRF_Approved','Study_Pcnt_Comp_Database_Ready','Study_Pcnt_Comp_First_Drug_Shipped','Study_Pcnt_Comp_Fifty_Pcnt_Sites_Active','Study_Pcnt_Comp_FSFV','Study_Pcnt_Comp_TwentyFive_Pcnt_Subjects_Randomized','Study_Pcnt_Comp_FSFD','Study_Pcnt_Comp_Fifty_Pcnt_Enrollment','Study_Pcnt_Comp_LSFV','Study_Pcnt_Comp_LSLV','Study_Pcnt_Comp_Database_Release','Study_Pcnt_Comp_DB_Release_Suppl','Pcnt_Comp_Study_Primary_Completion','Study_Pcnt_Comp_TLR','Study_Pcnt_Comp_Final_Tables','Study_Pcnt_Comp_Draft_CSR','Study_Pcnt_Comp_Final_CSR','Study_Pcnt_Comp_Final_CSR','Study_Pcnt_Comp_Final_Suppl_CSR','Study_Pcnt_Comp_SeventyFive_Pcnt_Enrollment']
Milestones_srcs = ['','','Study_Final_Approved_Protocol_Source','','','','','','','','','','','','','Study_LSLV_Source','','','','','','','Study_Final_CSR_Source','Study_Final_CSR_Source','','']

Milestones_dtry_2 = {}
mile_der_lst=[]

#Iterate over milestones dataframe by each record
for index,row in study_data_child_df.iterrows():
    #study_data_child_df_2.loc[index,'Study_Number'] = row['Study_Number_1']
    Milestones_dtry_2 = {}
    Milestones_dtry_2['Study_Number'] = row['Study_Number_1']
    #Iterate over each milestone in the record
    #if value found for corresponding milestone then derive datestring,datenum,dateexcel,pcnt,src values and store them in dictionary as key value pairs
    #for each milestone the dictionary looks like : {Study_Number : 'A6181018', <milestone name> : {"date": "2003-01-16 00:00:00", "dateString": "16-Jan-2003", "dateNum": 1042675200, "dateExcel": 37637, "pcnt": 100.0}}
    #if value not found for corresponding milestone then assign NULL as value to milestone key and store it in dictionary as key value pair
    for each_attr in Milestones:
        if pd.notnull(row[each_attr]) and row[each_attr].strip()!='':
            Milestones_dtry = {}
            Milestones_dtry_notnull_values = {}
            Milestone_Date = pd.to_datetime(row[each_attr]).date()
            Milestones_dtry['date'] = Milestone_Date
            Milestone_index = Milestones.index(each_attr)
            pcnt_cmplt_attr = Milestones_pcnt_cmplt[Milestone_index]
            src_attr = Milestones_srcs[Milestone_index]
            Milestones_dtry['dateString'] = Milestone_Date.strftime('%d-%b-%Y')
            Milestones_dtry['dateNum'] = str((Milestone_Date - date(1970,1,1)).days * 24 * 3600)
            Milestones_dtry['dateExcel'] = (Milestone_Date - date(1899,12,30)).days
            if pcnt_cmplt_attr != '' and pd.to_numeric(row[pcnt_cmplt_attr]) >= 0 :
                Milestones_dtry['pcnt'] = int(row[pcnt_cmplt_attr])
            else:
                Milestones_dtry['pcnt'] = np.NaN
            if src_attr != '' and row[src_attr]:
                Milestones_dtry['src'] = row[src_attr]
                
            else :
                Milestones_dtry['src'] = np.NaN
                
            #Remove keys with NULL values
            Milestones_dtry_notnull_values = {k:v for k,v in Milestones_dtry.items() if v!='' and str(v)!='nan'}
            for col,dtype in int_dtypes.items():
                if col in Milestones_dtry_notnull_values.keys():
                    Milestones_dtry_notnull_values[col]=dtype(Milestones_dtry_notnull_values[col])
            Milestones_dtry_2[each_attr] = Milestones_dtry_notnull_values
        else:
            Milestones_dtry_2[each_attr] = np.NaN
    #After building key value pairs for all milestones in a study then insert that dictionary into a dataframe
    mile_der_lst.append(Milestones_dtry_2)

study_data_child_df_2=pd.DataFrame(mile_der_lst)



#Build final dataframe
#Iterate through each record, remove NULL attributes if any in the record, and append the record to a list
study_data_formatted_df = pd.merge(study_data_master_df,study_data_child_df_2, on="Study_Number",how="inner")
study_data_formatted_df = pd.merge(study_data_formatted_df,study_data_df[['Study_Number','PLW_Export']], on="Study_Number"
                                   ,how="inner")

# display(study_data_formatted_df)
study_data_final_dtry = study_data_formatted_df.to_dict(orient='records')
final_list_items = []
for each_item in study_data_final_dtry:
    temp_dtry = {}
    temp_dtry = {k:v for k,v in each_item.items() if str(v)!='nan' and v!='' and v!=pd.NaT}
    for col,dtype in int_dtypes.items():
        if col in temp_dtry.keys():
            temp_dtry[col]=dtype(temp_dtry[col])
    final_list_items.append(temp_dtry)


#Convert the final list into a JSON string
study_json = json.dumps(final_list_items,indent=1,ensure_ascii=False,default=str)
study_json = study_json.replace('\n','\r\n')
study_output = '{"Metadata" :{"run_date": "'+date.today().strftime('%d-%b-%Y')+'","run_date_time":"'+datetime.now().strftime('%m/%d/%Y %H:%M:%S')+'"},"Study":'+study_json+'}'

# converting all the float values to int
try:
    StudyData_json=json.loads(study_output, cls=Decoder)
    StudyData_json=json.dumps(StudyData_json,indent=1,ensure_ascii=False,default=str)
except Exception as e:
        print('Unable to find', '->', str(e))

#######################################################################################################################

 # publishing json file to S3 folder
filenameMAIN = 'StudyData.txt'
folders = ['IPORT_STUDY_DATA_S3_FOLDER']
write_to_folder(StudyData_json,folders,filenameMAIN)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# #################### ***********PROD SP 2013 PUSH Json************** ###################

# sharePointUrl = dataiku.get_custom_variables()['sharepoint_url']
# Auth=HttpNtlmAuth(dataiku.get_custom_variables()['DSS_ACCOUNT'],dataiku.get_custom_variables()['DSS_ACCOUNT_PWD'])

# tSite=sharePointUrl+'/_api/contextinfo'
# headers= {'accept': 'application/json;odata=verbose'}
# r=requests.post(tSite, auth=Auth, headers=headers)
# form_digest_value = r.json()['d']['GetContextWebInformation']['FormDigestValue']

# updated_headers = {
#     "Accept":"application/json;odata=verbose",
#     "Content-Type":"application/json;odata=verbose",
#     "X-RequestDigest" : form_digest_value
#     }



# requestUrl = dataiku.get_custom_variables()['POST_StudyData_URL']

# ### Commented below code writing json file untill the scenario runs and testing completes ###
# # r=requests.put(requestUrl,data=study_output.encode("utf-8"),auth=Auth,headers=updated_headers)


#################### ***********DEV SP 2013 PUSH Json************** ###################

# sharePointUrl = dataiku.get_custom_variables()['sharepoint_url_dev']
# Auth=HttpNtlmAuth(dataiku.get_custom_variables()['DSS_ACCOUNT'],dataiku.get_custom_variables()['DSS_ACCOUNT_PWD'])

# tSite=sharePointUrl+'/_api/contextinfo'
# headers= {'accept': 'application/json;odata=verbose'}
# r=requests.post(tSite, auth=Auth, headers=headers)
# form_digest_value = r.json()['d']['GetContextWebInformation']['FormDigestValue']

# updated_headers = {
#     "Accept":"application/json;odata=verbose",
#     "Content-Type":"application/json;odata=verbose",
#     "X-RequestDigest" : form_digest_value
#     }


# devRequestUrl = dataiku.get_custom_variables()['POST_StudyData_URL_dev']

# ### Temp. Implementation ###
# r=requests.put(devRequestUrl,data=study_output.encode("utf-8"),auth=Auth,headers=updated_headers)

# study_data_final_df = study_data_formatted_df # For this sample code, simply copy input to output

# Write recipe outputs
#study_data_final = dataiku.Dataset("STUDY_DATA")
#study_data_final.write_with_schema(study_data_final_df)


# # filenameMAIN = 'StudyData.txt'

# # try:
# #     filename = '/' + filenameMAIN
# #     handle = dataiku.Folder('I6sAYC3W')

# #     with handle.get_writer(filename) as w:
# #         w.write(study_output.encode('utf-8'))
# # except Exception as e:
# #         print('Unable to write to sharepoint due:', '->', str(e))
# #         pass

# user = dataiku.get_custom_variables()['SHAREPOINT_ACCOUNT']
# password = dataiku.get_custom_variables()['SHAREPOINT_ACCOUNT_PWD']

# filenameMAIN = 'StudyData.txt'

# dev_base_url = 'http://ecfd13.pfizer.com/sites/atcc/iPortStudy'#http://ecfd13.pfizer.com/sites/atcc/iPortStudy/assets/data
# dev_library_path = '/sites/atcc/iPortStudy/assets/data'



# #push_JSON_to_sharepoint DEV ENV is a Library funtion in this project
# resultMain = push_JSON_to_sharepoint(dev_base_url, user, password, dev_library_path, study_output, filenameMAIN)


# # prod_base_url = 'http://ecfd13.pfizer.com/sites/portfolioreporting/iportstudy'#http://ecfd13.pfizer.com/sites/portfolioreporting/iportstudy/assets/data
# # prod_library_path = '/sites/portfolioreporting/iportstudy/assets/data'

# # #push_JSON_to_sharepoint PROD ENV is a Library funtion in this project
# # resultMain = push_JSON_to_sharepoint(prod_base_url, user, password, prod_library_path, study_output, filenameMAIN)
