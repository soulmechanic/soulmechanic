# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import json
from IDASHFunctions import write_to_folder, jsonify
from jsonfunctions import prep_for_JSON

# Read recipe inputs
GMP_DASHBOARD_STG_df = dataiku.Dataset("GMP_DASHBOARD_STG").get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Function to generate dict with Key column as separate column
def gen_dict_for_each_id(df,dict_col,key_col1,key_col2):
    try:
        p_unique_list = list(df[key_col1].unique())
        c_unique_list = list(df[key_col2].unique())
        Final_lst=[]
        for p_code in p_unique_list:
            final_dict={}
            F_DF1 = df[df[key_col1]==p_code].iloc[:,1:].copy()

            my_dict = (F_DF1.groupby(['Code','Category', 'Project_Name','Discovery_Code'])
                       .apply(lambda x: x[['name','core', 'desc', 'date', 'pcnt']].to_dict('records'))
                       .reset_index()
                       .rename(columns={0:'Milestones'})
                       .to_dict(orient='records'))

            final_dict[key_col1]=p_code
            final_dict[dict_col]=my_dict
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        return 'Unable to find', '->', str(e)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
GMP_DASHBOARD_STG_df = GMP_DASHBOARD_STG_df.applymap(prep_for_JSON).sort_values(by=['Program_Code']).fillna('nan')

GMP_DF = gen_dict_for_each_id(GMP_DASHBOARD_STG_df,"Projects","Program_Code","Code")

GMP_Dashboard_json = jsonify(GMP_DF,'GMP_PYTHON_RECIPE','GMP_DATA')
# print(GMP_Dashboard_json)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# REMOVE NAN VALUES FROM JSON
GMP_Dashboard_json = json.loads(GMP_Dashboard_json)
try:
    for obj in GMP_Dashboard_json['GMP_DATA']:
        for dict_1 in obj['Projects']:

            for key,values in dict_1.copy().items():
                if(str(values) == "nan"):
                    del dict_1[key]

                if(str(key) == "Milestones"):
                    for list_ in dict_1[key]:
                        for key_2, val_2 in list_.copy().items():
                            if(str(val_2)=="nan"):
                                del list_[key_2]
except Exception as e:
    print('Unable to find', '->', str(e))

GMP_Dashboard_json = json.dumps(GMP_Dashboard_json,indent=1,ensure_ascii=False,default=str)
# print(GMP_Dashboard_json)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# push json to S3 folder and SharePoint online folder
filenameMAIN = 'GMP_Dashboard_DSS.txt'
folders = ['DEV_GMP_SPONLINE_FOLDER','PROD_GMP_SPONLINE_FOLDER']
write_to_folder(GMP_Dashboard_json,folders,filenameMAIN)
