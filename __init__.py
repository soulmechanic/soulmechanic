# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import numpy as np
from datetime import date
from datetime import datetime, timedelta
import json

DSS_ENV_URL = dataiku.get_custom_variables()["DSS_ENV_URL"]


# Function to generate dict with Key column as separate column
def gen_dict_for_each_id(df,dict_col,key_col):
    try:
        unique_list = list(df[key_col].unique())
        Final_lst=[]
        for each_id in unique_list:
            final_dict={}
            F_DF = df[df[key_col]==each_id].iloc[:,1:].copy()
            F_DICT = [{k:v for k,v in m.items() if pd.notnull(v) and v!="" and str(v)!='nan'} for m in F_DF.to_dict(orient='records')]
            final_dict[key_col]=each_id
            final_dict[dict_col]=F_DICT
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        return 'Unable to find', '->', str(e)

def format_name(df,name_col):
    df[name_col] = df[name_col].str.replace(', ',',').str.replace(',',', ')
    return df[name_col]

def jsonify(df,receipe_name,root_element):
    try:
        source = DSS_ENV_URL +'/projects/GBL_DAI_IDASH/recipes/'+receipe_name+'/'

        #convert df to json,dropping NA elements
        _json = (json.dumps([row.dropna().to_dict() for index,row in df.iterrows()],indent=1,ensure_ascii=False,default=str))

        #tidy json and add metadata wrapper
        _json = _json.replace('\n','\r\n')

        #NOTE: original JSON for this component did not have Metadata tag
        _json = ('{"Metadata" :{"run_date": "'+date.today().strftime('%d-%b-%Y')+'","run_date_time":"'+datetime.now()
                 .strftime('%m/%d/%Y %H:%M:%S')+'", "Source": "' + source + '"},'+'"'+root_element+'"'+':'+ _json +'}')

        return _json
    except Exception as e:
        return 'Unable to write to folder:', '->', str(e)
        
        
def write_to_folder(JsonFile,folders,FileNameMain):
    
    for folder in folders: 
        # push Json to SharePoint Online
        try:
            filename = '/' + FileNameMain
            handle = dataiku.Folder(folder)

            with handle.get_writer(filename) as w:
                w.write(JsonFile.encode('utf-8'))
        except Exception as e:
            return 'Unable to write to folder:', '->', str(e)