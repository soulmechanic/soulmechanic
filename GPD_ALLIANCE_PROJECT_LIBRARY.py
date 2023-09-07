# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import numpy as np
from datetime import date
from datetime import datetime, timedelta
import json



def prep_for_JSON(value):
    try:
        if isinstance(value, date):
            #print(type(value))
            if not pd.isnull(value):
                return value.strftime('%Y/%m/%d')
            else: #empty dates are still date instances, will be set to none here
                pass 
        else:
            return value

    except:
        return value


def jsonify(df,receipe_name,root_element):
    try:
        
        DSS_ENV_URL = dataiku.get_custom_variables()["DSS_ENV_URL"]
        ProjectName = dataiku.get_custom_variables()["projectKey"]
        
        source = DSS_ENV_URL +'/projects/'+ProjectName+'/recipes/'+receipe_name+'/'

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
        
def FilterByCode(DF,F_DF):
    CODE_DF = DF.set_index('Code')#.dropna(subset=['DiscoveryFinanceCode'])
    DISC_DF = DF.set_index('DiscoveryFinanceCode')
    F_DF = F_DF.set_index('FEAResearchCode')
    
    # If no Project code created yet then milestones will be fetched based on Research code.
    F1_DF = CODE_DF[CODE_DF.index.isin(F_DF.index)].reset_index()
    
    # If Project code is available then milestones will be fetched based on Project Code.
    F2_DF = DISC_DF[DISC_DF.index.isin(F_DF.index)].reset_index()
    
    FINAL_DF = F1_DF.append(F2_DF)
    return FINAL_DF

def gen_dict_by_key(DF):
    try:
        F_df = DF.drop(['Compound_Name','DiscoveryFinanceCode', 'ACTIVITY_TYPE','PROJECT_ONB','TASK_ONB'], axis=1)
        code_lst = list(F_df['Code'].unique())
        final=[]
        for code in code_lst:
            mil_df = F_df[F_df['Code']==code].sort_values(['date'], ascending=True)
            
            # renaming the milestone name with suffix
            s = mil_df.groupby(['Code', 'Milestone']).cumcount()
            mil_df['Milestone'] = (mil_df.Milestone + s[s>0].astype(str)).fillna(mil_df.Milestone)

            mil_lst = mil_df['Milestone'].tolist()
            for mil in mil_lst:
                final_dict={}
                df = mil_df[mil_df['Milestone']==mil].drop([ 'Milestone'], axis=1).sort_values(['date'], ascending=True)
                df2dict=df.set_index('Code').transpose().to_dict(orient='dict')
                dicttolist=[[k,{k1:v1 for k1,v1 in v.items() if v1 is not None and str(v1)!='nan'}] for k,v in df2dict.items()]
                df2=pd.DataFrame(dicttolist,columns=['Code', mil])
                final.append(df2)
                
        final_df = pd.concat(final)
        final_df = final_df.groupby('Code', as_index=False).first()
        return final_df
    except Exception as e:
        return 'Unable to find', '->', str(e)
    
def gen_list_milestones(DF):
    try:
        code_lst = list(DF['Code'].unique())
        final_milestones_lst=[]
        for code in code_lst:
            final_dict={}

            df = DF[DF['Code']==code].sort_values(['date'], ascending=False)

            # renaming the milestone name with suffix
            s = df.groupby(['Code', 'Milestone']).cumcount()
            df['Milestone'] = (df.Milestone + s[s>0].astype(str)).fillna(df.Milestone)
            df = df.loc[:, df.columns != 'Code'].sort_values(['date'])
    #         df = df.loc[:, df.columns != 'Candidate_Task_Core_Code_Short_Desc']

            # creating dict2
            df = df.reset_index()
            dict2 = df['Milestone'].to_list()

            final_dict['Code'] = code
            final_dict['Milestones'] = dict2

            final_milestones_lst.append(final_dict)
        milestone_df = pd.DataFrame(final_milestones_lst)
        return milestone_df
    except Exception as e:
            return 'Unable to find', '->', str(e)
