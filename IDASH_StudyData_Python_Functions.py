# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import re
from datetime import date
from datetime import datetime, timedelta
import functools
import json
from jsonfunctions import prep_for_JSON
from spfunctions import push_JSON_to_sharepoint










# Function to generate dict with Key column as separate column used in sections: MOPS Issues, Country Status
def gen_dict_for_each_study(df,dict_col,study_col):
    try:
        unique_list = list(df[study_col].unique())
        Final_lst=[]
        for each_study in unique_list:
            final_dict={}
            F_DF = df[df[study_col]==each_study].iloc[:,1:].copy()
            F_DICT = [{k:v for k,v in m.items() if pd.notnull(v)} for m in F_DF.to_dict(orient='records')]
            final_dict["Study_Number"]=each_study
            final_dict[dict_col]=F_DICT
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# Function to generate logic used for DVSO recruitment used in sections: DVSO Recruitment
def gen_dvso_rec_logic(DF,PCNT_COL,COMPLET_COL,VALUES):
    try:
        conditions=[(DF[COMPLET_COL]==1),(DF[PCNT_COL]>=100)&(DF[COMPLET_COL]!=1),(DF[PCNT_COL]<100)&(DF[COMPLET_COL]!=1)]
        values = VALUES
        DF[COMPLET_COL] = np.select(conditions, values)
        return DF[COMPLET_COL]
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

def Get_Study_Milestones_Dict(df,lookupdf):
    try:
        study_list = list(df['Study_Number'])
        MilestoneName_list = list(lookupdf['MilestoneName'])
        date_list = list(lookupdf['date'])
        pcnt_list = list(lookupdf['pcnt'])
        baseline_list = list(lookupdf['baseline'])
        df['Study_Pcnt_Comp_SAP_dummy'] = np.nan
        df['Study_SAP_Baseline_dummy'] = np.nan
        df['Study_Final_CSR_Baseline_dummy'] = np.nan
        df['Study_Pcnt_Study_Completion'] = np.nan
        Final_lst=[]
        for each_study in study_list:
            study_mile_DF = df[df['Study_Number']==each_study].copy()
            final_dict={}
            for MilestoneName,date,pcnt,baseline in zip(MilestoneName_list,date_list,pcnt_list,baseline_list):
                mile_df = study_mile_DF[[date,pcnt,baseline]].rename(columns={date:'date',pcnt:'pcnt',baseline:'baseline'}).copy()
                _dict = mile_df.to_dict(orient='records')
                mile_dict={k:v for k,v in _dict[0].items() if pd.notnull(v) and v!='' and str(v)!='nan'}
                if mile_dict !={}:
                    final_dict["Study_Number"]=each_study
                    final_dict[MilestoneName]=mile_dict
                else:
                    final_dict["Study_Number"]=each_study
                    final_dict[MilestoneName]=np.nan
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

# function to drop columns after generating study milestones dict
def drop_cols(df,lookupdf):
    try:
        MilestoneName_list = list(lookupdf['MilestoneName'])
        date_list = list(lookupdf['date'])
        pcnt_list = list(lookupdf['pcnt'])
        baseline_list = list(lookupdf['baseline'])
        df['Study_Pcnt_Comp_SAP_dummy'] = np.nan
        df['Study_SAP_Baseline_dummy'] = np.nan
        df['Study_Final_CSR_Baseline_dummy'] = np.nan
        df['Study_Pcnt_Study_Completion'] = np.nan
        lists_combined = MilestoneName_list+date_list+pcnt_list+baseline_list
        final_df=df.drop(lists_combined, axis = 1)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

def Get_Study_NextMilestone_Dict(df):
    try:
        study_list = list(df['STUDY_NUMBER'])
        Final_lst=[]
        for each_study in study_list:
            final_dict={}
            study_nxt_mile_DF = df[df['STUDY_NUMBER']==each_study].copy()
            NxtMile_df = study_nxt_mile_DF.rename(columns={'STUDYNEXTMILESTONE':'nextmilestone',
                                                        'STUDYNEXTMILESTONEDATE':'date'}).iloc[:,1:].copy()
            _dict = NxtMile_df.to_dict(orient='records')
            NxtMile_dict={k:v for k,v in _dict[0].items() if pd.notnull(v) and v!='' and str(v)!='nan'}
            if NxtMile_dict !={}:
                final_dict["Study_Number"]=each_study
                final_dict['DVSO_Study_NextMilestone']=NxtMile_dict
            else:
                final_dict["Study_Number"]=each_study
                final_dict['DVSO_Study_NextMilestone']=np.nan
            Final_lst.append(final_dict)
        final_df = pd.DataFrame(Final_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass

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

# function to convert date columns to required format
def StrDateToISOFormat(DF, list_date_cols):
    try:
        for col in list_date_cols:
            DF[col] = pd.to_datetime(DF[col], utc=True)#.dt.strftime('%m-%d-%Y')
        return DF
    except Exception as e:
        print('Unable to find', '->', e)
        pass
    
    
    
    
# Function to generate logic used for study list whether or not include based on logic. used for : Master List of Study Numbers
def gen_dist_list_study(DF,VALUES, _COL, _list):
    try:
        conditions=[(DF['study_status_plan']=='P-Proposed') &
                    (DF['study_priority'].str.contains('Priority')),
                    (DF['study_status_plan']=='P-Proposed') &
                    (DF['Study_Final_Approved_Protocol_Date']<= date_N_days),
                    (DF['study_status_plan'].isin(_list))]
        values = VALUES
        DF[_COL] = np.select(conditions, values)
        return DF[_COL]
    except Exception as e:
        print('Unable to find', '->', str(e))
        pass