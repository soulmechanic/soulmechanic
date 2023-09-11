import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime
import re

### Function to overwrite input dataset with manually filled dataset

def overwrite_data_p2p(overwite_df, key1, key2, col, currentValue):
    try:
        overwritecell = overwite_df[(overwite_df.PFIZER_CODE == key1) & (overwite_df.BASELINE_EVENT == key2)].iloc[0][col]
        if pd.notnull(overwritecell):
            print(key1,key2, col, currentValue, ' -> ', overwritecell)
            return overwritecell
        else:
            return currentValue
    except Exception as e:
        return currentValue


#####

def var_cal(DF,var_column, column_name):
    try:
        pd.set_option('mode.chained_assignment', None)
        # this will look for projects with baseline which has DP3 and calculate the variances
        codes = list(DF['PFIZER_CODE'].unique())
        DF_list = []
        for code in codes:
            df = DF[DF['PFIZER_CODE'] == code]
            if df['BASELINE_EVENT'].str.contains('DP3').any():
                df = df.set_index(df['BASELINE_EVENT'])
                display(df)
                dp3_value = df.loc['DP3', var_column]
#                 display(dp3_value)
#                 df['DP3_Col'] = dp3_value
                df[column_name]= (df['PSTART_TO_SUB_CT_MNTS']-dp3_value)/1
                DF_list.append(df[column_name])
            else:
                df[column_name]=''
                DF_list.append(df[column_name])
        final_col=pd.concat(DF_list, ignore_index=True)
        final_col=pd.to_numeric(final_col,errors='coerce')
        return final_col
    except Exception as e:
        print('Unable to find', '->', e)
        pass


### Function to force in or force out the project based on the table


def force_proj_in_out(DF, FORCE_DF):
    try:
        FORCE_PROJ_OUT_LST = list(FORCE_DF['Pfizer_Code'][FORCE_DF['Force'] == 'out'])
        FORCE_BEvent_OUT_LST = list(FORCE_DF['Baseline_Event'][FORCE_DF['Force'] == 'out'])
        FINAL_DF = DF.drop(DF[(DF['PFIZER_CODE'].isin(FORCE_PROJ_OUT_LST)) & (DF['BASELINE_EVENT'].isin(FORCE_BEvent_OUT_LST))].index)
        return FINAL_DF
    except Exception as e:
        print('Unable to find', '->', e)
        pass
    


### Function to Change Date format to custom format ###

def ChangeDateFormat(df,column_names,date_format = '%d-%m-%Y'):
    for column_name in column_names:
        #df[column_name] = df[column_name].fillna('')
        if(date_format == '%Y-%m-%d'):
            df[column_name] = pd.to_datetime(df[column_name], utc=True).dt.strftime('%m-%d-%Y')
        else:
            try:
                df[column_name] = pd.to_datetime(df[column_name], utc=True).dt.strftime(date_format)
            except:
                print('Please provide correct format of date column' , ' -> ',column_name)
    return(df)




### Function to add BM 2.3 Target values based on the given condition from base table BM_Target ###

def Add_BM_Target(BM_TARGETS_DF, modality, FIP_FIH):
    #adds the target based on modality and Category
    #print(modality, category)
    try:
        #find rows matching modality
        matchingModalityRows = BM_TARGETS_DF[BM_TARGETS_DF.Modality == modality]
        matchingFIH_FIPRows = matchingModalityRows[matchingModalityRows.FIP_FIH == FIP_FIH]

        if matchingFIH_FIPRows.empty:
            #use the default row if no match for a category
            defaultRow  = matchingModalityRows[matchingModalityRows.FIP_FIH == 'FIP']
            return defaultRow.iloc[0]['Target']
        else:
            return matchingFIH_FIPRows.iloc[0]['Target']
    except Exception as e:
        #not finding a target for this modality
        #TODO: log to error
        print('Unable to find a target for this modality', '->', e)
        pass
    
    
### Function to overwrite input dataset with manually filled dataset
    
def overwrite_data(overwite_df, key, col, currentValue):
    #finds first matching row from overwrites data using key
    #iterates through columns and overwrites any where overwrite row has a non null value
    #FIXME: inefficient code pattern - try replacing whole row at once
    try:
        overwritecell = overwite_df[overwite_df.Pfizer_Code == key].iloc[0][col]
        if pd.notnull(overwritecell):
            print(key, col, currentValue, ' -> ', overwritecell)
            return overwritecell
        else:
            return currentValue

    except Exception as e:
        #expect most rows to have no overwrites
        #pass
        #print('Look at this error:', e)
        return currentValue
    

### Function to find difference either when case is between dates or two integers 

def GetDiffBwColumns(column1, column2, CaseWhen):
    try:
        if column1 != '' and column2 != '':
            if CaseWhen == 'date_diff':
                diff_col = (column1 - column2).days/30.42
                return diff_col
            elif CaseWhen == 'int_diff':
                diff_col = (column1 - column2)
                return diff_col
        else:
            print("")
    except Exception as e:
        print('Look at this error:', e)
        pass

    
### Function to create a new column FIH_FIP based on condtions given
    
def Add_FIH_FIP(column1,column2, condition1, condition2, condition3):
    try:
        if column1 == condition1 or column1 == condition2 or column2==condition3:
            df_col = "FIP"
            return df_col
        else:
            df_col = "FIH"
            return df_col
    except Exception as e:
        pass
    
    
    
#### Function to create Last and Next Milestones columns

def last_next_milestone(df, columns_list, col):
    try:
        curr_dt = pd.to_datetime("now", utc=True)
        milestones_df = df[columns_list]
        #milestones_df = milestones_df.set_index('Pfizer_Code')
        milestones_df = milestones_df.sub(curr_dt)#.div(30.42)#.fillna(value=9999)
        milestones_df = (milestones_df / np.timedelta64(1, 'D'))#.astype(int)
        milestones_df = milestones_df.fillna(value=9999).astype(int)

        list_index = list(milestones_df.index)
        miles_list = []
        pcode_list = []
        if col == 'Last_Milestone':
            milestones_df[milestones_df > 0] = -9999
    #         display(milestones_df)

            for i in list_index:
                #print(i)
                a = milestones_df.loc[i].idxmax(skipna = True)
                a = re.sub('[^A-Za-z0-9]+',' ', a)
                a = re.sub(r'[0-9]+', '', a)
                miles_list.append(a)
                pcode_list.append(i)

    #             data = pd.DataFrame({'Pfizer_code': pcode_list, 'Previous_Milestone':miles_list})
                data = pd.DataFrame({'Last_Milestone':miles_list})
            return data
        elif col == 'Next_Milestone':
            milestones_df[milestones_df < 0] = 9999
    #         display(milestones_df)

            for i in list_index:
                #print(i)
                a = milestones_df.loc[i].idxmin(skipna = True)
                a = re.sub('[^A-Za-z0-9]+',' ', a)
                a = re.sub(r'[0-9]+', '', a)
                if a != ' CS':
                    miles_list.append(a)
                    pcode_list.append(i)
                else:
                    a = ''
                    miles_list.append(a)
                    pcode_list.append(i)

    #             data = pd.DataFrame({'Pfizer_code': pcode_list, 'Future_Milestone':miles_list})
                data = pd.DataFrame({'Next_Milestone':miles_list})
            return data
    except Exception as e:
        print('Unable to find', '->', e)
        pass
        
