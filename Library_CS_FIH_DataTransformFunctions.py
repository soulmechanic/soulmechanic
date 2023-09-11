import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime
import re
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.listitems.listitem import ListItem


### Function to force in or force out the project based on the table


def force_proj_in_out(CS_FIH_DF, FORCE_DF, proj_status_conditions):
    try:
        ongoing_df = CS_FIH_DF[CS_FIH_DF['Project_Status'].isin(proj_status_conditions)]
        except_ongoing_df = CS_FIH_DF[~CS_FIH_DF['Project_Status'].isin(proj_status_conditions)]
        FORCE_PROJ_IN_LST = list(FORCE_DF['Pfizer_Code'][FORCE_DF['Force'] == 'in'])
        FORCE_PROJ_OUT_LST = list(FORCE_DF['Pfizer_Code'][FORCE_DF['Force'] == 'out'])
        forcein_df = except_ongoing_df[except_ongoing_df['Pfizer_Code'].isin(FORCE_PROJ_IN_LST)]
        forceout_df = ongoing_df[~ongoing_df['Pfizer_Code'].isin(FORCE_PROJ_OUT_LST)]
        FINAL_DF = pd.concat([forceout_df, forcein_df], ignore_index=True, axis=0)
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
    
### Function to overwrite empty and non empty input dataset with manually filled dataset
    
def overwrite_data_empty(overwite_df, key, col, currentValue):
    #finds first matching row from overwrites data using key
    #iterates through columns and overwrites any where overwrite row has a non null value
    #FIXME: inefficient code pattern - try replacing whole row at once
    try:
        overwritecell = overwite_df[overwite_df.Pfizer_Code == key].iloc[0][col]
        if pd.isnull(overwritecell):
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
    
    
def read_sharepoint_list(site_url, client_id, client_secret, list_name):
    """
    Reads a SharePoint list and converts it to a Pandas DataFrame.
    :param site_url: SharePoint site URL
    :param client_id: Client ID for authentication
    :param client_secret: Client secret for authentication
    :param list_name: Name of the SharePoint list to read
    :return: Pandas DataFrame containing the SharePoint list data
    """
    try:
        context_auth = AuthenticationContext(url=site_url)
        context_auth.acquire_token_for_app(client_id=client_id, client_secret=client_secret)
        ctx = ClientContext(site_url, context_auth)
        sp_list = ctx.web.lists.get_by_title(list_name)
        items = sp_list.get_items()
        ctx.load(items)
        ctx.execute_query()
        return pd.DataFrame([item.properties for item in items])
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# FIH_FIP_VARIANCE calculation functions
    
def snowflake_datetime_fix(df):
    # adding to 10 hours to the datetime columns as to address the timezone issue in Snowflake
    try:
        DateCols = df.select_dtypes(include=['datetime64[ns, UTC]']).columns
        for DCol in DateCols:
            df[DCol] = df[DCol].dt.normalize()
#             df[DCol] = df[DCol] + pd.Timedelta(hours=10)
#             df = df.sort_values(df.columns[0], ascending = True).reset_index(drop=True)
        return df
    except Exception as e:
        return 'Failed to add 10 hours to datetime column due to:', e
    
def date_variance(df,var_column, date1, date2, days=True):
    try:
        df[date1] = pd.to_datetime(df[date1])
        df[date2] = pd.to_datetime(df[date2])
        if days:
            df[var_column] = (df[date2] - df[date1]).dt.days
            #return df
        else:
            df[var_column] = ((df[date2] - df[date1])/np.timedelta64(1, 'M'))#.dt.days/30.42
            df[var_column] = df[var_column].round(3)
        return df#[var_column]
    except KeyError:
        raise KeyError("One or both of the specified date columns do not exist in the DataFrame.")
    except TypeError:
        raise TypeError("One or both of the specified date columns do not contain valid date values.")
        
def move_column(df, column_to_move, column_to_move_after):
    """
    This function takes in a DataFrame `df`, the name of the column you want to move `column_to_move`,
    and the name of the column you want to move it after `column_to_move_after`.
    It returns a new DataFrame with the specified column moved to the desired position.
    If one or both of the specified columns do not exist in the DataFrame,
    it will raise a KeyError and return the original DataFrame.
    """
    cols = df.columns.tolist()
    if column_to_move not in cols or column_to_move_after not in cols:
        raise KeyError("One or both of the specified columns do not exist in the DataFrame.")
    cols.insert(cols.index(column_to_move_after) + 1, cols.pop(cols.index(column_to_move)))
    df = df[cols]
    return df
