# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import json
from jsonfunctions import prep_for_JSON

# Read recipe inputs
kpi_GOALS_MAIN = dataiku.Dataset("KPI_GOALS_MAIN")
kpi_GOALS_MAIN_DF = kpi_GOALS_MAIN.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
pd.show_versions(as_json=False)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
kpi_GOALS_MAIN_DF.columns# = kpi_GOALS_MAIN_DF[kpi_GOALS_MAIN_DF['Code']=='A022c']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
KPI_GOALS_json = json.dumps([row.dropna().to_dict() for index,row in kpi_GOALS_MAIN_DF.iterrows()],indent=1,ensure_ascii=False,default=str)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
kpi_GOALS_MAIN_DF = kpi_GOALS_MAIN_DF.applymap(prep_for_JSON)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
kpi_GOALS_MAIN_DF.head()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
grouped_df = kpi_GOALS_MAIN_DF.groupby(['Status', 'Target', 'LE', 'Actual', 'GoalDescription', 'GoalSort'], as_index=False)['KPINumber', 'KPIDescription', 'KPIComments', 'KPISort', 'KPIMitigation', 'KPIStudy', 'KPIGroupLine', 'KPIGroupOwner']

for key, item in grouped_df:
    gf_df = grouped_df.get_group(key)
    display(grouped_df.get_group(key), "\n\n")
    a = gf_df.to_dict('r')
    display(a)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def add_KPIGoals(df,col_name):
    try:
        final_team_goals_lst=[]
        df = df.sort_values(by=["Code"])
        uniq_proj_code = list(df["Code"].unique())
        kip_goals_df = df[['Code', 'Status', 'Target', 'LE', 'Actual', 'GoalDescription', 'GoalSort']].drop_duplicates(subset=['GoalSort'])
        kip_df = df[['Code','KPINumber', 'KPIDescription', 'KPIComments', 'KPISort', 'KPIMitigation', 'KPIStudy', 'KPIGroupLine', 'KPIGroupOwner']]
#         display(kip_df)
        for each_code in uniq_proj_code:
            final_dict={}
            kpi_goals_f_df = kip_goals_df[kip_goals_df["Code"]==each_code].iloc[:,1:].copy()
            kpi_f_df = kip_df[kip_df["Code"]==each_code].iloc[:,1:].copy()
#             display(kpi_f_df)
            kpi_goals_list = [{k:v for k,v in m.items() if pd.notnull(v)} for m in kpi_goals_f_df.to_dict(orient='records')]
#             kpi_goals_list = kpi_goals_f_df.to_dict(orient='list')
            kpi_list = [{k:v for k,v in m.items() if pd.notnull(v)} for m in kpi_f_df.to_dict(orient='records')]
            print(kpi_goals_list)
            final_dict['Code'] = each_code
#             a = kpi_goals_list.extend(kpi_list)
#             print(a)
            final_dict[col_name] = kpi_goals_list#.extend(kpi_list)
#             final_dict['KPI'] = {kpi_list}
            final_team_goals_lst.append(final_dict)
        final_df = pd.DataFrame(final_team_goals_lst)
        return final_df
    except Exception as e:
        print('Unable to find', '->', e)
        pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
f_df = add_KPIGoals(KPI_GOALS_DF,'KPI_GOALS')
f_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
f_df.iloc[0]['KPI_GOALS']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def df_to_json(df, candidate):
    _json = {'Features':{}}

    for _, row in df.iterrows():
        feature = {'Candidate':[]}
        for prop in candidate:
            print(prop)
            feature['Candidate'][prop] = row[prop]
        _json['Features'].append(feature)
    return _json

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
kpi_df = kpi_GOALS_MAIN_DF[['Status', 'Target', 'LE', 'Actual', 'GoalDescription']]
kpi_df_NEW = kpi_GOALS_MAIN_DF[['Code','KPINumber', 'KPIDescription', 'KPIComments', 'KPISort', 'KPIMitigation', 'KPIStudy', 'KPIGroupLine', 'KPIGroupOwner']]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
cols = kpi_df.columns
kpi_json = df_to_json(kpi_df, cols)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
Final_DF = KPI_GOALS_DF[KPI_GOALS_DF['Code']=='A548k']
Final_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
list_codes = list(kpi_GOALS_MAIN_DF["Code"].unique())
final_kpi_goals_lst = []
for code in list_codes:
#     print(code)
    final_dict = {}
    DF = kpi_GOALS_MAIN_DF[kpi_GOALS_MAIN_DF['Code']==code].fillna('')
#     display(DF)
    j = (DF.groupby(by=[ 'Status', 'Target', 'LE', 'Actual', 'GoalDescription', 'GoalSort'], as_index=True)
             .apply(lambda x: x[['KPINumber', 'KPIDescription', 'KPIComments', 'KPISort', 'KPIMitigation', 'KPIStudy', 'KPIGroupLine', 'KPIGroupOwner']].to_dict('r'))
             .reset_index()
             .rename(columns={0:'KPI'})
             .to_dict('r'))
#              .to_json(orient='records'))
    new_dict={k:v for k,v in j[0].items() if v!='nan' and str(v)!=''}
    display(new_dict)

    final_dict['Code'] = code
    final_dict['KPIGoals'] = j
    final_kpi_goals_lst.append(final_dict)
final_df = pd.DataFrame(final_kpi_goals_lst)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
final_df
# final_dict_lst = final_df.to_dict(orient='records')
# final_dict_lst
# for each_dict in final_dict_lst:
#     new_dict={k:v for k,v in each_dict.items() if v!='' and str(v)!=''}
#     display(new_dict)

#     d = j[0]
# #     display(d)
#     for k,v in d.items():
#         if v == 'PlaceHolder':
#             del d[k]

# #     temp_dict={del j[0][k] for k,v in j[0].items() if str(v) == 'PlaceHolder'}
#     display(d)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
j = (Final_DF.groupby([ 'Code', 'Status', 'Target', 'LE', 'Actual', 'GoalDescription', 'GoalSort'], as_index=True)
#              .apply(lambda x: x[['Status', 'Target', 'LE', 'Actual', 'GoalDescription', 'GoalSort']].drop_duplicates(subset=['GoalSort']).to_dict('r'))
             .apply(lambda x: x[['KPINumber', 'KPIDescription', 'KPIComments', 'KPISort', 'KPIMitigation', 'KPIStudy', 'KPIGroupLine', 'KPIGroupOwner']].to_dict('r'))
             .reset_index()
             .rename(columns={0:'KPI'})
#              .rename(columns={0:'KPI_GOALS'})
             .to_json(orient='records'))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# print(json.dumps(json.loads(j), indent=2, sort_keys=True))
Final_DF['KPIGoals'] = j
a = final_df.iloc[1]['KPIGoals']
del j['Code']
kpi_final_df = Final_DF[['Code','KPIGoals']]
kpi_final_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#j
KPI_GOALS_json = json.dumps([row.dropna().to_dict() for index,row in final_df.iterrows()],indent=1,ensure_ascii=False,default=str)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# with open('KPI_GOALS_json', 'r') as data_file:
data = json.load(KPI_GOALS_json)

for element in data:
    element.pop('Status', None)

# with open('data.json', 'w') as data_file:
data = json.dump(data, data_file)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# a = final_df.iloc[0]['KPIGoals']
# a
print(json.dumps(json.loads(KPI_GOALS_json), indent=2, sort_keys=True))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
pd.__version__
pip list

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import functools
new_df = functools.reduce(lambda x,y: pd.merge(x,y, on='Code', how='left'), [j, kpi_df_NEW])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
new_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
b = (new_df.groupby([ 'Code' ,'Status', 'Target', 'LE', 'Actual', 'GoalDescription', 'GoalSort'], as_index=True)
#              .apply(lambda x: x[['Status', 'Target', 'LE', 'Actual', 'GoalDescription', 'GoalSort']].drop_duplicates(subset=['GoalSort']).to_dict('r'))
             .apply(lambda x: x[['KPI', 'KPINumber', 'KPIDescription', 'KPIComments', 'KPISort', 'KPIMitigation', 'KPIStudy', 'KPIGroupLine', 'KPIGroupOwner']].to_dict('r'))
             .reset_index()
#              .rename(columns={0:'KPI'}))
             .rename(columns={0:'KPI_GOALS'})
             .to_json(orient='records'))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
b

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
print(json.dumps(json.loads(j), indent=2, sort_keys=True))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
KPI_Goals_DF = add_KPIGoals(kpi_GOALS_MAIN_DF, 'KPI')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
KPI_Goals_DF

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a Pandas dataframe
# NB: DSS also supports other kinds of APIs for reading and writing data. Please see doc.

kpi_GOALS_COMPUTED_df = KPI_Goals_DF # For this sample code, simply copy input to output


# Write recipe outputs
kpi_GOALS_COMPUTED = dataiku.Dataset("KPI_GOALS_COMPUTED")
kpi_GOALS_COMPUTED.write_with_schema(kpi_GOALS_COMPUTED_df)