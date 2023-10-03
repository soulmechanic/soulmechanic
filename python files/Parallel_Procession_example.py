# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#4 THREAD PARALLEL
import timeit
start = timeit.default_timer()
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import requests
#from requests_ntlm import HttpNtlmAuth
from dataiku.scenario import Scenario
from dataiku.core.sql import SQLExecutor2
from datetime import datetime
from io import BytesIO
import boto3
import os
from multiprocessing import Pool

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
sqlconn=SQLExecutor2(connection="LDW_ONESOURCE_PROD_RW")
snp_date_sql='Select distinct SNP_DATE from ONESRC_STG_OWNER.RAPID_DEMAND_SNP_ARC'
SNP_DATES_SET=sqlconn.query_to_df(snp_date_sql)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
SNP_DATES_SET['SNP_DATE']=SNP_DATES_SET['SNP_DATE'].astype('datetime64[ns]')
AWS_SERVER_KEY_ID = 'AKIA6NIP3JYIOHD7I2FZ'
AWS_SERVER_ACCESS_KEY = 'uZV883T+2903eYtML/vVgRt76fq2BGX1BJRXFUaf'
Bucketname = 'pfe-baiaes-eu-w1'
region='eu-west-1'#'us-east-1'
session = boto3.Session(aws_access_key_id=AWS_SERVER_KEY_ID,aws_secret_access_key=AWS_SERVER_ACCESS_KEY,region_name=region)
s3_client=session.client('s3')

def parallel_method(i):
    print(i)
    out_buffer = BytesIO()
    SNP_DATES_SET['SNP_DATE'][i] =SNP_DATES_SET['SNP_DATE'][i].strftime('%m/%d/%Y')
    data_sql="Select * from ONESRC_STG_OWNER.RAPID_DEMAND_SNP_ARC where SNP_DATE = " + "to_date('"+SNP_DATES_SET['SNP_DATE'][i]+"','mm/dd/yyyy')"
    print('before')
    RAPID_DEMAND_SNP_ARC=sqlconn.query_to_df(data_sql)
    print('after')
    print(RAPID_DEMAND_SNP_ARC)
    RAPID_DEMAND_SNP_ARC = RAPID_DEMAND_SNP_ARC.astype(str)
    print('break')
    SNP_DATES_SET['SNP_DATE'][i]=SNP_DATES_SET['SNP_DATE'][i].replace('/','_')
    filename='OneSource/RAPID_DEMAND_SNP_ARC_PROD/RAPID_DEMAND_SNP_ARC_'+SNP_DATES_SET['SNP_DATE'][i]+'.parquet'
    print(filename)
    RAPID_DEMAND_SNP_ARC.to_parquet(out_buffer)
    s3_client.put_object(Bucket=Bucketname,Key=filename,Body=out_buffer.getvalue())
    out_buffer.close()

pool = Pool(processes=3)
print(len(SNP_DATES_SET))
dates = range(len(SNP_DATES_SET))
pool.map(parallel_method, dates)

stop = timeit.default_timer()

print('Time: ', stop - start)
print("complete")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
print("boom")




def load_to_partition(df,partition_table):
    #Group dataframe for easy partitioning
    GROUPED_df = df.groupby(['PORTFOLIO_ID'])
    try:
        if not df.empty:
            for key, TMP_PORTFOLIO_df in GROUPED_df:
                partition_table.set_write_partition(key)
                #print('Writing Portfolio: ', str(int(key)), ' Rows: ', str(len(TMP_PORTFOLIO_df.index)))
                logging.info('***********Writing to ' + partition_table +': ' +  str(int(key)) + ' Rows: ' + str(len(TMP_PORTFOLIO_df.index)) )
                partition_table.write_from_dataframe(TMP_PORTFOLIO_df)
    except Exception as e:
            logging.info('********Writing to ' + partition_table +': '+  str(int(key)) + ' Rows: ' + str(len(TMP_PORTFOLIO_df.index)) + 'failed, error:' + str(e))
