# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu




def load_to_partition(df, partition_table,partitioned_on, writeWithSchema = False):
    #Used Main Pipeline run Scenario for writing Master views
    #TODO: get the partition_table  schema for pre-write checking https://doc.dataiku.com/dss/latest/python-api/datasets-reference.html?highlight=write_schema#dataiku.Dataset.read_schema
    try:    
        partition_table_dataset = dataiku.Dataset(partition_table)
        #Group dataframe for easy partitioning
        GROUPED_df = df.groupby([partitioned_on])
        for key, TMP_PORTFOLIO_df in GROUPED_df:
            #TODO: precheck the TMP_PORTFOLIO_df schema vs the partition_table schema - if incompatible, either fail this portfolio (and log) or abort whole scenario 
            #https://doc.dataiku.com/dss/latest/python-api/datasets-reference.html?highlight=write_schema#dataiku.Dataset.read_schema

            partition_table_dataset.set_write_partition(key)
            #logging.info('***********Writing to ' + partition_table +': ' +  str(int(key)) + ' Rows: ' + str(len(TMP_PORTFOLIO_df.index)) )
            if writeWithSchema:
                #variable datasets (e.g. project Metadata) should set write with schema to true
                partition_table_dataset.write_schema_from_dataframe(TMP_PORTFOLIO_df)  

            partition_table_dataset.write_from_dataframe(TMP_PORTFOLIO_df)

        return "Success"
    except Exception as e:
        return " Failure: " + str(e)