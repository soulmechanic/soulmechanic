def load_to_partition(df, partition_table,partitioned_on, writeWithSchema = False):
    #Group dataframe for easy partitioning
    #df, partition_table,partitioned_on = args[0] , args[1], args[2]
    partition_table_dataset = dataiku.Dataset(partition_table)
    GROUPED_df = df.groupby([partitioned_on])
    
    #Swallows errors - not passed back to caller, 
    #FIXME: add compulsory success/fail return value to allow caller to handle gracefully
    #try:
        #if not df.empty:
            #for key, TMP_PORTFOLIO_df in GROUPED_df:
                #partition_table_dataset.set_write_partition(key)
                #print('Writing Portfolio: ', str(int(key)), ' Rows: ', str(len(TMP_PORTFOLIO_df.index)))
                #logging.info('***********Writing to ' + partition_table +': ' +  str(int(key)) + ' Rows: ' + str(len(TMP_PORTFOLIO_df.index)) )
                #partition_table_dataset.write_from_dataframe(TMP_PORTFOLIO_df)
    #except Exception as e:
            #logging.info('********Writing to ' + partition_table +': '+  str(int(key)) + ' Rows: ' + str(len(TMP_PORTFOLIO_df.index)) + 'failed, error:' + str(e))
    
    #DELETEME: for now recoding without try except to force scenario failure on any errors     
    for key, TMP_PORTFOLIO_df in GROUPED_df:
        partition_table_dataset.set_write_partition(key)
        #print('Writing Portfolio: ', str(int(key)), ' Rows: ', str(len(TMP_PORTFOLIO_df.index)))
        logging.info('***********Writing to ' + partition_table +': ' +  str(int(key)) + ' Rows: ' + str(len(TMP_PORTFOLIO_df.index)) )
        
        #FIXME: We have a problem with updating schema when slipstream columns change
        #This optional write with scehma option does not appear to work
        #also need to understand which dataframe to use
        if writeWithSchema:
            partition_table_dataset.write_schema_from_dataframe(df) 

        partition_table_dataset.write_from_dataframe(TMP_PORTFOLIO_df)  


'''
# load data into partitioned tables
def load_to_partition(df, partition_table,partitioned_on):
    #Group dataframe for easy partitioning
    #df, partition_table,partitioned_on = args[0] , args[1], args[2]
    partition_table_dataset = dataiku.Dataset(partition_table)
    GROUPED_df = df.groupby([partitioned_on])
    try:
        if not df.empty:
            for key, TMP_PORTFOLIO_df in GROUPED_df:
                partition_table_dataset.set_write_partition(key)
                #print('Writing Portfolio: ', str(int(key)), ' Rows: ', str(len(TMP_PORTFOLIO_df.index)))
                logging.info('***********Writing to ' + partition_table +': ' +  str(int(key)) + ' Rows: ' + str(len(TMP_PORTFOLIO_df.index)) )
                partition_table_dataset.write_from_dataframe(TMP_PORTFOLIO_df)
    except Exception as e:
            logging.info('********Writing to ' + partition_table +': '+  str(int(key)) + ' Rows: ' + str(len(TMP_PORTFOLIO_df.index)) + 'failed, error:' + str(e))

#Parallely loading data to Snowflake
def load_to_partition_parallel(df,partition_table,partitioned_on):
    p = multiprocessing.Process(target=load_to_partition, args=(df,partition_table,partitioned_on)) 
    p.start()
    p.join()
    #pool = multiprocessing.Pool(no_of_times)
    #result1 = 
    #pool.map(load_to_partition, [[df,partition_table,partitioned_on]])
    #print(result1)
'''   
#Write dataframe to Snowflake table
def write_snowflake(dataset,df):
    snowflake_destination = dataiku.Dataset(dataset)
    snowflake_destination.spec_item["appendMode"] = True
    with snowflake_destination.get_writer() as writer:
        writer.write_dataframe(df)
        
        
############DEPRECATED - MOVED CODE  BACK TO SCENARIO STEP #################        
# -------------------------------------------------------------------------------- 
# ### MERGE ONESOURCE OVERRIDEN DATA WITH ONESOURCE ENRICH DATA ###
# -------------------------------------------------------------------------------- 

# function to merge OneSource overriden data with OneSource Enrich Data
# 
def DLELETEME_MERGE_OS_OVERRIDEN_AND_OS_ENRICH_DATA(LEFT_DF, RIGHT_DF, key='CANDIDATE_CODE'):
    try:
        # Finding Common columns
        common_cols = list(np.intersect1d(LEFT_DF.columns, RIGHT_DF.columns))
        common_cols.remove(key)

        # for each common column in left df it fills na values from right df  and finally merges two dfs(needs to confirm)
        for col in common_cols:
            LEFT_DF[col].fillna(RIGHT_DF[col])
            RIGHT_DF = RIGHT_DF.drop(col,axis=1)
        DF = pd.merge(LEFT_DF,RIGHT_DF,on=key,  how='left')

        # sorts the columns alphabetically
        DF = DF.reindex(sorted(DF.columns), axis=1)

        #move the PROJECT_ID, PORTFOLIO_ID, key columns to starting of the dataframe
        starting_cols = ['PROJECT_ID','PORTFOLIO_ID',key]
        DF = DF[ starting_cols + [ col for col in DF.columns if col not in starting_cols] ]
        DF[['PROJECT_ID','PORTFOLIO_ID']] = DF[['PROJECT_ID','PORTFOLIO_ID']].astype(int)

        return DF
    except Exception as e:
        print('merge_os_df failed, error:', str(e))
        pass