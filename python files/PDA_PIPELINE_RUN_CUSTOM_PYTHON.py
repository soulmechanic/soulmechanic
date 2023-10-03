###############################################################################Portfolio Metadata Partitioned########################################################################################


# -------------------------------------------------------------------------------- 
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------- 

import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import logging
from pda_common_code import load_to_partition, load_to_partition_parallel

logging.basicConfig(level=logging.DEBUG)

# -------------------------------------------------------------------------------- 
# ### READING SLIPSTREAM AND ONESOURCE ENRICH SNOWFLAKE TABLES ###
# --------------------------------------------------------------------------------

PDA_STG_ENRICH_PORTFOLIO_METADATA = dataiku.Dataset("PDA_STG_ENRICH_PORTFOLIO_METADATA")
PDA_STG_ENRICH_PORTFOLIO_METADATA_df = PDA_STG_ENRICH_PORTFOLIO_METADATA.get_dataframe()

PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES = dataiku.Dataset("PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES")
PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES_df = PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES.get_dataframe()

# -------------------------------------------------------------------------------- 
# ### MERGING SLIPSTREAM AND ONESOURCE ENRICH SNOWFLAKE TABLES ###
# --------------------------------------------------------------------------------

PDA_STG_ENRICH_PORTFOLIO_METADATA_MERGED_df = PDA_STG_ENRICH_PORTFOLIO_METADATA_df.merge(PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES_df,
             how='left', on=["PORTFOLIO_ID"] ,suffixes=('_DROPME', '')).filter(regex='^(?!.*_DROPME)')

# -------------------------------------------------------------------------------- 
# ### WRITING TO OUTPUT ###
# -------------------------------------------------------------------------------- 

PDA_MV_PORTFOLIO_METADATA = dataiku.Dataset("PDA_MV_PORTFOLIO_METADATA")

# Loading data by partition using functions stored in project code library
load_to_partition_parallel(PDA_STG_ENRICH_PORTFOLIO_METADATA_MERGED_df,'PDA_MV_PORTFOLIO_METADATA', 'PORTFOLIO_ID',3)     



##############################################################Project Metadata Partitioned###############################################################################


# -------------------------------------------------------------------------------- 
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------- 

import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import functools
import json
import logging
from pda_common_code import load_to_partition, MERGE_OS_OVERRIDEN_AND_OS_ENRICH_DATA, load_to_partition_parallel

logging.basicConfig(level=logging.DEBUG)

# importing functions from project libraries
from SharePointFunctions import read_SPList
from OVERRIDE_FUNCTIONS import override_dataframe

client = dataiku.api_client()
DSSProject = client.get_project("PDANEXUSPIPELINE")

# -------------------------------------------------------------------------------- 
# ### READING ONESOURCE AND ONESOURCE ENRICH SNOWFLAKE TABLES ###
# --------------------------------------------------------------------------------

# Read recipe inputs
PDA_MV_PROJECT_MASTER = dataiku.Dataset("PDA_MV_PROJECT_MASTER")
PDA_MV_PROJECT_MASTER_df = PDA_MV_PROJECT_MASTER.get_dataframe()

PDA_STG_PROJECT_REFERENCE_ENRICH = dataiku.Dataset("PDA_STG_PROJECT_REFERENCE_ENRICH")
PDA_STG_PROJECT_REFERENCE_ENRICH_df = PDA_STG_PROJECT_REFERENCE_ENRICH.get_dataframe()

# -------------------------------------------------------------------------------- 
# ### SETTING UP VARIABLES AT PROJECT LEVEL ###
# --------------------------------------------------------------------------------

#set variables to get Project id's
Project_id=PDA_STG_PROJECT_REFERENCE_ENRICH_df.PROJECT_ID.unique()
Project_cnt = len(PDA_STG_PROJECT_REFERENCE_ENRICH_df.PROJECT_ID.unique())
variables = DSSProject.get_variables()
variables["standard"]["ENRICH_PROJECT_IDS_TO_INCLUDE"] = Project_id.tolist()

#set variables to get count of Project id's
variables["standard"]["ENRICH_PROJECT_IDS_COUNT"] = Project_cnt
DSSProject.set_variables(variables)

# -------------------------------------------------------------------------------- 
# ### CALLING THE FUNCTION TO MERGE DATA BASED ON CONDITION ###
# -------------------------------------------------------------------------------- 

LEFT_DF = PDA_STG_PROJECT_REFERENCE_ENRICH_df.copy()
RIGHT_DF = PDA_MV_PROJECT_MASTER_df.copy()

# functions stored in project code library
PDA_MV_PROJECT_METADATA_df = MERGE_OS_OVERRIDEN_AND_OS_ENRICH_DATA(LEFT_DF, RIGHT_DF)

# -------------------------------------------------------------------------------- 
# ### WRITING TO OUTPUT ###
# -------------------------------------------------------------------------------- 

PDA_MV_PROJECT_METADATA = dataiku.Dataset("PDA_MV_PROJECT_METADATA")
PDA_MV_PROJECT_METADATA.write_schema_from_dataframe(PDA_MV_PROJECT_METADATA_df)

# Loading data by partition using functions stored in project code library
load_to_partition_parallel(PDA_MV_PROJECT_METADATA_df,'PDA_MV_PROJECT_METADATA', 'PORTFOLIO_ID',3)            


#PDA_MV_PROJECT_OVERRIDE_REFERENCE = dataiku.Dataset("PDA_MV_PROJECT_MASTER")
#PDA_MV_PROJECT_OVERRIDE_REFERENCE.write_with_schema(PDA_MV_PROJECT_MASTER_df)



##############################################################Output Values Partitioned###############################################################################



# -------------------------------------------------------------------------------- 
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------- 

import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import json
from datetime import date, datetime
import logging
from pda_common_code import load_to_partition, load_to_partition_parallel


logging.basicConfig(level=logging.DEBUG)

# -------------------------------------------------------------------------------- 
# ### READING SLIPSTREAM AND ONESOURCE ENRICH SNOWFLAKE TABLES ###
# --------------------------------------------------------------------------------

PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES = dataiku.Dataset("PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES")
PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES_df = PDA_SLIPSTREAM_PORTFOLIO_ATTRIBUTES.get_dataframe()

PDA_SLIPSTREAM_VALUE_TYPES = dataiku.Dataset("PDA_SLIPSTREAM_VALUE_TYPES")
PDA_SLIPSTREAM_VALUE_TYPES_df = PDA_SLIPSTREAM_VALUE_TYPES.get_dataframe()

PDA_STG_ENRICH_FLATFILE = dataiku.Dataset("PDA_STG_ENRICH_FLATFILE")
PDA_STG_ENRICH_FLATFILE_df = PDA_STG_ENRICH_FLATFILE.get_dataframe()

PDA_STG_ENRICH_DC = dataiku.Dataset("PDA_STG_ENRICH_DC")
PDA_STG_ENRICH_DC_df = PDA_STG_ENRICH_DC.get_dataframe()

# -------------------------------------------------------------------------------- 
# ### MERGING SLIPSTREAM AND ONESOURCE ENRICH SNOWFLAKE TABLES ###
# --------------------------------------------------------------------------------

FF_ECONOMICS_df =  (pd.merge( PDA_SLIPSTREAM_VALUE_TYPES_df, PDA_STG_ENRICH_FLATFILE_df, 
                             how='inner', left_on='SYSTEM_VALUES', right_on='COLUMN_NAME')
                    [['PORTFOLIO_ID','PROJECT_ID','SNAPSHOT_ID','CANDIDATE_CODE','COLUMN_NAME', 'COLUMN_DATA']])

# Renaming column names
FF_ECONOMICS_df.rename(columns={'COLUMN_NAME': 'TYPE', 'COLUMN_DATA': 'VALUE'}, inplace=True)


# -------------------------------------------------------------------------------- 
# ### WRITING TO OUTPUT ###
# --------------------------------------------------------------------------------

PDA_TMP_MV_OUTPUT_VALUES = dataiku.Dataset("PDA_MV_OUTPUT_VALUES")
PDA_TMP_MV_OUTPUT_VALUES.write_schema_from_dataframe(FF_ECONOMICS_df)

# Loading data by partition using functions stored in project code library
load_to_partition_parallel(FF_ECONOMICS_df,'PDA_MV_OUTPUT_VALUES', 'PORTFOLIO_ID',3)            
    

##############################################################Output Timeseries Partitioned###############################################################################


# -------------------------------------------------------------------------------- 
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------- 

import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import time
import logging
from pda_common_code import load_to_partition, load_to_partition_parallel

logging.basicConfig(level=logging.DEBUG)

# -------------------------------------------------------------------------------- 
# ### READING SLIPSTREAM AND ONESOURCE ENRICH SNOWFLAKE TABLES ###
# --------------------------------------------------------------------------------

PDA_OUTPUT_TIMESERIES_TYPES_df = dataiku.Dataset("PDA_SLIPSTREAM_TIMESERIES_TYPES")
PDA_OUTPUT_TIMESERIES_TYPES_df = PDA_OUTPUT_TIMESERIES_TYPES_df.get_dataframe()

#Replacing OS Global and Regional PNLs with Unioned snowflake PNL (pre-filtered by Portfolio ID)
PDA_STG_ENRICH_PNL = dataiku.Dataset("PDA_STG_ENRICH_PNL")
PDA_STG_ENRICH_PNL_df = PDA_STG_ENRICH_PNL.get_dataframe()

#Adding Dev Costs - this is a bit of a force fit may need to run as a separate table, depending on how captario structures
PDA_STG_ENRICH_DC = dataiku.Dataset("PDA_STG_ENRICH_DC")
PDA_STG_ENRICH_DC_df = PDA_STG_ENRICH_DC.get_dataframe()

# -------------------------------------------------------------------------------- 
# ### DATA PROCESSING ###
# --------------------------------------------------------------------------------

#Sharepoint Derived listing to edit see:
#list includes both PNL and Dev Cost elements, Implicit assumption there is no naming overlap between these entities
SYSTEM_TIMESERIES_list = PDA_OUTPUT_TIMESERIES_TYPES_df['SYSTEM_TIMESERIES'].tolist()
SYSTEM_TIMESERIES_list = list(set(SYSTEM_TIMESERIES_list))

#creating specific list relevant to PNL, unpivot operation will fail if given invalid column references
SYSTEM_TIMESERIES_PNL_list =  list(set(SYSTEM_TIMESERIES_list) & set(PDA_STG_ENRICH_PNL_df.columns.values.tolist()))

# Unpivot PNL Data
PDA_STG_ENRICH_PNL_FILTERED_NORMALIZED_df = pd.melt(PDA_STG_ENRICH_PNL_df,
                                             id_vars=['PORTFOLIO_ID', 'PROJECT_ID', 'SNAPSHOT_ID','CANDIDATE_CODE',
                                                      'SOURCE','COMPONENT', 'REGION','YEAR','SCENARIO','PHASE'],
                                             value_vars= SYSTEM_TIMESERIES_PNL_list,
                                              var_name='TYPE', value_name='VALUE')

#Union PNL And Dev costs
PDA_STG_CONCAT_PNL_DC_df = pd.concat([PDA_STG_ENRICH_PNL_FILTERED_NORMALIZED_df,PDA_STG_ENRICH_DC_df])

# -------------------------------------------------------------------------------- 
# ### WRITING TO OUTPUT ###
# --------------------------------------------------------------------------------

PDA_MV_OUTPUT_TIMESERIES = dataiku.Dataset("PDA_MV_OUTPUT_TIMESERIES")
PDA_MV_OUTPUT_TIMESERIES.write_schema_from_dataframe(PDA_STG_CONCAT_PNL_DC_df)

# Loading data by partition using functions stored in project code library
load_to_partition_parallel(PDA_STG_CONCAT_PNL_DC_df,'PDA_MV_OUTPUT_TIMESERIES', 'PORTFOLIO_ID',3)            
