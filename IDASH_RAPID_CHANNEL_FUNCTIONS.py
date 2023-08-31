# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import json
from datetime import date, datetime
import xml.etree.ElementTree as ET
from collections import OrderedDict



# Function to construct XML file and all the columns are inside one sub element of rows


def gen_XML(dFrames, subelement):
    """Function to generate an XML string from a provided pandas DataFrame

    # Arguments
        dFrame (pandas.DataFrame): DataFrame to be converted to an XML string
        subelement (str): Subelement of root to be used...can be just about anything

    # Returns
        String in XML format based on rows from dFrame
    """
    run_date = datetime.now().strftime('%m-%d-%Y %H:%M:%S')

    root = ET.Element('PortfolioData')
    DC = ET.SubElement(root,'dateCreated')
    DC.text = run_date
    Rows = ET.SubElement(root, "Rows") # this will allow all the column values to get under one subelement rows

    for column in dFrames.columns:

        dFrame = pd.DataFrame(dFrames[column].dropna().copy())

        for i in list(dFrame.index):
            ET.SubElement(Rows, subelement, OrderedDict([(x, str(dFrame[x].loc[i])) for x in dFrame.columns]))
    return ET.tostring(root, encoding='us-ascii', method='xml').decode('utf-8')


        
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
        
        
        
        
        
# Function creates xml file and each column values added for every rows of subelement
# import xml.etree.ElementTree as ET

# from collections import OrderedDict

# def gen_XML(dFrames, subelement):
#     """Function to generate an XML string from a provided pandas DataFrame

#     # Arguments
#         dFrame (pandas.DataFrame): DataFrame to be converted to an XML string
#         subelement (str): Subelement of root to be used...can be just about anything

#     # Returns
#         String in XML format based on rows from dFrame
#     """
#     run_date = datetime.now().strftime('%m-%d-%Y %H:%M:%S')

#     root = ET.Element('PortfolioData')
#     DC = ET.SubElement(root,'dateCreated')
#     DC.text = run_date

#     for column in dFrames.columns:
#         Rows = ET.SubElement(root, "Rows")
#         dFrame = pd.DataFrame(dFrames[column].dropna().copy())

#         for i in list(dFrame.index):
#             ET.SubElement(Rows, subelement, OrderedDict([(x, str(dFrame[x].loc[i])) for x in dFrame.columns]))
#     return ET.tostring(root, encoding='us-ascii', method='xml').decode('utf-8')