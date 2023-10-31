from openpyxl import load_workbook
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Protection , Font, PatternFill
from win32com.client.gencache import EnsureDispatch
from openpyxl.worksheet.table import Table
from openpyxl.utils import get_column_letter
# from openpyxl.workbook import workbookPassword
import pandas as pd
import string
import time

def add_data_validation_to_column(LOB, attribute_df, mapping_df,DropDownMapping_df, password):
    
    wb = Workbook()
     
    ws = wb.create_sheet(LOB, 0)
    
    hidden_ws = wb['Sheet']
    
    hidden_ws.title = 'MappingSheet'
    

    for r in dataframe_to_rows(attribute_df, index=False, header=True):
        ws.append(r)
    for r in dataframe_to_rows(DropDownMapping_df, index=False, header=True):
        hidden_ws.append(r)
        
    wb.create_named_range('PROCESSING_OTHERS', hidden_ws , '$B$2:$B$6')
    wb.create_named_range('PROCESSING', hidden_ws , '$B$7:$B$10')
    wb.create_named_range('NON_PROCESSING', hidden_ws , '$B$11:$B$20')
    wb.create_named_range('PROCESSING_OTHERS_CATEGORY', hidden_ws , '$C$2:$C$6')

    f_mapping_df = mapping_df[mapping_df['DropDownOptions'].notnull()]
    for index, rows in f_mapping_df.iterrows():
        
        if not rows['DropDownOptions']=='Named Range':
            listToStr = '\"' + str(rows['DropDownOptions']) + '\"'
            # Create a data validation object
            dv = DataValidation(type="list", formula1=listToStr, error='Your entry is not valid',
                                errorTitle='Invalid Entry', prompt='Please select from the list', 
                                promptTitle='Select Option')
            
            # Add the data validation to the worksheet
            ws.add_data_validation(dv)

            # Apply the data validation to the column
            for row in ws[rows['Position']]:
                dv.add(row)
                    
        elif (rows['DropDownOptions']=='Named Range') & (rows['Columns'] == 'Role'):            
            # Create a data validation object
            dv = DataValidation(type="list", formula1='INDIRECT(SUBSTITUTE(P1," ","_"))', 
                                allow_blank=True,error='Your entry is not valid',errorTitle='Invalid Entry', 
                                prompt='Please select from the list', promptTitle='Select Option')            
                  
            # Add the data validation to the worksheet
            ws.add_data_validation(dv)

            # Apply the data validation to the column
            for row in ws[rows['Position']]:
                dv.add(row)
                
        elif (rows['DropDownOptions']=='Named Range') & (rows['Columns'] == 'ReasonforProcessingOthers'):
            # Create a data validation object
            dv = DataValidation(type="list", formula1='INDIRECT(SUBSTITUTE(P1," ","_")&"_CATEGORY")', allow_blank=True,
                                error='Your entry is not valid',errorTitle='Invalid Entry', 
                                prompt='Please select from the list', promptTitle='Select Option')            

            # Add the data validation to the worksheet
            ws.add_data_validation(dv)

            # Apply the data validation to the column
            for row in ws[rows['Position']]:
                dv.add(row)
                
        # Formating the main sheet
        my_fill = PatternFill(start_color="C0C0C0", fill_type="solid")
        for rows in ws.iter_rows(min_row=1, max_row=1, min_col=None):
            for cell in rows:
                cell.font = Font(bold=True )
                cell.fill = my_fill
                
        ws.freeze_panes = ws['B2']
        ws.protection.sheet = True
        ws.protection.password = password
        ws.protection.enable()

        for col in list(mapping_df['Position']):
            for cell in ws[col]:
                cell.protection = Protection(locked=False)
                
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(cell.value)
                except:
                    pass
            adjusted_width = (max_length + 2) * 1.2
            ws.column_dimensions[column_letter].width = adjusted_width
            
        

        hidden_ws.sheet_state = 'hidden'
        
        # Save the workbook
        wb.save('C:\\Users\\2014201\\Downloads\\' + LOB+'.xlsx')
        
    return


def set_wb_password(filePath, password):
    xl_file = EnsureDispatch("Excel.Application")
    wb = xl_file.Workbooks.Open(filePath)
    xl_file.DisplayAlerts = False
    xl_file.Visible = False
    wb.SaveAs(filePath, Password = password)
    wb.Close()
    xl_file.Quit()
    
        
        

path = 'C:\\Users\\2014201\Downloads\\'         
AttributesFileName = 'Attributes.xlsx'
MappingFileName = 'Attribute_Mapping.xlsx'

AtrributesFilePath = path + AttributesFileName
MappingFilePath = path + MappingFileName
password = 'WFM_GBS'
                               
attribute_df = pd.read_excel(AtrributesFilePath)

DropDownMapping_df = pd.read_excel(MappingFilePath, sheet_name='DropDownMapping')
ColumnMapping_df = pd.read_excel(MappingFilePath, sheet_name='ColumnMapping')

attribute_df['LOB'] = attribute_df['LOB'].str.upper()

listOfLOBs = list(set(attribute_df['LOB'].str.upper().dropna()))

for LOB in listOfLOBs:
    f_attribute_df = attribute_df.loc[attribute_df['LOB'].str.contains(LOB, case=False,na=False)]
    add_data_validation_to_column(LOB, f_attribute_df, ColumnMapping_df,DropDownMapping_df, password)
#     set_wb_password(path+LOB+'.xlsx', password)

