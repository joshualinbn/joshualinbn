import pandas as pd
from pandas.io.formats import excel
excel.ExcelFormatter.header_style = None
import os

# Function to extract last x letters from a string
def extract_last_letters(row):
    x = row['Site'].find(']')+1
    return row['Site'][x:]

# Function to remove last split from a string
def extract_Pindex(row):	
    x = len(row['Location Code'].split('.'))-1
    y = '.'.join(row['Location Code'].split('.')[:x])
    return y

# Function to remove last split from a string
def extract_index(row):	
    values = [' ', 'CLIENT','LINE']
    x = row['Location Code'].upper()
    for value in values:
        x = x.replace(value,'')
    return x

# Function to rename the sheet name

def sheet_rename (sheet_name):
    dictionary = {'PNI_RACK':'RACK','PNI_EOF':'OEF','PNI_CHASSIS':'SHELF','PNI_CARD':'CARD','PNI_XFP_D':'XFP'}
    if sheet_name:
        sh_name = dictionary[sheet_name]
    return sh_name 

root_path = 'C:\Support\TDD\Python\PNI_DOWNLOAD'

# List of CSV files to combine
csv_files = ['PNI_RACK.csv', 'PNI_EOF.csv', 'PNI_CHASSIS.csv','PNI_CARD.CSV','PNI_XFP_D.CSV']

# Create an Excel writer object
excel_writer = pd.ExcelWriter(os.path.join(root_path, 'PNI_DATA.xlsx'), engine='openpyxl') #'xlsxwriter'

# Loop through CSV files and write each to a separate sheet in the Excel file
for csv_file in csv_files:
    sheet_name = csv_file[:-4] #rename CSV file name as sheet name
    sheet_name = sheet_rename (sheet_name)
    print(sheet_name, csv_file)
    if sheet_name != 'None':
        data = pd.read_csv(os.path.join(root_path, csv_file), low_memory=False)
        data.columns= data.columns.str.replace('_',' ')
        data['Site'] = data.apply(extract_last_letters, axis=1)
        if sheet_name == 'SHELF':
            data.columns = data.columns.str.replace('Description','Shelf_No')
            data['Shelf_No'] = data['Equipment Name'].str[-2:]
            data.columns = data.columns.str.replace('RE Symbol','INDEX')
            data['PINDEX'] = data.apply(extract_Pindex, axis=1)
            data['INDEX'] = data['Site'].str[:4] + '.' + data['PINDEX']   
            data = data.sort_values(by=['INDEX', 'Location Code'])
        if sheet_name == 'XFP':
            data.columns = data.columns.str.replace('Description','INDEX')
            data['PINDEX'] = data.apply(extract_index, axis=1)
            data['INDEX'] = data['Site'].str[:4] + '.' + data['PINDEX']
            data = data.sort_values(by=['INDEX', 'Location Code'])
        data.to_excel(excel_writer, sheet_name=sheet_name, index=False)

# Save the Excel file
excel_writer._save()

print('CSV files combined and saved as Excel.')
