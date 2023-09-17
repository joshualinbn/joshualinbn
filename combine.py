import os
import pandas as pd

def combine_csv(root_path,sub_path):
	folder_path = os.path.join(root_path, sub_path)
	csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
	combined_data = pd.DataFrame()
	for csv_file in csv_files:
    		file_path = os.path.join(folder_path, csv_file)
    		df = pd.read_csv(file_path)
    		combined_data = pd.concat([combined_data, df], ignore_index=True)
	combined_data.to_csv(os.path.join(root_path, 'combined_' + sub_path + '.csv'), index=False)
	print(os.path.join(root_path, 'combined_' + sub_path + '.csv'))

def filter_dataframe(input_file, output_file, filter_value, column_name1,column_name2):
	# Read the CSV file into a DataFrame
	df = pd.read_csv(input_file,low_memory=False)

	# Filter the DataFrame based on the condition
	df.columns = df.columns.str.strip()
	df.columns = df.columns.str.replace(' ', '_')
	df = df[~df[column_name1].str.contains(filter_value, na = False)]	
	filtered_df = df[~df[column_name2].str.contains(filter_value, na = False)]

	# Write the filtered DataFrame back to a CSV file
	filtered_df.to_csv(output_file, index=False)
	os.remove(input_file)
	print(output_file)

# Function to extract last x letters from a string
def extract_last_letters(row):
    x = row['Site'].find(']')+1
    return row['Site'][x:]

# Function to remove last split from a string
def extract_Pindex(row):	
    x = len(row['Site'].split('.'))-1
    return '.'.join(row['Site'].split('.')[:x])

def Rack_Thor (input_file,output_file,selected_columns):
	df = pd.read_csv(input_file,low_memory=False)
	df = df[selected_columns]
	df.columns= [col.replace('Catalog_ID', 'CAT') for col in df.columns]
	df.columns= [col.replace('Equipment_Name', 'RACK') for col in df.columns]
	df.columns= [col.replace('Location_Code', 'LOCATION') for col in df.columns]
	df.columns= [col.replace('Attribute_25', 'STATUS') for col in df.columns]
	df.columns= [col.replace('Attribute_04', 'MAX_POWER') for col in df.columns]
	df.columns= [col.replace('Comments', 'COMMENT') for col in df.columns]
	df['Site'] = df.apply(extract_last_letters, axis=1)
	df['INDEX']=df['Site'].str[:4] + '.' + df['LOCATION']
		#df.columns= df.columns.str.upper()
	df = df.drop_duplicates()
		#print(df.columns)
	df = df.sort_values(by=['INDEX', 'LOCATION'])
	df = df[['INDEX','Site','CAT','RACK','STATUS','LOCATION','Description','MAX_POWER','COMMENT']]
	df.to_csv(output_file, index=False)
	print(output_file)

def Subrack_Thor (input_file,output_file,selected_columns):
	df = pd.read_csv(input_file,low_memory=False)
	df = df[selected_columns]
	df.columns= [col.replace('Catalog_ID', 'CAT') for col in df.columns]
	df.columns= [col.replace('Equipment_Name', 'CHASSIS') for col in df.columns]
	df.columns= [col.replace('Attribute_23', 'STATUS') for col in df.columns]
	df.columns= [col.replace('Location_Code', 'LOCATION') for col in df.columns]
	df.columns= [col.replace('Attribute_01', 'CB1') for col in df.columns]
	df.columns= [col.replace('Attribute_02', 'CB2') for col in df.columns]
	df.columns= [col.replace('Attribute_18', 'SERIAL') for col in df.columns]
	df.columns= [col.replace('Attribute_08', 'NDD') for col in df.columns]
	df.columns= [col.replace('Comments', 'COMMENT') for col in df.columns]
	df['Site'] = df.apply(extract_last_letters, axis=1)
	df['INDEX']=df['Site'].str[:4] + '.' + df['LOCATION']
		#df['PINDEX']=df.apply(extract_Pindex,axis=1)
	df['PINDEX']=df['INDEX'].str.split('.').apply(lambda word: f"{word[0]}.{word[1]}.{word[2]}.{word[3]}")
		#df.columns = df.columns.str.upper()
	df = df.drop_duplicates()
		#print(df.columns)
	df = df.sort_values(by=['INDEX', 'LOCATION'])
	df = df [['INDEX','PINDEX','Site','CAT','CHASSIS','STATUS','LOCATION','Manufacturer','Model','Part','CB1','CB2','SERIAL','NDD','COMMENT']]
	df.to_csv(output_file, index=False)
		#os.remove(input_file)
	print(output_file)

def Card_Thor (input_file,output_file,selected_columns):
	df = pd.read_csv(input_file,low_memory=False)
	df = df[selected_columns]
	df.columns= [col.replace('Catalog_ID', 'CAT') for col in df.columns]
	df.columns= [col.replace('Equipment_Name', 'CARD') for col in df.columns]
	df.columns= [col.replace('Attribute_22', 'STATUS') for col in df.columns]
	df.columns= [col.replace('Location_Code', 'LOCATION') for col in df.columns]
	df.columns= [col.replace('Attribute_17', 'SERIAL') for col in df.columns]
	df.columns= [col.replace('Attribute_07', 'NDD') for col in df.columns]
	df.columns= [col.replace('Comments', 'COMMENT') for col in df.columns]
	df['Site'] = df.apply(extract_last_letters, axis=1)
	df['INDEX']=df['Site'].str[:4] + '.' + df['LOCATION']
		#df['PINDEX']=df.apply(extract_Pindex,axis=1)
	df['PINDEX']=df['INDEX'].str.split('.').apply(lambda word: f"{word[0]}.{word[1]}.{word[2]}.{word[3]}.{word[4]}")
		#df.columns= df.columns.str.upper()
	df = df.drop_duplicates()
		#print(df.columns)
	df = df.sort_values(by=['INDEX', 'LOCATION'])
	df = df [['INDEX','PINDEX','Site','CAT','CARD','STATUS','LOCATION','Manufacturer','Model','Part','SERIAL','NDD','COMMENT']]
	df.to_csv(output_file, index=False)
		#os.remove(input_file)
	print(output_file)

def XFP_Thor (input_file,output_file,selected_columns):
	df = pd.read_csv(input_file,low_memory=False)
	df = df[selected_columns]
	df.columns= [col.replace('Catalog_ID', 'CAT') for col in df.columns]
	df.columns= [col.replace('Parent_Chassis_Name', 'CHASSIS') for col in df.columns]
	df['CHASSIS']=df['CHASSIS'].str.strip()
	df['CARD']=df['Parent_Housing'].str.split(':').apply(lambda word: f"{word[1]}")
	df['CARD']=df['CARD'].str.strip()
	df.columns= [col.replace('Equipment_Name', 'XFP') for col in df.columns]
	df.columns= [col.replace('Object_Status', 'STATUS') for col in df.columns]
	df.columns= [col.replace('Location_Code', 'LOCATION') for col in df.columns]
	df.columns= [col.replace('Master_Circuit_ID', 'MC') for col in df.columns]
	df.columns= [col.replace('E2E_Circuit_Bearer_ID', 'CB') for col in df.columns]
	df['RESERVED']=df['Attribute_05'] + ' ' + df['Attribute_06']
	df.columns= [col.replace('Design_Documentation_ID', 'NDD') for col in df.columns]
	df.columns= [col.replace('Comments', 'COMMENT') for col in df.columns]
	df['Site'] = df.apply(extract_last_letters, axis=1)
	df['PINDEX']=df['Site'].str[:4] + '.' + df['Parent_Housing'].str.split(':').apply(lambda word: f"{word[0]}")
	df['PINDEX']=df['PINDEX'].str.strip()
		#del df['Parent_Housing']
		#del df['Attribute_05']
		#del df['Attribute_06']
	df['shelf'] = df['CHASSIS'].str[18:]
	df['shelf'] = df['shelf'].str.lstrip('0')
	df['slot'] = df['PINDEX'].str[-2:]
	df['slot'] = df['slot'].str.lstrip('0')
	df['INDEX']='OT' + df['CHASSIS'].str[11:14] + '-' + df['Site'].str[:4] + '-00' + df['CHASSIS'].str[15:17] + '.' +df['shelf'] + '.' +df['slot']
		#del df['shelf']
		#del df['slot']
		#df.columns = df.columns.str.upper()
	df = df.drop_duplicates()
		#print(df.columns)
	df = df.sort_values(by=['INDEX', 'LOCATION'])
	df = df [['INDEX','PINDEX','CHASSIS','CARD','CAT','Model','XFP','STATUS','LOCATION','MC','CB','RESERVED','NDD','COMMENT']]
	df.to_csv(output_file, index=False)
		#os.remove(input_file)
	print(output_file)

root_path = 'C:\Support\TDD\Python\PNI_DOWNLOAD'
sub = os.listdir(root_path)
value = "XXXX|1CNI|TEMP|1ABC|xRPA|AAAA|BBBB|CCCC|YYYY|ZZZZ|Template|]-0"
col1 = 'Site'
col2 = 'Equipment_Name'
 
for sub_path in sub:
	if os.path.isdir(os.path.join(root_path,sub_path)):
		if len(os.listdir(os.path.join(root_path,sub_path)))>0:
			combine_csv(root_path,sub_path)
			input_file = os.path.join(root_path, 'combined_' + sub_path + '.csv')
			output_file = os.path.join(root_path, 'PNI_' + sub_path + '.csv')
			if os.path.exists(input_file):
				filter_dataframe(input_file,output_file,value,col1,col2)


# filter and reformat the PNI_RACK.CSV file 
input_file = os.path.join(root_path, 'PNI_RACK.csv')
output_file = os.path.join(root_path, 'PNI_RACK_Thor.csv')
selected_columns = ['Site','Catalog_ID','Equipment_Name','Attribute_25','Location_Code','Description','Attribute_04','Comments']
Rack_Thor (input_file,output_file,selected_columns)

# filter and reformat the PNI_CHASSIS.CSV file 
input_file = os.path.join(root_path, 'PNI_CHASSIS.csv')
output_file = os.path.join(root_path, 'PNI_CHASSIS_Thor.csv')
selected_columns = ['Site','Catalog_ID','Equipment_Name','Attribute_23','Location_Code','Manufacturer','Model','Part','Attribute_01','Attribute_02','Attribute_18','Attribute_08','Comments']
Subrack_Thor (input_file,output_file,selected_columns)

# filter and reformat the PNI_EOF.CSV file 
input_file = os.path.join(root_path, 'PNI_EOF.csv')
output_file = os.path.join(root_path, 'PNI_EOF_Thor.csv')
selected_columns = ['Site','Catalog_ID','Equipment_Name','Attribute_23','Location_Code','Manufacturer','Model','Part','Attribute_01','Attribute_02','Attribute_18','Attribute_08','Comments']
Subrack_Thor (input_file,output_file,selected_columns)

# filter and reformat the PNI_ACT.CSV file 
input_file = os.path.join(root_path, 'PNI_ACT.csv')
output_file = os.path.join(root_path, 'PNI_ACT_Thor.csv')
selected_columns = ['Site','Catalog_ID','Equipment_Name','Attribute_23','Location_Code','Manufacturer','Model','Part','Attribute_01','Attribute_02','Attribute_18','Attribute_08','Comments']
Subrack_Thor (input_file,output_file,selected_columns)

# filter and reformat the PNI_CARD.CSV file 
input_file = os.path.join(root_path, 'PNI_CARD.csv')
output_file = os.path.join(root_path, 'PNI_CARD_Thor.csv')
selected_columns = ['Site','Catalog_ID','Equipment_Name','Attribute_22','Location_Code','Manufacturer','Model','Part','Attribute_17','Attribute_07','Comments']
Card_Thor (input_file,output_file,selected_columns)

# filter and reformat the PNI_XFP.CSV file 
input_file = os.path.join(root_path, 'PNI_XFP.csv')
output_file = os.path.join(root_path, 'PNI_XFP_Thor.csv')
selected_columns = ['Site','Catalog_ID','Parent_Chassis_Name','Parent_Housing','Model','Equipment_Name','Object_Status','Location_Code','Master_Circuit_ID','E2E_Circuit_Bearer_ID','Attribute_05','Attribute_06','Design_Documentation_ID','Comments']
XFP_Thor (input_file,output_file,selected_columns)