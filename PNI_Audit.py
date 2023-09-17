import os
import pandas as pd
import numpy as np

# Function to extract last x letters from a string
def extract_last_letters(row):
    x = row['Parent_Housing'].find(':')+1
    return row['Parent_Housing'][x:]

root_path = 'C:\Support\TDD\Python\PNI_Audit'
sub = os.listdir(root_path)
file1 = 'PNI_XFP.csv'
col1=['SpatialNET_Object','Model','Location_Code','Parent_Chassis_Name','Parent_Housing','Comments','Master_Circuit_ID','E2E_Circuit_Bearer_ID','Object_Status']
file2 = 'DWDM_INV_Thor.csv'
col2=['native_name','logical_name','shelf_id','slot_id','slot_card_type','port_id','port_details','application_code']
report_file = 'XFP_Audit_Report.csv'
dic1 = {'10Gbase-LR & OTU2 XFP':'LR','SFP+ 10GE/10GBE/10km':'LR','10Gbase-ER XFP':'ER','1000Base-LX SFP':'LX','1000Base-ZX SFP':'ZX','10Gbase-ZR XFP':'ZR','SFP+ 10GE/10GBE/40km':'ER','SFP+ 10GE/10GBE/80km':'ZR','CFP4-100G-LR4':'LR4','SFP+ 10G 40KM BIDI 1270T/1330R':'LRU','XFP FC8 800-SM-LC-L':'LR'}
dic2={'10GBASE-LR/ 10km':'LR','OTU2e P1I1-2D1/ 2km':'LR','10GBASE-ER/ 40km':'ER','GbE-LX/ 10km':'LX','GbE-ZX/ 80km':'ZX','10GBASE-ZR/ 80km':'ZR','100GBASE-LR4/ 10km':'LR4','10GBASE-LRU/ 40km':'LRU'}
 
for sub_file in sub:
	if sub_file == file1:
		PNI_df = pd.read_csv(os.path.join(root_path, file1))
		PNI_df = PNI_df[col1]
		#PNI_df = PNI_df[~PNI_df['Object_Status'].str.contains("PLANNED|DECOMMISSIONED|OUTOFSERVICE", na = False)]
		PNI_df['CARD'] = PNI_df.apply(extract_last_letters, axis=1)
		PNI_df['Comments'] = PNI_df['Comments'].str.replace(' ','')
		PNI_df['shelf'] = PNI_df['Parent_Chassis_Name'].str[18:]
		PNI_df['shelf'] = PNI_df['shelf'].str.lstrip('0')
		PNI_df['slot'] = PNI_df['Parent_Housing'].str[-24:-22]
		PNI_df['slot'] = PNI_df['slot'].str.lstrip('0')
		PNI_df['port'] = PNI_df['Location_Code'].str[-2:]
		PNI_df['port'] = PNI_df['port'].str.lstrip('0')
		#PNI_df['port'] = PNI_df['port'].astype(int)
		PNI_df['INDEX'] = 'OT' + PNI_df['Parent_Chassis_Name'].str[11:14] + '-' + PNI_df['Parent_Chassis_Name'].str[:4] + '-00' + PNI_df['Parent_Chassis_Name'].str[15:17] + '.' + PNI_df['shelf'] + '.' + PNI_df['slot'] + '.' + PNI_df['port'].astype(str)
		PNI_df= PNI_df.sort_values(by=['Parent_Chassis_Name','slot', 'port'])
		PNI_df= PNI_df[['SpatialNET_Object','INDEX','CARD','Comments','Model','Master_Circuit_ID','E2E_Circuit_Bearer_ID','Object_Status']]

	if sub_file == file2:
		TNMS_df = pd.read_csv(os.path.join(root_path, file2))
		TNMS_df = TNMS_df[col2]
		TNMS_df['application_code'] = TNMS_df['application_code'].astype(str)
		TNMS_df=TNMS_df[~TNMS_df['application_code'].str.contains('OTU-3v|OTU-4v')]
		TNMS_df['INDEX'] = TNMS_df['logical_name'] + '.' +  TNMS_df['shelf_id'].astype(str) + '.' + TNMS_df['slot_id'].astype(str) + '.' + TNMS_df['port_id'].astype(str)
		TNMS_df = TNMS_df[['INDEX','slot_card_type','native_name','application_code']]
		#TNMS_df.columns=TNMS_df.columns.str.replace('native_name','TNMS_name')
		#TNMS_df.columns=TNMS_df.columns.str.replace('application_code','TNMS_model')
		TNMS_df = TNMS_df.rename(columns={'native_name':'TNMS_name','application_code':'TNMS_model','slot_card_type':'TNMS_Card'})
#PNI_df['TNMS_Model'] = TNMS_df['application_code'].loc[TNMS_df['INDEX'] == PNI_df['INDEX']]

Report_df = PNI_df.merge(TNMS_df,on=['INDEX'],how = 'left')
Report_df['xfp1'] = Report_df['Model'].map(dic1)
Report_df['xfp2'] = Report_df['TNMS_model'].map(dic2)

#Report_df['XFP_CHK'] = Report_df.apply(lambda row: row['xfp1'] == row ['xfp2'] , axis = 1)
# Create a new column 'XFP_CHK' using np.where
Report_df['XFP_CHK'] = np.where(Report_df['xfp1'] == Report_df['xfp2'], 'Match', 
                                   np.where(Report_df['xfp1'].isna() | Report_df['xfp2'].isna(), np.nan, 'Mismatch'))
#Report_df['ET_CHK'] = Report_df.apply(contains_value(Report_df['Comments'],Report_df['TNMS_name']))
Report_df['TNMS_name2']=Report_df['TNMS_name'].str[:19]
#Report_df['ET_CHK'] = Report_df['Comments'].isin(Report_df['TNMS_name2'])
Report_df['ET_CHK'] = np.where(Report_df['Comments']==Report_df['TNMS_name2'], 'Match',
                                   np.where(Report_df['TNMS_name2'].isna()|(Report_df['TNMS_name2'].str.contains("OT2|-100|-150")), 'ignore','Mismatch'))
del Report_df['xfp1']
del Report_df['xfp2']
del Report_df['TNMS_name2']
Report_df.to_csv(os.path.join(root_path, report_file), index=False)
print (os.path.join(root_path, report_file))