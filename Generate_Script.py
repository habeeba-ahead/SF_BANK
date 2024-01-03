import pandas as pd
import os
from datetime import datetime


def generate_script_file(metadata_file_path,script_folder_path):
    metadata_df = pd.read_csv(metadata_file_path)
    for _, row in metadata_df.iterrows():
        table_name = row['Table_Description'].upper().replace(' ','_')
    current_datetime = datetime.now()
    formatted_date = current_datetime.strftime("%Y%m%d_%H%M%S")
    script_file_name = row['Table_Name']+'_'+table_name+'_'+formatted_date+'.sql'
    script_file_path = os.path.join(script_folder_path, script_file_name)
    return script_file_path
#Function for Stage Table Create Script Generation
def generate_create_stg_table(metadata_file_path,script_file_path,log_file_path):
    # Read metadata CSV file into a DataFrame
    metadata_df = pd.read_csv(metadata_file_path)
    # Create a dictionary to map Source data types to Snowflake data types
    snowflake_data_types =  {
        'CHARACTER': 'VARCHAR(16777216)',
        'NUMERIC': 'NUMBER(38,0)',
        'DECIMAL': 'NUMBER(38,6)',
        'DATE': 'DATE',
    }
    # Extract column definitions from metadata
    column_definitions = []
    for _, row in metadata_df.iterrows():
        table_name = row['Table_Description'].upper().replace(' ','_')
        table_comment = row['Table_Description']
        column_name = row['Column_Description'].upper().replace(' ','_')
        column_name = column_name.replace('INFORMATION','INFO').replace('DESCRIPTION','DESC')
        column_comment = ' COMMENT =\'' + row['Column_Description'] + '\''
        data_type = row['Data_Type']
        if row['Nullable'] == 'Y':
            nullable = ''
        else:
            nullable = ' NOT NULL'
        snowflake_dtype = snowflake_data_types.get(data_type, row['Data_Type'])
        if row['Data_Type'] == 'CHARACTER':
            if row['Size'] == 1 :
                snowflake_dtype = 'VARCHAR(1)'
        # Add column definition to the list
        column_definitions.append(f"\n{column_name} {snowflake_dtype}{nullable}{column_comment}")

    # Create the Snowflake CREATE TABLE statement
    create_table_statement = f"\nCREATE OR REPLACE TABLE {table_name}_STG\n({','.join(column_definitions)}"
    create_table_statement += f"\n)\nCOMMENT = '{table_comment}';"
    #Spool the generated script to a File
    with open(script_file_path, 'w') as file:
        file.write("\n--Create Table Statement....")
        file.write(create_table_statement)
        with open(log_file_path,'a') as file:
            current_datetime = datetime.now()
            file.write('\nGenerated DDL Script file :'+script_file_path+'....')
            file.write('\n'+current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")+':')
            file.write('\nStage Table creation script has been generated....')
    return True;

# Function for Snowpipe Definition Code Generation
def generate_create_snowpipe(metadata_file_path,script_file_path,log_file_path):
    metadata_df = pd.read_csv(metadata_file_path)
    for _, row in metadata_df.iterrows():
        table_name = row['Table_Description'].upper().replace(' ','_')
    create_snowpipe_statement = f"\nCREATE OR REPLACE PIPE PIPE_"+table_name+"\nAUTO_INGEST=TRUE\nINTEGRATION='INTEGRATION_NAME'\nAS\nCOPY INTO "+table_name
    create_snowpipe_statement += f"\nFROM @STAGE_SFBANK"
    create_snowpipe_statement += f"\nPATTERN = "+'\'.*'+row['Table_Name']+'*.csv\''
    create_snowpipe_statement += f"\nFILE_FORMAT = (FORMAT_NAME = FF_CSV);"
    with open(script_file_path, 'a') as file:
        file.write("\n\n--Create Snowpipe Statement....")
        file.write(create_snowpipe_statement)
    with open(log_file_path,'a') as file:
        current_datetime = datetime.now()
        file.write('\n'+current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")+':')
        file.write('\nSnowpipe creation script has been generated....')
    return True;

# Function for Stream Definition Code Generation
def generate_create_stream(metadata_file_path,script_file_path,log_file_path):
    metadata_df = pd.read_csv(metadata_file_path)
    for _, row in metadata_df.iterrows():
        table_name = row['Table_Description'].upper().replace(' ','_')
    create_stream_statement = f"\nCREATE OR REPLACE STREAM STREAM_"+table_name+" ON "+table_name+"_STG;"
    with open(script_file_path, 'a') as file:
        file.write("\n\n--Create Stream Statement....")
        file.write(create_stream_statement)
    with open(log_file_path,'a') as file:
        current_datetime = datetime.now()
        file.write('\n'+current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")+':')
        file.write('\nStream creation script has been generated....')
    return True;


#Function for Raw Table Create Script Generation
def generate_create_raw_table(metadata_file_path,script_file_path,log_file_path):
    # Read metadata CSV file into a DataFrame
    metadata_df = pd.read_csv(metadata_file_path)
    snowflake_data_types =  {
        'CHARACTER': 'VARCHAR(16777216)',
        'NUMERIC': 'NUMBER(38,0)',
        'DECIMAL': 'NUMBER(38,6)',
        'DATE': 'DATE',
    }
    # Extract column definitions from metadata
    column_definitions = []
    for _, row in metadata_df.iterrows():
        table_name = row['Table_Description'].upper().replace(' ','_')
        table_comment = row['Table_Description']
        if row['Column_Description'] is None:
            column_name = row['Column_Name']
        else:
            column_name = row['Column_Description'].upper().replace(' ','_')
            column_name = column_name.replace('INFORMATION','INFO').replace('DESCRIPTION','DESC')
        column_comment = ' COMMENT =\'' + row['Column_Description'] + '\''
        data_type = row['Data_Type']
        if row['Nullable'] == 'Y':
            nullable = ''
        else:
            nullable = ' NOT NULL'
        snowflake_dtype = snowflake_data_types.get(data_type, row['Data_Type'])
        if row['Data_Type'] == 'CHARACTER':
            if row['Size'] == 1 :
                snowflake_dtype = 'VARCHAR(1)'
        # Add column definition to the list
        column_definitions.append(f"\n{column_name} {snowflake_dtype}{nullable}{column_comment}")

    # Create the Snowflake CREATE TABLE statement
    create_table_statement = f"\nCREATE OR REPLACE TABLE {table_name}_RAW\n({','.join(column_definitions)},\n"
    create_table_statement += f"INSERT_DATE TIMESTAMP_NTZ COMMENT = 'Time of Data Insertion',\nFILE_DATE DATE COMMENT = 'Date of File Generation',\nFILE_NAME VARCHAR(16777216) COMMENT = 'Name of the Source File'\n)\nCOMMENT = '{table_comment}';"
    #Spool the generated script to a File
    with open(script_file_path, 'a') as file:
        file.write("\n\n--Create Table Statement....")
        file.write(create_table_statement)
        with open(log_file_path,'a') as file:
            current_datetime = datetime.now()
            file.write('\n'+current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")+':')
            file.write('\nRaw Table creation script has been generated....')
    return True;

#Function for Scheduled Task Create Script Generation
def generate_create_task(metadata_file_path,script_file_path,log_file_path):
    return True;

# Log File Generation
current_datetime = datetime.now()
formatted_date = current_datetime.strftime("%Y%m%d_%H%M%S")
base_file_name = "Log_"
file_extention = ".txt"
#log_folder_path = '/Users/arunkumar.vudayagiri/Documents/Work/Sunflower Bank/Python/Logs/'
log_folder_path = './Logs/'
log_file_name = f"{base_file_name}{formatted_date}{file_extention}"
log_file_path = os.path.join(log_folder_path, log_file_name)
metadata_file_path = "/Users/arunkumar.vudayagiri/Downloads/Customer_Master.csv"
script_folder_path = '/Users/arunkumar.vudayagiri/Documents/Work/Sunflower Bank/Python/Scripts/'
with open(log_file_path,'w') as file:
    file.write('\n'+current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")+':')
    file.write('\nLog file generated in the path:'+log_file_path+'....')
with open(log_file_path,'a') as file:
    current_datetime = datetime.now()
    file.write('\n'+current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")+':')
    file.write('\nMetadata file specified for DDL generation:'+metadata_file_path+'....')
script_file_path = generate_script_file(metadata_file_path,script_folder_path)

'''
#Commented Code Starts here
if generate_create_stg_table(metadata_file_path,script_file_path,log_file_path):
    print("Create Stage Stable Script has been generated successfully....")
else:
    print("Create Stage Table Script generation failed.... Please review the logs at:"+log_file_path+" and take necessary steps....")
if generate_create_snowpipe(metadata_file_path,script_file_path,log_file_path):
    print("Create Snowpipe Script has been generated successfully....")
else:
    print("Create Snowpipe Script generation failed.... Please review the logs at:"+log_file_path+" and take necessary steps....")
if generate_create_stream(metadata_file_path,script_file_path,log_file_path):
    print("Create Stream Script has been generated successfully....")
else:
    print("Create Stream Script generation failed.... Please review the logs at:"+log_file_path+" and take necessary steps....")
if generate_create_raw_table(metadata_file_path,script_file_path,log_file_path):
    print("Create Raw Table Script has been generated successfully....")
else:
    print("Create Raw Table Script generation failed.... Please review the logs at:"+log_file_path+" and take necessary steps....")
if generate_create_task(metadata_file_path,script_file_path,log_file_path):
    print("Create Task Script has been generated successfully....")
else:
    print("Create Task Script generation failed.... Please review the logs at:"+log_file_path+" and take necessary steps....")
#Commented Code Ends here
'''
generate_create_stg_table(metadata_file_path,script_file_path,log_file_path)
generate_create_snowpipe(metadata_file_path,script_file_path,log_file_path)
generate_create_stream(metadata_file_path,script_file_path,log_file_path)
generate_create_raw_table(metadata_file_path,script_file_path,log_file_path)
generate_create_task(metadata_file_path,script_file_path,log_file_path)
print("Metadata File used for DDL Generation:" + metadata_file_path)
print("Script has been generated in the file path:"+script_file_path)
print("Logs has been generated in the file path:"+log_file_path)
