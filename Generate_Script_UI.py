import pandas as pd
import os
from datetime import datetime
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
from functools import partial

checkbox_vars = []
checkbuttons = []
labels = ["Stage Table","SnowPipe","Stream for CDC","Raw Table","Scheduled Task"]

#Function to write the DDL scripts
def write_ddl_file(abs_ddl_file_path, ddl_stmt, ddl_comment):
    with open(abs_ddl_file_path,'a') as file:
        file.write(ddl_comment)
        file.write(ddl_stmt)


#Function for Stage Table Create Script Generation
def generate_create_stg_table(md_file_path,abs_ddl_file_path,abs_log_file_path):
    # Read metadata CSV file into a DataFrame
    metadata_df = pd.read_csv(md_file_path)
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
    ddl_comment = "\n\n--Create Table Statement...."
    ddl_stmt = "\n"+create_table_statement
    write_ddl_file(abs_ddl_file_path,ddl_stmt, ddl_comment)
    return True

# Function for Snowpipe Definition Code Generation
def generate_create_snowpipe(md_file_path,abs_ddl_file_path,abs_log_file_path):
    metadata_df = pd.read_csv(md_file_path)
    for _, row in metadata_df.iterrows():
        table_name = row['Table_Description'].upper().replace(' ','_')
    create_snowpipe_statement = f"\nCREATE OR REPLACE PIPE PIPE_"+table_name+"\nAUTO_INGEST=TRUE\nINTEGRATION='INTEGRATION_NAME'\nAS\nCOPY INTO "+table_name
    create_snowpipe_statement += f"\nFROM @STAGE_SFBANK"
    create_snowpipe_statement += f"\nPATTERN = "+'\'.*'+row['Table_Name']+'*.csv\''
    create_snowpipe_statement += f"\nFILE_FORMAT = (FORMAT_NAME = FF_CSV);"
    ddl_comment = "\n\n--Create Snowpipe Statement...."
    ddl_stmt = '\n' + create_snowpipe_statement
    write_ddl_file(abs_ddl_file_path,ddl_stmt,ddl_comment)
    return True;

# Function for Stream Definition Code Generation
def generate_create_stream(md_file_path,abs_ddl_file_path,abs_log_file_path):
    metadata_df = pd.read_csv(md_file_path)
    for _, row in metadata_df.iterrows():
        table_name = row['Table_Description'].upper().replace(' ','_')
    create_stream_statement = f"\nCREATE OR REPLACE STREAM STREAM_"+table_name+" ON "+table_name+"_STG;"
    ddl_comment = "\n\n--Create Stream Statement...."
    ddl_stmt = '\n' + create_stream_statement
    write_ddl_file(abs_ddl_file_path,ddl_stmt,ddl_comment)
    return True;

#Function for Raw Table Create Script Generation
def generate_create_raw_table(md_file_path,abs_ddl_file_path,abs_log_file_path):
    # Read metadata CSV file into a DataFrame
    metadata_df = pd.read_csv(md_file_path)
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
    ddl_comment = "\n\n--Create Table Statement...."
    ddl_stmt = "\n"+create_table_statement
    write_ddl_file(abs_ddl_file_path,ddl_stmt, ddl_comment)
    return True;

#Function for Scheduled Task Script Generation
def generate_create_task(md_file_path,abs_ddl_file_path,abs_log_file_path):
    return True;

#Function to Generate DDL File
def generate_ddl_file(abs_log_file_path):
    current_datetime = datetime.now()
    formatted_date = current_datetime.strftime("%Y%m%d_%H%M%S")
    base_file_name = "DDL_"
    file_extention = ".sql"
    ddl_folder_path = './Scripts/'
    ddl_file_name = f"{base_file_name}{formatted_date}{file_extention}"
    ddl_file_path = os.path.join(ddl_folder_path, ddl_file_name)
    abs_ddl_file_path = os.path.abspath(os.path.join(ddl_folder_path, ddl_file_name))
    with open(abs_ddl_file_path,'w') as file:
        file.write("--DDL Generation Starts here....!")
    return abs_ddl_file_path

#Function to write logs
def write_logs(abs_log_file_path,log_text):
    with open(abs_log_file_path,'a') as file:
        current_datetime = datetime.now()
        file.write('\n'+current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")+':')
        file.write('\n')
        file.write(log_text)

#Function to generate Log File
def generate_log_file():
    current_datetime = datetime.now()
    formatted_date = current_datetime.strftime("%Y%m%d_%H%M%S")
    base_file_name = "Log_"
    file_extention = ".txt"
    log_folder_path = './Logs/'
    log_file_name = f"{base_file_name}{formatted_date}{file_extention}"
    log_file_path = os.path.join(log_folder_path, log_file_name)
    abs_log_file_path = os.path.abspath(os.path.join(log_folder_path, log_file_name))
    with open(abs_log_file_path,'w') as file:
        log_text = 'Log file has been generated in the below path:'+'\n '+abs_log_file_path
        write_logs(abs_log_file_path,log_text)
    return abs_log_file_path

#MainFunction that runs on Click of Generate DDl button
def on_generate(md_file_path):
    abs_log_file_path = generate_log_file()
    log_text = 'Metadata File used for DDL Generation:'+'\n '+md_file_path
    write_logs(abs_log_file_path,log_text)
    if checkbox_vars[0].get() or checkbox_vars[1].get() or checkbox_vars[2].get() or checkbox_vars[3].get() or checkbox_vars[4].get():
        abs_ddl_file_path = generate_ddl_file(abs_log_file_path)
        log_text = 'DDL file has been generated in the below path:'+'\n '+abs_ddl_file_path
        write_logs(abs_log_file_path,log_text)
        if checkbox_vars[0].get():
            generate_create_stg_table(md_file_path,abs_ddl_file_path,abs_log_file_path)
            log_text = 'DDL Script for Stage Table has been generated successfully....'
            write_logs(abs_log_file_path,log_text)
        if checkbox_vars[1].get():
            generate_create_snowpipe(md_file_path,abs_ddl_file_path,abs_log_file_path)
            log_text = 'DDL Script for SnowPipe has been generated successfully....'
            write_logs(abs_log_file_path,log_text)
        if checkbox_vars[2].get():
            generate_create_stream(md_file_path,abs_ddl_file_path,abs_log_file_path)
            log_text = 'DDL Script for Stream for CDC has been generated successfully....'
            write_logs(abs_log_file_path,log_text)
        if checkbox_vars[3].get():
            generate_create_raw_table(md_file_path,abs_ddl_file_path,abs_log_file_path)
            log_text = 'DDL Script for Raw Table has been generated successfully....'
            write_logs(abs_log_file_path,log_text)
        if checkbox_vars[4].get():
            generate_create_task(md_file_path,abs_ddl_file_path,abs_log_file_path)
            log_text = 'DDL Script for Scheduled Task has been generated successfully....'
            write_logs(abs_log_file_path,log_text)
        with open(abs_ddl_file_path,'a') as file:
            file.write("\n\n--DDL Generation Ends here....!")
        for widget in window.winfo_children():
            widget.config(state="disabled")
        messagebox.showinfo("Information", "DDL generation completed successfully....!")
    else:
        messagebox.showinfo("Error", "Please choose an object type for DDL generation....!")

#Function to capture the object types selected for DDL Generation
def on_checkbox_clicked(checkbox_index):
    value = checkbox_vars[checkbox_index].get()

#Function to initiate on button click event
def on_button_click(in_md_file_path):
    on_generate(in_md_file_path)

#Function to generate the UI options to choose objects
def generate_options(md_file_path):
    label = tk.Label(window, text="Choose the object type:")
    label.grid(row=3, column = 0, padx =10, pady = 10, sticky = "w")
    for i in range(5):  # Create 5 checkboxes
        var = tk.BooleanVar()
        checkbox_vars.append(var)
        checkbox = tk.Checkbutton(window, text=labels[i], variable=var, command=lambda i=i: on_checkbox_clicked(i))
        checkbox.grid(row = 4+i, column = 0, padx =10, pady = 0, sticky = "w")
        checkbuttons.append(checkbox)
    button_generate = tk.Button(window, text = "Generate DDL", command = partial(on_button_click, md_file_path))
    button_generate.grid(row=9, column = 1, padx =10, pady = 10)

#Function to view file browser and choose Metadata file
def browse_file():
    md_file_path = filedialog.askopenfilename(filetypes=[("Supports CSV files only", "*.csv")])
    if md_file_path:
        file_entry.delete(0, tk.END)  # Clear any previous text
        file_entry.insert(0, md_file_path)  # Insert the selected file path
        generate_options(md_file_path)

# Create the main window
window = tk.Tk()
window.title("Snowflake DDL Generator")
window.geometry("+500+250")
window.resizable(False, False)

file_label = tk.Label(window, text="Select Metadata File:")
file_label.grid(row=0, column=0, padx=10, pady=10, sticky="w")

file_entry = tk.Entry(window, width=50)
file_entry.grid(row=1, column=0, padx=10, pady=10)

browse_button = tk.Button(window, text="Browse", command=browse_file)
browse_button.grid(row=1, column=1, padx=10, pady=10)

# Start the main event loop
window.mainloop()
