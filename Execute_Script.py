import snowflake.connector
# Connection parameters
snowflake_account = 'DWUTRTQ-GGA40671'
snowflake_user = 'arunkumar.vudayagiri'
snowflake_password = 'Vishank@1'
snowflake_warehouse = 'COMPUTE_WH'
snowflake_database = 'SFBANK_DB'
snowflake_schema = 'POC'

# Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema
)

file_path = "/Users/arunkumar.vudayagiri/Downloads/Results.txt"

# Open the file in read mode
with open(file_path, 'r') as file:
    # Read the entire content
    file_content = file.read()

rows = file_content.split(';')
# Process each row
for row in rows:
    if row:
        row += ';'
        print(row)
        try:
            cursor = conn.cursor()
            cursor.execute(row)
            print("Table created successfully.")
        finally:
            cursor.close()
# Close the connection
conn.close()
# Print or use the generated statement
