import tkinter as tk
from snowflake.connector import connect, ProgrammingError
from tkinter import filedialog
from tkinter import messagebox

def execute_script():
    try:
        # Get Snowflake connection details from the entry widgets
        username = username_entry.get()
        password = password_entry.get()
        account = account_entry.get()
        warehouse = warehouse_entry.get()
        database = database_entry.get()
        schema = schema_entry.get()

        # Snowflake connection
        con = connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        # Get script file path using file dialog
        script_file_path = filedialog.askopenfilename(title="Select Script File", filetypes=[("SQL Files", "*.sql")])

        # Read and execute the script
        with open(script_file_path, 'r') as script_file:
            sql_script = script_file.read()
            rows = sql_script.split(';')
            for row in rows:
                if row:
                    #print(row)
                    #row += ';'
                    try:
                        cur = con.cursor()
                        cur.execute(row)
                    finally:
                        cur.close()
            messagebox.showinfo("Information", "Script executed successfully....!")
            for widget in window.winfo_children():
                widget.config(state="disabled")

    except ProgrammingError as e:
        messagebox.showinfo("Error","Error:"+ str(e))

# Create the main window
window = tk.Tk()
window.title("Snowflake Connection and Script Execution")
window.geometry("+700+250")
window.resizable(False, False)

# Create and pack widgets
username_label = tk.Label(window, text="Username:")
username_label.pack(pady=5)
username_entry = tk.Entry(window)
username_entry.pack(pady=5)

password_label = tk.Label(window, text="Password:")
password_label.pack(pady=5)
password_entry = tk.Entry(window, show="*")
password_entry.pack(pady=5)

account_label = tk.Label(window, text="Account:")
account_label.pack(pady=5)
account_entry = tk.Entry(window)
account_entry.pack(pady=5)

warehouse_label = tk.Label(window, text="Warehouse:")
warehouse_label.pack(pady=5)
warehouse_entry = tk.Entry(window)
warehouse_entry.pack(pady=5)

database_label = tk.Label(window, text="Database:")
database_label.pack(pady=5)
database_entry = tk.Entry(window)
database_entry.pack(pady=5)

schema_label = tk.Label(window, text="Schema:")
schema_label.pack(pady=5)
schema_entry = tk.Entry(window)
schema_entry.pack(pady=5)


script_button = tk.Button(window, text="Execute Script", command=execute_script)
script_button.pack(pady=5)

# Start the main event loop
window.mainloop()
