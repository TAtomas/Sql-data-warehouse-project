import pyodbc

def run():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.1.7;"
        "DATABASE=DataWareHouse;"
        "Trusted_Connection=yes;"
    )
    print("🔗 Connecting to SQL Server for bronze...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("▶ Executing: EXEC bronze.load_bronze")
    cursor.execute("EXEC bronze.load_bronze")
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ bronze.load_bronze executed successfully!")

run()