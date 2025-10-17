
import pyodbc

def run():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.1.7;"
        "DATABASE=DataWareHouse;"
        "Trusted_Connection=yes;"
    )
    print("🔗 Connecting to SQL Server for silver...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("▶ Executing: EXEC silver.load_silver")
    cursor.execute("EXEC silver.load_silver")
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ silver.load_silver executed successfully!")


run()