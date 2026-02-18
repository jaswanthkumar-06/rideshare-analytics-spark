import os
import sqlite3
import pandas as pd

# Paths
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path = os.path.join(base_path, "data", "rides_raw.csv")
db_path = os.path.join(base_path, "rideshare.db")

print("CSV Path:", csv_path)
print("DB Path:", db_path)

# Read CSV
df = pd.read_csv(csv_path)
print("CSV Rows:", len(df))

# Add total_fare column
df["total_fare"] = df["fare_amount"] * df["surge_multiplier"]

# Connect to SQLite
conn = sqlite3.connect(db_path)

# Load into SQL
df.to_sql("rides", conn, if_exists="replace", index=False)
conn.commit()

print("Data inserted into SQL.")

# Verify insertion
count_check = pd.read_sql("SELECT COUNT(*) AS row_count FROM rides;", conn)
print("Rows inside SQL table:")
print(count_check)

# Run sample query
query = """
SELECT driver_id,
       SUM(total_fare) AS revenue
FROM rides
GROUP BY driver_id
ORDER BY revenue DESC
LIMIT 5;
"""

result = pd.read_sql(query, conn)
print("\nTop Drivers:")
print(result)

conn.close()