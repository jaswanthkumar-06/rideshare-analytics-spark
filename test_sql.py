import sqlite3
import pandas as pd

conn = sqlite3.connect("test.db")

df = pd.DataFrame({
    "a": [1,2,3],
    "b": [10,20,30]
})

df.to_sql("test_table", conn, if_exists="replace", index=False)

result = pd.read_sql("SELECT SUM(b) FROM test_table;", conn)
print(result)

conn.close()