import sqlite3
import pandas as pd

# Connection to the  database
conn = sqlite3.connect("DataEngineerETLassignment.db")

# SQL query to get total quantities by customer (age 18–35) and item
sql_query = """
SELECT 
    c.customer_id AS Customer,
    c.age AS Age,
    i.item_name AS Item,
    SUM(o.quantity) AS Quantity
FROM Customer c
JOIN Sales s ON c.customer_id = s.customer_id
JOIN Orders o ON s.sales_id = o.sales_id
JOIN Items i ON o.item_id = i.item_id
WHERE c.age BETWEEN 18 AND 35
  AND o.quantity IS NOT NULL
GROUP BY c.customer_id, c.age, i.item_name
HAVING SUM(o.quantity) > 0
ORDER BY c.customer_id, i.item_name;
"""

# Saving results in csv format
df = pd.read_sql_query(sql_query, conn)
df.to_csv("../output/output_sql.csv", sep=';', index=False)

conn.close()
