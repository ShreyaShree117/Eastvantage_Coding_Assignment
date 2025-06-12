import sqlite3
import pandas as pd

# Connect to  SQLite database
conn = sqlite3.connect("../data/DataEngineerETLassignment.db")

# Loading tables into DataFrames
customers = pd.read_sql_query("SELECT * FROM Customer", conn)
sales = pd.read_sql_query("SELECT * FROM Sales", conn)
orders = pd.read_sql_query("SELECT * FROM Orders", conn)
items = pd.read_sql_query("SELECT * FROM Items", conn)

# Merge the tables
df = orders.merge(sales, on="sales_id").merge(customers, on="customer_id").merge(items, on="item_id")
           
# Filter by age and quantity non-null
df = df[(df["age"].between(18, 35)) & (df["quantity"].notnull())]

# Group by customer, age, item and sum quantity
df_sum = df.groupby(["customer_id", "age", "item_name"], as_index=False)["quantity"].sum()

# Filtering zero quantities
df_sum = df_sum[df_sum["quantity"] > 0]

# Renaming and converting quantity to integer
df_sum.columns = ["Customer", "Age", "Item", "Quantity"]
df_sum["Quantity"] = df_sum["Quantity"].astype(int)

# Savin output in CSV file format
df_sum.to_csv("output_pandas.csv", sep=';', index=False)

conn.close()
