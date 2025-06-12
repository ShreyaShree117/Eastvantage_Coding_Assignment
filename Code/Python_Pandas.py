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
df = orders.merge(sales, on="sales_id") \
           .merge(customers, on="customer_id") \
           .merge(items, on="item_id")

# Filter by age and quantity non-null
df = df[(df["age"].between(18, 35)) & (df["quantity"].notnull())]

# Group by customer, age, item and sum quantity
summary = df.groupby(["customer_id", "age", "item_name"], as_index=False)["quantity"].sum()

# Filtering zero quantities
summary = summary[summary["quantity"] > 0]

# Rename and convert quantity to integer
summary.columns = ["Customer", "Age", "Item", "Quantity"]
summary["Quantity"] = summary["Quantity"].astype(int)

# Savin output in CSV file format
summary.to_csv("output_pandas.csv", sep=';', index=False)


conn.close()
