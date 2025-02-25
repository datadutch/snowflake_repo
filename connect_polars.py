# https://docs.snowflake.com/en/user-guide/python-connector.html

import snowflake.connector
import json
import polars as pl

with open('config.json','r') as file:
    data = json.load(file)
    user = data['user']
    password = data['password']
    account = data['account']

con = snowflake.connector.connect(
    user = user,
    password = password,
    account = account
)

con.cursor().execute("USE WAREHOUSE COMPUTE_WH;") 

query_inf = "SELECT * FROM DEMO.INFORMATION_SCHEMA.TABLES;"

# Execute the query and fetch all results
cursor = con.cursor()
cursor.execute(query_inf)
result_set = cursor.fetchall()

# Get column names
columns = [desc[0] for desc in cursor.description]

# Create a Polars DataFrame
df = pl.DataFrame(result_set, schema=columns)
print(df)
