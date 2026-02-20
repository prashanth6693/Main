# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a9a1e87a-de4c-4634-884b-1b836bb1f2d0",
# META       "default_lakehouse_name": "Bronze_Layer",
# META       "default_lakehouse_workspace_id": "0ef96a5d-6189-4a8b-a938-4fc18e107cf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "a9a1e87a-de4c-4634-884b-1b836bb1f2d0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_customers = 1000

df_customers = pd.DataFrame({
    "CustomerID": range(1, num_customers + 1),
    "CustomerName": ["Customer_" + str(i) for i in range(1, num_customers + 1)],
    "City": np.random.choice(["New York", "Chicago", "Dallas", "Miami", "Seattle"], num_customers),
    "AccountType": np.random.choice(["Savings", "Current", "Credit"], num_customers),
    "Status": np.random.choice(["Active", "Inactive"], num_customers),
    "Age": np.random.randint(18, 75, num_customers)
})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_branches = pd.DataFrame({
    "BranchID": range(1, 21),
    "BranchCity": np.random.choice(["New York", "Chicago", "Dallas", "Miami", "Seattle"], 20),
    "Region": np.random.choice(["North", "South", "East", "West"], 20)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_accounts = pd.DataFrame({
    "CustomerID": range(1, num_customers + 1),
    "BranchID": np.random.randint(1, 21, num_customers),
    "Balance": np.random.randint(1000, 50000, num_customers)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_transactions = 5000
start_date = datetime(2023, 1, 1)

df_transactions = pd.DataFrame({
    "TransactionID": range(1, num_transactions + 1),
    "CustomerID": np.random.randint(1, num_customers + 1, num_transactions),
    "Amount": np.random.randint(100, 5000, num_transactions),
    "TransactionType": np.random.choice(["Deposit", "Withdrawal", "Transfer"], num_transactions),
    "TransactionDate": [start_date + timedelta(days=random.randint(0, 365)) for _ in range(num_transactions)]
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customers.to_csv("/lakehouse/default/Files/customers.csv", index=False)
df_branches.to_csv("/lakehouse/default/Files/branches.csv", index=False)
df_accounts.to_csv("/lakehouse/default/Files/accounts.csv", index=False)
df_transactions.to_csv("/lakehouse/default/Files/transactions.csv", index=False)

print("Data generated successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
