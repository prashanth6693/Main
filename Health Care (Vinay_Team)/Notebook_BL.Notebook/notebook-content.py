# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a4389d3c-ef32-4e21-9bf6-7c54eeb16bd4",
# META       "default_lakehouse_name": "BronzeLayer_LH",
# META       "default_lakehouse_workspace_id": "0ef96a5d-6189-4a8b-a938-4fc18e107cf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "a4389d3c-ef32-4e21-9bf6-7c54eeb16bd4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

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

NUM_PATIENTS = 20000
NUM_DOCTORS = 500
NUM_VISITS = 100000

np.random.seed(42)
random.seed(42)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_departments = pd.DataFrame({
    "DepartmentID": [f"DEP{str(i).zfill(2)}" for i in range(1, 11)],
    "DepartmentName": [
        "Cardiology", "Orthopedics", "Neurology", "Oncology", "Pediatrics",
        "Emergency", "Dermatology", "ENT", "Urology", "Gastroenterology"
    ]
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_doctors = pd.DataFrame({
    "DoctorID": [f"D{str(i).zfill(4)}" for i in range(1, NUM_DOCTORS + 1)],
    "DoctorName": [f"Doctor_{i}" for i in range(1, NUM_DOCTORS + 1)],
    "Specialization": np.random.choice(df_departments["DepartmentName"], NUM_DOCTORS),
    "DepartmentID": np.random.choice(df_departments["DepartmentID"], NUM_DOCTORS)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_date = pd.Timestamp("1935-01-01")

df_patients = pd.DataFrame({
    "PatientID": [f"P{str(i).zfill(6)}" for i in range(1, NUM_PATIENTS + 1)],
    "Gender": np.random.choice(["M", "F"], NUM_PATIENTS),
    "DateOfBirth": base_date + pd.to_timedelta(
        np.random.randint(0, 90 * 365, NUM_PATIENTS),
        unit="D"
    ),
    "City": np.random.choice(
        ["Bangalore", "Hyderabad", "Chennai", "Pune", "Mumbai", "Delhi"],
        NUM_PATIENTS
    ),
    "InsuranceType": np.random.choice(
        ["Private", "Government", "SelfPay"],
        NUM_PATIENTS,
        p=[0.5, 0.3, 0.2]
    )
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

visit_start = datetime(2022, 1, 1)

visit_dates = [
    visit_start + timedelta(days=random.randint(0, 900))
    for _ in range(NUM_VISITS)
]

stay_days = np.random.randint(1, 15, NUM_VISITS)

df_visits = pd.DataFrame({
    "VisitID": [f"V{str(i).zfill(7)}" for i in range(1, NUM_VISITS + 1)],
    "PatientID": np.random.choice(df_patients["PatientID"], NUM_VISITS),
    "DoctorID": np.random.choice(df_doctors["DoctorID"], NUM_VISITS),
    "DepartmentID": np.random.choice(df_departments["DepartmentID"], NUM_VISITS),
    "VisitDate": visit_dates,
    "DischargeDate": [visit_dates[i] + timedelta(days=int(stay_days[i])) for i in range(NUM_VISITS)],
    "AdmissionType": np.random.choice(["Emergency", "Planned"], NUM_VISITS),
    "TotalCost": np.random.randint(5000, 250000, NUM_VISITS)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_billing = pd.DataFrame({
    "BillID": [f"B{str(i).zfill(7)}" for i in range(1, NUM_VISITS + 1)],
    "VisitID": df_visits["VisitID"],
    "BillingDate": df_visits["DischargeDate"] + pd.to_timedelta(
        np.random.randint(1, 10, NUM_VISITS), unit="D"
    ),
    "PaidAmount": (df_visits["TotalCost"] * np.random.uniform(0.7, 1.0, NUM_VISITS)).round(2),
    "PaymentMode": np.random.choice(["Insurance", "Cash", "UPI", "Card"], NUM_VISITS)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_patients.to_csv("patients.csv", index=False)
df_doctors.to_csv("doctors.csv", index=False)
df_departments.to_csv("departments.csv", index=False)
df_visits.to_csv("patient_visits.csv", index=False)
df_billing.to_csv("billing.csv", index=False)

print("Fabric safe healthcare dataset generated successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

base_path = "/lakehouse/default/Files/bronze/healthcare"

os.makedirs(base_path, exist_ok=True)

print("Folder created in OneLake")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_path = "/lakehouse/default/Files/bronze/healthcare"

df_patients.to_csv(f"{base_path}/patients.csv", index=False)
df_doctors.to_csv(f"{base_path}/doctors.csv", index=False)
df_departments.to_csv(f"{base_path}/departments.csv", index=False)
df_visits.to_csv(f"{base_path}/patient_visits.csv", index=False)
df_billing.to_csv(f"{base_path}/billing.csv", index=False)

print("Files successfully saved to HealthcareLH → Files → bronze → healthcare")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
