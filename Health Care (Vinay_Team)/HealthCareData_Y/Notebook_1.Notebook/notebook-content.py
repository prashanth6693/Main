# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3e7fe138-74f4-40e0-bfbd-64f0027c39e6",
# META       "default_lakehouse_name": "BronzeLayer",
# META       "default_lakehouse_workspace_id": "0ef96a5d-6189-4a8b-a938-4fc18e107cf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "3e7fe138-74f4-40e0-bfbd-64f0027c39e6"
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

num_patients = 100000
num_visits = 250000
num_bills = 300000
num_doctors = 5000

hospitals = ["Apollo", "Fortis", "Manipal", "Aster", "Narayana"]
cities = ["Hyderabad", "Bangalore", "Chennai", "Mumbai", "Delhi"]
genders = ["Male", "Female", "M", "F", "Unknown", None]
insurance_types = ["Private", "Govt", "Corporate", None]
payment_status = ["Paid", "Pending", "Rejected", None]
specializations = ["Cardiology", "Neurology", "Orthopedic", "General", "ICU"]
diagnosis_list = ["Diabetes", "Heart Disease", "Asthma", "Cancer", "Flu", None]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

patients = []
for i in range(num_patients):
    patients.append({
        "patient_id": random.randint(10000, 50000),   # duplicates
        "patient_name": f"Patient_{random.randint(1,200000)}",
        "age": random.choice([random.randint(1, 90), None]),
        "gender": random.choice(genders),
        "city": random.choice(cities),
        "created_date": random.choice([
            datetime.today() - timedelta(days=random.randint(1, 2000)),
            (datetime.today() - timedelta(days=random.randint(1, 2000))).strftime("%d/%m/%Y")
        ])
    })

df_patients = pd.DataFrame(patients)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

doctors = []
for i in range(num_doctors):
    doctors.append({
        "doctor_id": i + 1,
        "doctor_name": f"Dr_{random.randint(1,20000)}",
        "specialization": random.choice(specializations),
        "hospital": random.choice(hospitals)
    })

df_doctors = pd.DataFrame(doctors)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

visits = []
for i in range(num_visits):
    admit = datetime.today() - timedelta(days=random.randint(1, 1000))
    discharge = admit + timedelta(days=random.randint(1, 15))

    visits.append({
        "visit_id": i + 1,
        "patient_id": random.randint(10000, 50000),
        "doctor_id": random.randint(1, num_doctors),
        "hospital": random.choice(hospitals),
        "diagnosis": random.choice(diagnosis_list),
        "admission_date": random.choice([
            admit.strftime("%Y-%m-%d"),
            admit.strftime("%d/%m/%Y"),
            admit
        ]),
        "discharge_date": discharge
    })

df_visits = pd.DataFrame(visits)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bills = []
for i in range(num_bills):
    bills.append({
        "bill_id": i + 1,
        "patient_id": random.randint(10000, 50000),
        "visit_id": random.randint(1, num_visits),
        "insurance_type": random.choice(insurance_types),
        "bill_amount": random.choice([
            round(random.uniform(500, 80000), 2),
            None,
            -500   # bad data
        ]),
        "payment_status": random.choice(payment_status)
    })

df_bills = pd.DataFrame(bills)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vitals = []
for i in range(150000):
    vitals.append({
        "patient_id": random.randint(10000, 50000),
        "heart_rate": random.randint(50, 150),
        "bp": random.randint(80, 180),
        "temperature": round(random.uniform(96, 104), 1),
        "recorded_time": datetime.now() - timedelta(minutes=random.randint(1, 500000))
    })

df_vitals = pd.DataFrame(vitals)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Correct Fabric Lakehouse path
base_path = "Files/bronze_healthcare/"

# Create folder if not exists
import os
os.makedirs("/lakehouse/default/" + base_path, exist_ok=True)

df_patients.to_csv("/lakehouse/default/" + base_path + "patients_raw.csv", index=False)
df_doctors.to_csv("/lakehouse/default/" + base_path + "doctors_raw.csv", index=False)
df_visits.to_csv("/lakehouse/default/" + base_path + "visits_raw.csv", index=False)
df_bills.to_csv("/lakehouse/default/" + base_path + "billing_raw.csv", index=False)
df_vitals.to_csv("/lakehouse/default/" + base_path + "vitals_raw.csv", index=False)

print("Saved successfully to Bronze Lakehouse")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
