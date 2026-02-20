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

NUM_PATIENTS = 20000
NUM_DOCTORS = 500
NUM_VISITS = 100000

np.random.seed(42)
random.seed(42)

# -------------------------
# Departments
# -------------------------
df_departments = pd.DataFrame({
    "DepartmentID": [f"DEP{str(i).zfill(2)}" for i in range(1, 11)],
    "DepartmentName": [
        "Cardiology", "Orthopedics", "Neurology", "Oncology", "Pediatrics",
        "Emergency", "Dermatology", "ENT", "Urology", "Gastroenterology"
    ]
})

# -------------------------
# Doctors
# -------------------------
df_doctors = pd.DataFrame({
    "DoctorID": [f"D{str(i).zfill(4)}" for i in range(1, NUM_DOCTORS + 1)],
    "DoctorName": [f"Doctor_{i}" for i in range(1, NUM_DOCTORS + 1)],
    "Specialization": np.random.choice(df_departments["DepartmentName"], NUM_DOCTORS),
    "DepartmentID": np.random.choice(df_departments["DepartmentID"], NUM_DOCTORS)
})

# -------------------------
# Patients
# -------------------------
base_date = pd.Timestamp("1935-01-01")

insurance_options = ["Private", "Government", "SelfPay"]

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
        insurance_options,
        NUM_PATIENTS,
        p=[0.5, 0.3, 0.2]
    )
})

# -------------------------
# Introduce Insurance Errors
# -------------------------

# 5% NULL insurance
null_indices = np.random.choice(df_patients.index, int(0.05 * NUM_PATIENTS), replace=False)
df_patients.loc[null_indices, "InsuranceType"] = None

# 5% Mixed case / inconsistent values
mixed_indices = np.random.choice(df_patients.index, int(0.05 * NUM_PATIENTS), replace=False)
df_patients.loc[mixed_indices, "InsuranceType"] = df_patients.loc[mixed_indices, "InsuranceType"].apply(
    lambda x: str(x).lower() if pd.notnull(x) else x
)

# -------------------------
# Visits
# -------------------------
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
    "AdmissionType": np.random.choice(
        ["Emergency", "Planned", "emergency", "PLANNED"],
        NUM_VISITS
    ),
    "TotalCost": np.random.randint(5000, 250000, NUM_VISITS)
})

# -------------------------
# Introduce Invalid Discharge Dates (3%)
# -------------------------
invalid_indices = np.random.choice(df_visits.index, int(0.03 * NUM_VISITS), replace=False)
df_visits.loc[invalid_indices, "DischargeDate"] = df_visits.loc[invalid_indices, "VisitDate"] - timedelta(days=2)

# -------------------------
# Billing
# -------------------------
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

spark.sql("DROP TABLE IF EXISTS dbo.patients")
spark.sql("DROP TABLE IF EXISTS dbo.doctors")
spark.sql("DROP TABLE IF EXISTS dbo.departments")
spark.sql("DROP TABLE IF EXISTS dbo.patient_visits")
spark.sql("DROP TABLE IF EXISTS dbo.billing")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_patients.write.mode("overwrite").saveAsTable("dbo.patients")
spark_doctors.write.mode("overwrite").saveAsTable("dbo.doctors")
spark_departments.write.mode("overwrite").saveAsTable("dbo.departments")
spark_visits.write.mode("overwrite").saveAsTable("dbo.patient_visits")
spark_billing.write.mode("overwrite").saveAsTable("dbo.billing")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT COUNT(*) FROM dbo.patients").show()
spark.sql("SELECT COUNT(*) FROM dbo.patient_visits").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT 
    COUNT(*) AS total_patients,
    SUM(CASE WHEN InsuranceType IS NULL THEN 1 ELSE 0 END) AS null_insurance_count
FROM dbo.patients
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT InsuranceType, COUNT(*) AS cnt
FROM dbo.patients
GROUP BY InsuranceType
ORDER BY cnt DESC
""").show(50, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT COUNT(*) AS invalid_discharge_count
FROM dbo.patient_visits
WHERE DischargeDate < VisitDate
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT AdmissionType, COUNT(*) AS cnt
FROM dbo.patient_visits
GROUP BY AdmissionType
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT PatientID, COUNT(*) AS visit_count
FROM dbo.patient_visits
GROUP BY PatientID
HAVING COUNT(*) > 1
ORDER BY visit_count DESC
""").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
