import json
from faker import Faker
import random
import boto3

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'
folder_path = 'hr-department/employee_attendance/'

fake = Faker()

# Generating employee attendance data
employee_attendance = []
for i in range(100):  # Number of employees
    employee_id = f"E{str(i+1).zfill(3)}"
    for j in range(30):  # Number of days
        date = fake.date_this_month()
        status = random.choice(['Present', 'Absent', 'On Leave'])
        shift_time = random.choice(['Morning', 'Afternoon', 'Night'])
        employee_attendance.append({
            "employee_id": employee_id,
            "date": date,
            "status": status,
            "shift_time": shift_time
        })

# Upload to S3
s3_client.put_object(Bucket=bucket_name, Key=folder_path, Body=json.dumps(employee_attendance))

print(f"employee_attendance.json uploaded to {bucket_name}/{folder_path}.")
