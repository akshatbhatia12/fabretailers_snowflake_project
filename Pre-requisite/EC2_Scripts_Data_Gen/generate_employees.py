import pandas as pd
import random
from faker import Faker
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'

# Folder paths
hr_folder = 'hr_department/employees/'

fake = Faker()

# Function to generate employee data for a specified number of employees
def generate_employee_data(file_index, num_employees=100):
    employees = []
    # Generating employee data
    for i in range(num_employees):
        employee_id = f"E{str(i + (file_index * num_employees)).zfill(5)}"  # Ensure unique ID across files
        first_name = fake.first_name()
        last_name = fake.last_name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=65)
        email = fake.email()
        phone = fake.phone_number()
        hire_date = fake.date_this_decade()
        department = random.choice(['Sales', 'Marketing', 'Finance', 'IT', 'HR'])
        salary = round(random.uniform(30000, 120000), 2)

        employees.append([employee_id, first_name, last_name, dob, email, phone, hire_date, department, salary])

    # Create DataFrame
    df = pd.DataFrame(employees, columns=['employee_id', 'first_name', 'last_name', 'dob', 'email', 'phone', 'hire_date', 'department', 'salary'])
    
    # Convert the DataFrame to Parquet
    table = pa.Table.from_pandas(df)

    # Create in-memory Parquet file using BytesIO
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)
    
    # Generate a unique file name per file_index
    file_name = f"employee_data_part_{file_index + 1}.parquet"
    
    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=hr_folder + file_name, Body=parquet_buffer.getvalue())
    print(f"{file_name} uploaded to {bucket_name}/{hr_folder}.")

# Generate multiple files
def generate_multiple_employee_files(num_files=5, num_employees_per_file=100):
    for file_index in range(num_files):
        generate_employee_data(file_index, num_employees=num_employees_per_file)

# Call function to generate 5 files with 100 employees in each
generate_multiple_employee_files(num_files=5, num_employees_per_file=100)
