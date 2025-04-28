import pandas as pd
import random
from faker import Faker
import numpy as np
import boto3
from io import StringIO

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'

# Folder paths
expense_folder = 'finance/expenses/'

fake = Faker()

# Expense types for extreme values
expense_types = ['Travel', 'Office Supplies', 'Software', 'Employee Benefits', 'Other']

# Function to generate Expense data with duplicates, extremes, and nulls
def generate_expense_data(file_index, num_records=101):
    expenses = []
    
    for i in range(num_records):
        expense_id = f"EXP{str(i + (file_index * num_records)).zfill(6)}"
        employee_id = f"E00{random.randint(100, 500)}"
        expense_type = random.choice(expense_types)
        
        # Extreme values
        if random.random() < 0.05:  # 5% chance for extreme values
            amount = random.randint(50000, 200000)  # Extreme high values
        else:
            amount = random.randint(100, 5000)
        
        # Null values
        expense_date = fake.date_this_year() if random.random() > 0.1 else None  # 10% chance of null
        expense_description = fake.text(max_nb_chars=100) if random.random() > 0.1 else None  # 10% chance of null

        # Duplicate values
        if random.random() < 0.05:  # 5% chance to duplicate expense_id
            expense_id = f"EXP{str(random.randint(1, 10)).zfill(6)}"  # Random duplication

        expenses.append([expense_id, employee_id, expense_type, amount, expense_date, expense_description])

    # Create DataFrame
    df = pd.DataFrame(expenses, columns=['expense_id', 'employee_id', 'expense_type', 'amount', 'expense_date', 'expense_description'])
    
    # Create in-memory CSV file using StringIO
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Generate a unique file name per file_index
    file_name = f"expense_data_part_{file_index + 1}.csv"
    
    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=expense_folder + file_name, Body=csv_buffer.getvalue())
    print(f"{file_name} uploaded to {bucket_name}/{expense_folder}.")

# Generate multiple files
def generate_multiple_expense_files(num_files=5, num_records_per_file=100):
    for file_index in range(num_files):
        generate_expense_data(file_index, num_records=num_records_per_file)

# Call function to generate 5 files with 100 records in each
generate_multiple_expense_files(num_files=5, num_records_per_file=100)
