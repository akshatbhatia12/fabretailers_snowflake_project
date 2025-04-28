import json
import random
from faker import Faker
import boto3
import numpy as np

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'

# Folder paths
performance_folder = 'hr-department/employee_performance/'

fake = Faker()

# Function to generate Employee Performance data with duplicates, extremes, and nulls
def generate_employee_performance_data(file_index, num_records=100):
    employee_performances = []
    
    for i in range(num_records):
        employee_id = f"EMP{random.randint(1, 100)}"
        
        # Extreme values for performance score
        if random.random() < 0.05:  # 5% chance for extreme values
            performance_score = random.choice([100, -100])  # Extreme scores
        else:
            performance_score = random.randint(50, 95)
        
        # Null values
        evaluation_date = fake.date_this_year() if random.random() > 0.1 else None  # 10% chance of null
        review_comments = fake.text(max_nb_chars=150) if random.random() > 0.1 else None  # 10% chance of null

        # Duplicate values
        if random.random() < 0.05:  # 5% chance to duplicate employee_id or performance_score
            employee_id = f"EMP{random.randint(1, 10)}"  # Random duplication
            performance_score = random.choice([50, 75, 85])  # Random duplication of performance score

        employee_performances.append([employee_id, performance_score, evaluation_date, review_comments])

    # Create DataFrame
    performance_data = []
    for performance in employee_performances:
        performance_data.append({
            "employee_id": performance[0],
            "performance_score": performance[1],
            "evaluation_date": performance[2],
            "review_comments": performance[3]
        })
    
    # Generate a unique file name per file_index
    file_name = f"employee_performance_data_part_{file_index + 1}.json"
    
    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=performance_folder + file_name, Body=json.dumps(performance_data))
    print(f"{file_name} uploaded to {bucket_name}/{performance_folder}.")

# Generate multiple files
def generate_multiple_employee_performance_files(num_files=5, num_records_per_file=100):
    for file_index in range(num_files):
        generate_employee_performance_data(file_index, num_records=num_records_per_file)

# Call function to generate 5 files with 100 records in each
generate_multiple_employee_performance_files(num_files=5, num_records_per_file=100)

