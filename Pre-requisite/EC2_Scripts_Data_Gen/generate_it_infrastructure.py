import pandas as pd
import random
from faker import Faker
import boto3
from io import StringIO
import datetime

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'

# Folder paths
it_folder = 'it-department/it_infrastructure/'

fake = Faker()

# Function to generate IT infrastructure data
def generate_it_infrastructure_data(file_index, num_devices=100):
    devices = []
    
    device_types = ['Laptop', 'Desktop', 'Server', 'Router', 'Switch']
    operating_systems = ['Windows', 'Linux', 'Mac', 'Ubuntu']
    device_statuses = ['Active', 'Inactive', 'Maintenance']
    
    for i in range(num_devices):
        device_id = f"D{str(i + (file_index * num_devices)).zfill(6)}"  # Ensure unique device ID across files
        department = random.choice(['Sales', 'IT', 'Finance', 'HR'])
        device_type = random.choice(device_types)
        operating_system = random.choice(operating_systems)
        device_status = random.choice(device_statuses)
        purchase_date = fake.date_this_decade()
        last_service_date = fake.date_this_year()
        location = fake.city()
        
        devices.append([device_id, department, device_type, operating_system, device_status, purchase_date, last_service_date, location])

    # Create DataFrame
    df = pd.DataFrame(devices, columns=['device_id', 'department', 'device_type', 'operating_system', 'device_status', 'purchase_date', 'last_service_date', 'location'])
    
    # Create in-memory CSV file using StringIO
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Generate a unique file name per file_index
    file_name = f"it_infrastructure_data_part_{file_index + 1}.csv"
    
    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=it_folder + file_name, Body=csv_buffer.getvalue())
    print(f"{file_name} uploaded to {bucket_name}/{it_folder}.")

# Generate multiple files
def generate_multiple_it_infrastructure_files(num_files=1, num_devices_per_file=100):
    for file_index in range(num_files):
        generate_it_infrastructure_data(file_index, num_devices=num_devices_per_file)

# Call function to generate 5 files with 100 devices in each
generate_multiple_it_infrastructure_files(num_files=1, num_devices_per_file=100)
