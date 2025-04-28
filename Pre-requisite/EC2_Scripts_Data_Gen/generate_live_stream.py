import json
import random
from datetime import datetime
import boto3
import time

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'

# Folder paths
it_folder = 'it-department/live-stream-data/'

# Devices and events data
device_ids = [f"D{str(i+1).zfill(6)}" for i in range(100)]  # Example device IDs
event_types = ['Login', 'Error', 'Device Check', 'Maintenance', 'Shutdown']

# Simulate live event data generation
def generate_event_data(event_id):
    device_id = random.choice(device_ids)
    event_type = random.choice(event_types)
    event_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    event_details = f"{event_type} details for device {device_id}"
    
    event_data = {
        "event_id": event_id,
        "device_id": device_id,
        "event_type": event_type,
        "event_timestamp": event_timestamp,
        "event_details": event_details
    }
    
    return event_data

# Upload data to S3
def upload_event_data_to_s3(event_id):
    event_data = generate_event_data(event_id)
    file_name = f"event_data_{event_id}.json"
    
    # Convert dict to JSON string
    json_data = json.dumps(event_data)
    
    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=it_folder + file_name, Body=json_data)
    print(f"{file_name} uploaded to {bucket_name}/{it_folder}.")

# Simulate continuous data generation
def simulate_continuous_data(num_events=1000, interval_seconds=10):
    for event_id in range(1, num_events+1):
        upload_event_data_to_s3(event_id)
        time.sleep(interval_seconds)  # Simulate real-time interval

# Simulate 1000 events with 10-second intervals
simulate_continuous_data(num_events=1000, interval_seconds=10)
