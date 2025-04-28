import json
from faker import Faker
import random
import boto3

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'
folder_path = 'sales-marketing/store_locations/store_locations.json'

fake = Faker()

# Generating store locations data
store_ids = ['ST001', 'ST002', 'ST003','ST004','ST005']
store_locations = []
for store_id in store_ids:
    store_locations.append({
        "store_id": store_id,
        "store_name": fake.company(),
        "city": fake.city(),
        "region": random.choice(['North', 'South', 'East', 'West']),
        "store_manager": fake.name(),
        "contact_info": fake.phone_number()
    })

# Upload to S3
s3_client.put_object(Bucket=bucket_name, Key=folder_path, Body=json.dumps(store_locations))

print(f"store_locations.json uploaded to {bucket_name}/{folder_path}.")
