import pandas as pd
import random
from faker import Faker
import boto3
from io import StringIO

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'
folder_path = 'finance/revenue_summary/revenue_summary.csv'

fake = Faker()

# Generating revenue summary data
revenue_summary = []
store_ids = ['ST001', 'ST002', 'ST003','ST004','ST005']
for i in range(100):  # Number of revenue entries
    revenue_id = f"R{str(i+1).zfill(3)}"
    store_id = random.choice(store_ids)
    total_revenue = round(random.uniform(1000, 5000), 2)
    revenue_date = fake.date_this_year()
    region = random.choice(['North', 'South', 'East', 'West'])
    revenue_summary.append([revenue_id, store_id, total_revenue, revenue_date, region])

# Save to CSV in memory
df_revenue = pd.DataFrame(revenue_summary, columns=['revenue_id', 'store_id', 'total_revenue', 'revenue_date', 'region'])
csv_buffer = StringIO()
df_revenue.to_csv(csv_buffer, index=False)

# Upload to S3
s3_client.put_object(Bucket=bucket_name, Key=folder_path, Body=csv_buffer.getvalue())

print(f"revenue_summary.csv uploaded to {bucket_name}/{folder_path}.")
