import pandas as pd
import random
from faker import Faker
import boto3
from io import StringIO

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'snowflake-fabretailers-project'
folder_path = 'sales-marketing/marketing_campaigns/'

fake = Faker()

# Generating marketing campaigns data
marketing_campaigns = []
for i in range(50):  # Number of campaigns
    campaign_id = f"C{str(i+1).zfill(3)}"
    campaign_name = random.choice(['Spring Sale', 'Winter Promotion', 'Back to School', 'Black Friday Sale'])
    start_date = fake.date_this_year()
    end_date = fake.date_this_year()
    budget = random.randint(50000, 100000)
    status = random.choice(['Active', 'Completed', 'Planned'])
    store_ids = random.choice(['ST001', 'ST002', 'ST003','ST004','ST005'])
    marketing_campaigns.append([campaign_id, campaign_name, start_date, end_date, budget, status,store_ids])

# Save to CSV in memory
df_campaigns = pd.DataFrame(marketing_campaigns, columns=['campaign_id', 'campaign_name', 'start_date', 'end_date', 'budget', 'status','store_ids'])
csv_buffer = StringIO()
df_campaigns.to_csv(csv_buffer, index=False)

# Upload to S3
s3_client.put_object(Bucket=bucket_name, Key=folder_path, Body=csv_buffer.getvalue())

print(f"marketing_campaigns.csv uploaded to {bucket_name}/{folder_path}.")
