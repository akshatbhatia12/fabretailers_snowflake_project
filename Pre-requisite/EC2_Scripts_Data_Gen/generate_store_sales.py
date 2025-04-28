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
sales_folder = 'sales-marketing/store_sales/'

fake = Faker()

# Function to generate sales data for a specified number of sales records
def generate_sales_data(file_index, num_sales=100):
    sales = []
    
    # Generating sales data
    store_ids = [f"ST{str(i+1).zfill(3)}" for i in range(5)]  # Example store IDs: ST001, ST002, etc.
    products = ['Laptop', 'Smartphone', 'Headphones', 'Tablet', 'Monitor']
    payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Online Payment', 'Gift Card']
    
    for i in range(num_sales):
        sale_id = f"S{str(i + (file_index * num_sales)).zfill(6)}"  # Ensure unique sale ID across files
        store_id = random.choice(store_ids)
        product = random.choice(products)
        quantity_sold = random.randint(1, 10)
        sale_price = round(random.uniform(100, 1500), 2)
        total_sale = round(quantity_sold * sale_price, 2)
        payment_method = random.choice(payment_methods)
        sale_date = fake.date_this_year()
        
        sales.append([sale_id, store_id, product, quantity_sold, sale_price, total_sale, payment_method, sale_date])

    # Create DataFrame
    df = pd.DataFrame(sales, columns=['sale_id', 'store_id', 'product', 'quantity_sold', 'sale_price', 'total_sale', 'payment_method', 'sale_date'])
    
    # Create in-memory CSV file using StringIO
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Generate a unique file name per file_index
    file_name = f"sales_data_part_{file_index + 1}.csv"
    
    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=sales_folder + file_name, Body=csv_buffer.getvalue())
    print(f"{file_name} uploaded to {bucket_name}/{sales_folder}.")

# Generate multiple files
def generate_multiple_sales_files(num_files=15, num_sales_per_file=100):
    for file_index in range(num_files):
        generate_sales_data(file_index, num_sales=num_sales_per_file)

# Call function to generate 5 files with 100 sales records in each
generate_multiple_sales_files(num_files=5, num_sales_per_file=100)

