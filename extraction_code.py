import requests
import pandas as pd
import boto3

#AWS credentials and S3 bucket details

bucket_name = 'web-analytics-input'
file_path = 'path/to/your/data.csv'


url = 'https://data.brla.gov/resource/n9u7-h9i7.csv'
data = pd.read_csv(url)
df = pd.DataFrame(data)

#print(df.head())
#df.to_csv('/Users/rd/coding_stuff/DBT/DBT_Project/api_data.csv')



# Initialize S3 client and put data to s3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
csv_buffer = df.to_csv(index=False).encode()
s3.put_object(Bucket=bucket_name, Key='analytics_input.csv', Body=csv_buffer)






