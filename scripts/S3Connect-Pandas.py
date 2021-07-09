import os
import boto3
import io
import pandas as pd
from pprint import pprint

aws_access_key_id = os.getenv('HappinessProjectAccessKeyId')
aws_secret_access_key = os.getenv('HappinessProjectAccessKey')

s3 = boto3.client(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

response = s3.list_objects(
    Bucket='happiness-project-data',
)

def getKey(obj):
    return obj['Key']

def filterCsv(obj):
    return ".csv" in obj


keys = map(getKey, response['Contents'])
keys_filtered = filter(filterCsv, keys)

data = []
for key in keys_filtered:
    response = s3.get_object(Bucket='happiness-project-data', Key=key)
    s3_data = io.BytesIO(response.get('Body').read())
    try:
        df = pd.read_csv(s3_data, encoding='utf-8-sig')
    except UnicodeDecodeError:
        df = pd.read_csv(s3_data, encoding='latin-1')
    data.append([key,df])

#    print(df.head(10))