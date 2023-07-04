import boto3
import pandas as pd

def lambda_handler(event, context):
    bucket_name = event['bucket_name']
    access_key  = event['aws_access_key_id']
    secret_key  = event['aws_secret_access_key']
    key         = event['key']
    
    session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        
    s3_client = session.client('s3')
    
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)

    if "_part" in key:
        chunk_df = pd.read_csv(obj['Body'], chunksize=1000000, encoding='utf-8', header=None)
    else:
        chunk_df = pd.read_csv(obj['Body'], chunksize=1000000, encoding='utf-8')

    pd_df = pd.concat(chunk_df)

    return int(len(pd_df))
    