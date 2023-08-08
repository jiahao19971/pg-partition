"""
  This function is used to deploy to AWS Lambda.
  The purpose of this package is to allow concurrent
  count of all the part files in S3 bucket when postgres
  aws s3 export extension copy the data to s3.

  The reason why we need to count the part files is to
  ensure that all the data is copied to s3 before we
  remove it from the database.

  Args:
    event: dict[
      bucket_name: str <s3 bucket name>
      aws_access_key_id: str <
          aws iam user access
          key with access to the s3 bucket
        >
      aws_secret_access_key: str <
        aws iam user secret access key with
        access to the s3 bucket
        >
      key: str <path to the file in s3>
    ]

  Returns:
    int <number of rows in the file>
"""
import boto3
import pandas as pd


def lambda_handler(event, context):
  bucket_name = event["bucket_name"]
  access_key = event["aws_access_key_id"]
  secret_key = event["aws_secret_access_key"]
  key = event["key"]

  session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
  )

  s3_client = session.client("s3")

  obj = s3_client.get_object(Bucket=bucket_name, Key=key)

  if "_part" in key:
    chunk_df = pd.read_csv(
      obj["Body"], chunksize=1000000, encoding="utf-8", header=None
    )
  else:
    chunk_df = pd.read_csv(obj["Body"], chunksize=1000000, encoding="utf-8")

  pd_df = pd.concat(chunk_df)

  return int(len(pd_df))
