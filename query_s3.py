import boto3, os
from dotenv import load_dotenv

load_dotenv()


s3_client = boto3.client('s3')

file = "kfit_app_staging/singapore/versions/2017/singapore_versions_2017_20230614200040.csv"

data = s3_client.select_object_content(
    Bucket=os.environ['BUCKET_NAME'],
    Key=file,
    Expression="select * from s3object WHERE created_at = '2017-06-21 04:51:11.557596'",
    ExpressionType='SQL',
    InputSerialization={
        'CSV': {
            'FileHeaderInfo': 'USE',
            'AllowQuotedRecordDelimiter': True
        },
        'CompressionType': 'NONE',
    },
    OutputSerialization={
        'CSV': {}
    },
)

for event in data['Payload']:
    if records := event.get('Records'):
        field = records['Payload']
        field = field.decode("utf-8")

        print(field)