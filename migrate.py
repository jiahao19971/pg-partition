from dotenv import load_dotenv
import datetime, os, subprocess
from db.db import DBLoader
from common.common import _open_config
import boto3
from botocore.exceptions import ClientError
import os
    

load_dotenv()

def main():
    server = {
        'local_bind_host': os.environ['DB_HOST'],
        'local_bind_port': 5432,
    }
    conn = DBLoader(server, 'kfit_app_staging')
    conn = conn.connect()

    cur = conn.cursor()

    config = _open_config("config.yaml")

    for table in config['table']:
        print(f"Set Search path to {table['schema']}")
        cur.execute(f"SET search_path TO '{table['schema']}';")

        cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")

        today = datetime.date.today()

        current_year = today.year

        archive_year = current_year - table['interval'] - 1

        cur.execute(f"SELECT min({table['partition']}) FROM {table['name']};")

        data = cur.fetchall()

        min_year = data[0][0].year

        create_loop_year = archive_year - min_year

        if create_loop_year > 0:
            print(f"Migrating for: {table['schema']}.{table['name']}")
            for looper_year in range(0, create_loop_year + 1):
                new_year = min_year + looper_year 

                cur.execute(f"SELECT count(*) FROM {table['name']}_{new_year};")

                count_table = cur.fetchall()
                now = datetime.datetime.now()
                file_name = f"{table['schema']}_{table['name']}_{new_year}_{now.strftime('%Y%m%d%H%M%S')}"
                regions = "ap-southeast-1"
                bucket = os.environ['BUCKET_NAME']
                path = f"kfit_app_staging/{table['schema']}/{table['name']}/{new_year}" 
                file = f"{path}/{file_name}.csv"

                migrate_data = f"""
                    SELECT *
                        FROM aws_s3.query_export_to_s3(
                        'SELECT * FROM "{table['schema']}".{table['name']}_{new_year}',
                        aws_commons.create_s3_uri(
                        '{bucket}',
                        '{file}',
                        '{regions}'
                        ), 
                    options :='format csv, HEADER true'
                    );
                """

                cur.execute(migrate_data)
                print("data migrated to s3 for year: ", new_year)

                s3_client = boto3.client('s3')

                data = s3_client.select_object_content(
                    Bucket=bucket,
                    Key=file,
                    Expression='select count(*) from s3object',
                    ExpressionType='SQL',
                    InputSerialization={
                        'CSV': {
                            'FileHeaderInfo': 'IGNORE',
                            'AllowQuotedRecordDelimiter': True
                        },
                        'CompressionType': 'NONE',
                    },
                    OutputSerialization={
                        'CSV': {}
                    },
                )
                print("S3 Get Content completed")

                tables = f'"{table["schema"]}".{table["name"]}_{new_year}'

                table_sql = '{a}_{b}_{c}.sql'.format(a=table["schema"], b=table["name"], c=new_year)

                
                if os.environ['PASSWORD'] == "":
                    db_url = f"postgres://{os.environ['USERNAME']}@{os.environ['DB_HOST']}:5432/{os.environ['DATABASE']}?sslrootcert={os.environ['DB_SSLROOTCERT']}&sslcert={os.environ['DB_SSLCERT']}&sslkey={os.environ['DB_SSLKEY']}&sslmode={os.environ['DB_SSLMODE']}"
                else:
                    db_url = f"postgres://{os.environ['USERNAME']}:{os.environ['PASSWORD']}@{os.environ['DB_HOST']}:5432/{os.environ['DATABASE']}"

                db = f"--dbname={db_url}"
                try:
                    process = subprocess.Popen(
                        ['pg_dump',
                        db,
                        '-s', 
                        '-t',
                        tables,
                        '-f',
                        table_sql
                        ],
                        stdout=subprocess.PIPE
                    )
                    print("Running pgdump to backup DDL")
                    process.communicate()[0]
                    if process.returncode != 0:
                        print('Command failed. Return code : {}'.format(process.returncode))
                        exit(1)

                    try:
                        s3_client.upload_file(table_sql, bucket, f"{path}/ddl/{file_name}.sql")
                        print("Uploded: ", table_sql)
                        os.remove(table_sql)
                        print("Removed from local directory: ", table_sql)
                    except ClientError as exp:
                        print(exp)
                        exit(1)
                    finally:
                        for event in data['Payload']:
                            if records := event.get('Records'):
                                field = records['Payload']
                                field = field.decode("utf-8").replace("\n", "")

                                if int(field) == int(count_table[0][0]):
                                    print("Data migrated check successfully", new_year)

                                    cur.execute(f'DROP TABLE "{table["schema"]}".{table["name"]}_{new_year};')

                                    print("Removing table from the partition and database")

                except Exception as e:
                    print(e)
                    exit(1)

                conn.commit()
        else:
            print(f"No migration needed for: {table['schema']}.{table['name']}")
            
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
