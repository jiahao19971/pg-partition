from dotenv import load_dotenv
import datetime, os, subprocess, time
from db.db import DBLoader
from common.common import _open_config, logs, background
import boto3
from tunnel.tunnel import Tunneler
from botocore.exceptions import ClientError
from common.query import (
    get_order_by_limit_1,
    table_check
)
import os
    
load_dotenv()

db_name = os.environ['DATABASE']
logger = logs("PG_Migrate")

def check_table_partition(table, cur):
    checker = table_check.format(a=table['name'], b=table['schema'])
    cur.execute(checker)
    data = cur.fetchall()

    if "partitioned table" in list(data[0]):
        return True
    else:
        return False

@background
def migrate_run(server, table):
    application_name = f"{table['schema']}.{table['name']}"

    conn = DBLoader(server, db_name, application_name=application_name)

    conn = conn.connect()
    cur = conn.cursor()
    logger = logs(application_name)

    qry = f"Set search_path to '{table['schema']}'"
    logger.info(qry)
    cur.execute(f"{qry};")

    partitioning = check_table_partition(table, cur)

    if partitioning:

        cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")

        today = datetime.date.today()

        current_year = today.year

        archive_year = current_year - table['interval'] - 1

        min = get_order_by_limit_1.format(a=table['partition'], b=table['name'], c=table['pkey'], d="ASC")

        cur.execute(min)

        data = cur.fetchall()

        min_year = data[0][0].year

        create_loop_year = archive_year - min_year

        if create_loop_year > 0:
            logger.info(f"Migrating for: {table['schema']}.{table['name']}")
            for looper_year in range(0, create_loop_year + 1):
                new_year = min_year + looper_year 

                cur.execute(f"SELECT count(*) FROM {table['name']}_{new_year};")

                count_table = cur.fetchall()
                now = datetime.datetime.now()
                file_name = f"{table['schema']}_{table['name']}_{new_year}_{now.strftime('%Y%m%d%H%M%S')}"
                regions = "ap-southeast-1"
                bucket = os.environ['BUCKET_NAME']
                
                path = f"{db_name}/{table['schema']}/{table['name']}/{new_year}" 
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
                logger.info("data migrated to s3 for year: ", new_year)

                session = boto3.Session(
                            aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                        )

                s3_client = session.client('s3')

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
                logger.info("S3 Get Content completed")

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
                    logger.info("Running pgdump to backup DDL")
                    process.communicate()[0]
                    if process.returncode != 0:
                        logger.error('Command failed. Return code : {}'.format(process.returncode))
                        exit(1)

                    try:
                        s3_client.upload_file(table_sql, bucket, f"{path}/ddl/{file_name}.sql")
                        logger.info(f"Uploded: {table_sql}")
                        os.remove(table_sql)
                        logger.info(f"Removed from local directory: {table_sql}")
                    except ClientError as exp:
                        logger.error(exp)
                        exit(1)
                    finally:
                        for event in data['Payload']:
                            if records := event.get('Records'):
                                field = records['Payload']
                                field = field.decode("utf-8").replace("\n", "")

                                if int(field) == int(count_table[0][0]):
                                    logger.info(f"Data migrated check successfully {new_year}")

                                    cur.execute(f'DROP TABLE "{table["schema"]}".{table["name"]}_{new_year};')

                                    logger.info("Removing table from the partition and database")

                except Exception as e:
                    logger.error(e)
                    exit(1)

                conn.commit()
        else:
            logger.info(f"No migration needed for: {table['schema']}.{table['name']}")
    else:
        logger.info(f"No migration needed for: {table['schema']}.{table['name']}")
        
    conn.commit()
    conn.close()

def main():
    DB_HOST=os.environ['DB_HOST']
    try:
        server = Tunneler(DB_HOST, 5432)

        server = server.connect()

        server.start()
    except:
        server = {
            'local_bind_host': DB_HOST,
            'local_bind_port': 5432,
        }    

    config = _open_config("config.yaml")

    for table in config['table']:
        tic = time.perf_counter()
        migrate_run(server, table)
        toc = time.perf_counter()
        logger.debug(f"Script completed in {toc - tic:0.4f} seconds")
    

if __name__ == "__main__":
    main()
