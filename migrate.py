from dotenv import load_dotenv
import datetime, os, subprocess, time
from db.db import DBLoader
import json, os
from common.common import _open_config, logs
import boto3, botocore, multiprocessing
from tunnel.tunnel import Tunneler
from common.query import (
    get_order_by_limit_1,
    table_check
)
    
load_dotenv()

access_key = os.environ['AWS_ACCESS_KEY']
secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
regions = os.environ['REGION']
lambda_arn = os.environ['LAMBDA_ARN']
bucket_name = os.environ['BUCKET_NAME']
DB_PASSWORD = os.environ['PASSWORD']
DB_NAME = os.environ['DATABASE']
DB_USERNAME = os.environ['USERNAME']
DB_SSELROOTCERT = os.environ['DB_SSLROOTCERT']
DB_SSLCERT = os.environ['DB_SSLCERT']
DB_SSLKEY = os.environ['DB_SSLKEY']
DB_SSLMODE = os.environ['DB_SSLMODE']
DB_HOST=os.environ['DB_HOST']

logger = logs("PG_Migrate")

cfg = botocore.config.Config(read_timeout=900, connect_timeout=900)

stag_session = boto3.Session(
            aws_access_key_id=os.environ['STAG_ACCESS_KEY'],
            aws_secret_access_key=os.environ['STAG_SECRET_ACCESS_KEY'],
        )

session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
        )

s3_client = session.client('s3')
lambda_client = stag_session.client('lambda', config=cfg)

def check_table_partition(table, cur):
    checker = table_check.format(a=table['name'], b=table['schema'])
    cur.execute(checker)
    data = cur.fetchall()

    if "partitioned table" in list(data[0]):
        return True
    else:
        return False 

inputParams = {
  "key": "",
  "bucket_name": bucket_name,
  "aws_access_key_id": access_key,
  "aws_secret_access_key": secret_access_key
}

def invoked_client(key):
    tic = time.perf_counter()
    inputParams['key'] = key

    logger.info(f"Invoking lambda function: {key}")
            
    response = lambda_client.invoke(
        FunctionName = lambda_arn,
        InvocationType = 'RequestResponse',
        Payload = json.dumps(inputParams)
    )
         
    responseFromChild = json.load(response['Payload'])

    toc = time.perf_counter()
    logger.debug(f"Lambda completed in {toc - tic:0.4f} seconds")
    return int(responseFromChild)

def get_count_from_s3(path):
    logger.debug(f"Getting count for path: {path}")
    tic = time.perf_counter()
    data = s3_client.list_objects(
        Bucket=bucket_name,
        Prefix=path
    )

    get_key = [x['Key'] for x in data['Contents'] if ".sql" not in x['Key'] and path != x['Key']]

    process = 10
    
    logger.info(f"Initializing multiprocessing with {process} processors")
    pool = multiprocessing.Pool(processes = process)

    data = pool.map(invoked_client, get_key)
    
    data_count = sum(data)

    toc = time.perf_counter()

    logger.debug(f"Count completed in {toc - tic:0.4f} seconds")

    return data_count

def migrate_ddl_from_table_to_s3(tables, table_sql, file_name, bucket_name, path, host, port):
    if DB_PASSWORD == "":
        db_url = f"postgres://{DB_USERNAME}@{host}:{port}/{DB_NAME}?sslrootcert={DB_SSELROOTCERT}&sslcert={DB_SSLCERT}&sslkey={DB_SSLKEY}&sslmode={DB_SSLMODE}"
    else:
        db_url = f"postgres://{DB_USERNAME}:{DB_PASSWORD}@{host}:{port}/{DB_NAME}"

    db = f"--dbname={db_url}"

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

    s3_client.upload_file(table_sql, bucket_name, f"{path}/ddl/{file_name}.sql")
    logger.info(f"Uploded: {table_sql}")
    os.remove(table_sql)
    logger.info(f"Removed from local directory: {table_sql}")

# @background
def migrate_run(server, table):
    application_name = f"{table['schema']}.{table['name']}"

    conn = DBLoader(server, DB_NAME, application_name=application_name)

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

        if create_loop_year >= 0:
            logger.info(f"Migrating for: {table['schema']}.{table['name']}")
            for looper_year in range(0, create_loop_year + 1):
                new_year = min_year + looper_year 

                logger.debug("Counting the amount of rows the table have")
                cur.execute(f"SELECT count(*) FROM {table['name']}_{new_year};")

                count_table = cur.fetchall()
                now = datetime.datetime.now()
                file_name = f"{table['schema']}_{table['name']}_{new_year}_{now.strftime('%Y%m%d%H%M%S')}"
                
                path = f"{DB_NAME}/{table['schema']}/{table['name']}/{new_year}" 
                file = f"{path}/{file_name}"

                logger.info(f"Migrating data from table {new_year} to s3 {bucket_name}")

                migrate_data = f"""
                    SELECT *
                        FROM aws_s3.query_export_to_s3(
                        'SELECT * FROM "{table['schema']}".{table['name']}_{new_year}',
                        aws_commons.create_s3_uri(
                        '{bucket_name}',
                        '{file}',
                        '{regions}'
                        ), 
                    options :='format csv, HEADER true, ENCODING UTF8'
                    );
                """

                cur.execute(migrate_data)
                logger.info(f"Data migrated to s3 for year: {new_year}")

                tables = f'"{table["schema"]}".{table["name"]}_{new_year}'

                table_sql = '{a}_{b}_{c}.sql'.format(a=table["schema"], b=table["name"], c=new_year)

                try:
                    host = server['local_bind_host']
                    port = server['local_bind_port']
                except: 
                    host = server.local_bind_host
                    port = server.local_bind_port

                try:
                    migrate_ddl_from_table_to_s3(tables, table_sql, file_name, bucket_name, path, host, port)
                except Exception as exp:
                    logger.error(exp)
                    exit(1)
                finally:
                    count_tb = get_count_from_s3(path)
                    if int(count_table[0][0]) == int(count_tb):
                        logger.info(f"Data migrated check successfully {new_year}")

                        cur.execute(f'DROP TABLE "{table["schema"]}".{table["name"]}_{new_year};')

                        logger.info("Removing table from the partition and database")
                    else:
                        logger.error(f"Data migrated check failed {new_year}")

                conn.commit()
        else:
            logger.info(f"No migration needed for: {table['schema']}.{table['name']}")
    else:
        logger.info(f"No migration needed for: {table['schema']}.{table['name']}")

    conn.commit()
    conn.close()

def main():
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
