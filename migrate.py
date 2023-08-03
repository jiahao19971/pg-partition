from dotenv import load_dotenv
import datetime, os, subprocess
import json, os
from db.db import _get_db
from sshtunnel import SSHTunnelForwarder
from tunnel.tunnel import _get_tunnel
from common.common import PartitionCommon
from common.wrapper import get_config_n_secret, background
import boto3, botocore, multiprocessing
from common.query import (
    get_order_by_limit_1,
    table_check
)
from functools import partial
    
load_dotenv()



def invoked_client(key, database_config):
    cfg = botocore.config.Config(read_timeout=900, connect_timeout=900)
    aws_config = database_config['aws']

    lambda_access_key = aws_config['lambda_aws_access_key']
    lambda_secret_access_key = aws_config['lambda_aws_secret_access_key']

    lambda_session = boto3.Session(
            aws_access_key_id=lambda_access_key,
            aws_secret_access_key=lambda_secret_access_key,
            region_name=aws_config['region'] if "region" in aws_config else "ap-southeast-1"
    )

    bucket_name = aws_config['bucket_name']
    access_key = aws_config['aws_access_key']
    aws_secret_access_key = aws_config['aws_secret_access_key']

    inputParams = {
        "key": key,
        "bucket_name": bucket_name,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": aws_secret_access_key
    }

    lambda_client = lambda_session.client('lambda', config=cfg)

    response = lambda_client.invoke(
        FunctionName = aws_config['lambda_arn'],
        InvocationType = 'RequestResponse',
        Payload = json.dumps(inputParams)
    )
    
    responseFromChild = json.load(response['Payload'])
    return int(responseFromChild)

class MigratePartition(PartitionCommon):
    def __init__(self) -> None:
        super().__init__()
        self.logger = self.logging_func("PG_Migrate")

    def _get_aws_secret(self, database_config):
        if "aws" in database_config:
            aws_config = database_config['aws']
            try:
                ## Set Local Variable
                access_key = aws_config['aws_access_key']
                secret_access_key = aws_config['aws_secret_access_key']

                ## Set Global Variable in secrets
                self.region = aws_config['region'] if "region" in aws_config else "ap-southeast-1"
                self.bucket_name = aws_config['bucket_name']
                self.lambda_arn = aws_config['lambda_arn']
                
                self.session = boto3.Session(
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_access_key,
                            region_name=self.region
                        )
                
                ## Set Global AWS Variable in secrets
                self.s3_client = self.session.client('s3')
            except Exception as e:
                self.logger.error(e)
                raise Exception(e)
        else:
            raise Exception("AWS credentials not found")

    def check_table_partition(self, table, cur):
        checker = table_check.format(a=table['name'], b=table['schema'])
        cur.execute(checker)
        data = cur.fetchall()

        if "partitioned table" in list(data[0]):
            return True
        else:
            return False 

    def get_count_from_s3(self, path, database_config):
        self.logger.debug(f"Getting count for path: {path}")
        data = self.s3_client.list_objects(
            Bucket=self.bucket_name,
            Prefix=path
        )

        get_key = [x['Key'] for x in data['Contents'] if ".sql" not in x['Key'] and path != x['Key']]

        process = 10
        
        self.logger.info(f"Initializing multiprocessing with {process} processors")
        pool = multiprocessing.Pool(processes = process)

        counting = pool.map(partial(invoked_client, database_config=database_config), get_key)
        
        data_count = sum(counting)

        return data_count
    
    def _create_db_url(self, new_conn, database_config):
        db_name = new_conn["dbname"]
        db_user = new_conn["user"]
        db_host = new_conn["host"]
        db_port = new_conn["port"]

        users = f"{db_user}"
        if "password" in new_conn:
            users = f"{db_user}:{database_config['db_password']}"

        db_url = "postgres://{a}@{b}:{c}/{d}".format(
            a=users,
            b=db_host,
            c=db_port,
            d=db_name
        )
        
        ssl = ""
        if "sslmode" in new_conn and "sslrootcert" in new_conn:
            ssl = "?sslrootcert={a}&sslcert={b}&sslkey={c}&sslmode={d}".format(
                    a=new_conn["sslrootcert"],
                    b=new_conn["sslcert"],
                    c=new_conn["sslkey"],
                    d=new_conn["sslmode"]
                    )
        elif "sslmode" in new_conn and "sslrootcert" not in new_conn:
            ssl = "?sslmode={a}".format(a=new_conn["sslmode"])

        db_url = db_url + ssl

        return db_url, db_name

    def migrate_ddl_from_table_to_s3(
            self, 
            tables, 
            table_sql, 
            file_name, 
            bucket_name, 
            path, 
            db_url
        ):

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
        self.logger.info("Running pgdump to backup DDL")
        process.communicate()[0]
        if process.returncode != 0:
            self.logger.error('Command failed. Return code : {}'.format(process.returncode))
            exit(1)

        self.s3_client.upload_file(table_sql, bucket_name, f"{path}/ddl/{file_name}.sql")
        self.logger.info(f"Uploded: {table_sql}")
        os.remove(table_sql)
        self.logger.info(f"Removed from local directory: {table_sql}")

    def migrate_run(self, table, database_config, application_name):
        db_identifier = database_config['db_identifier']
        logger = self.logging_func(application_name=application_name)

        server = _get_tunnel(database_config)
        db_conn = _get_db(server, database_config, application_name)
        logger.debug(f"Connected: {db_identifier}")

        conn = db_conn.connect()
        
        cur = conn.cursor()
        try:
            rds_client = self.session.client('rds')

            db_identifier = database_config['db_identifier']

            rds_instance = rds_client.describe_db_instances(
                            DBInstanceIdentifier=db_identifier
                        )
            s3_error = "S3 Export not enabled"
            if len(rds_instance['DBInstances'][0]['AssociatedRoles']) == 0:
                raise Exception(s3_error)

            s3Enable = [True for x in rds_instance['DBInstances'][0]['AssociatedRoles'] if x['FeatureName'] == 's3Export' and x['Status'] == "ACTIVE"][0]
            
            if s3Enable is False:
                raise Exception(s3_error)
            
            split_string_conn = conn.dsn.split(" ")

            new_conn = {}
            for conn_part in split_string_conn:
                splitter = conn_part.split("=")
                new_conn[splitter[0]] = splitter[1]


            db_url, db_name = self._create_db_url(new_conn, database_config)
                
            qry = f"Set search_path to '{table['schema']}'"
            self.logger.info(qry)
            cur.execute(f"{qry};")
            
            partitioning = self.check_table_partition(table, cur)

            if partitioning:
                cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")

                today = datetime.date.today()

                current_year = today.year

                archive_year = current_year - table['interval'] - 1

                min = get_order_by_limit_1.format(
                    a=table['partition'], 
                    b=table['name'], 
                    c=table['pkey'], 
                    d="ASC"
                )

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
                        
                        path = f"{db_name}/{table['schema']}/{table['name']}/{new_year}" 
                        file = f"{path}/{file_name}"

                        logger.info(f"Migrating data from table {new_year} to s3 {self.bucket_name}")

                        migrate_data = f"""
                            SELECT *
                                FROM aws_s3.query_export_to_s3(
                                'SELECT * FROM "{table['schema']}".{table['name']}_{new_year}',
                                aws_commons.create_s3_uri(
                                '{self.bucket_name}',
                                '{file}',
                                '{self.region}'
                                ), 
                            options :='format csv, HEADER true, ENCODING UTF8'
                            );
                        """

                        cur.execute(migrate_data)
                        logger.info(f"Data migrated to s3 for year: {new_year}")

                        tables = f'"{table["schema"]}".{table["name"]}_{new_year}'

                        table_sql = '{a}_{b}_{c}.sql'.format(a=table["schema"], b=table["name"], c=new_year)

                        try:
                            self.migrate_ddl_from_table_to_s3(tables, table_sql, file_name, self.bucket_name, path, db_url)
                        except Exception as exp:
                            raise Exception(exp)
                        finally:
                            count_tb = self.get_count_from_s3(path, database_config)
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
        except Exception as e:
            conn.rollback()
            self.logger.error(e)
            self.logger.error("Error occured while partitioning, rolling back")
        finally:
            if type(server) == SSHTunnelForwarder:
                server.stop()
            conn.close()

    @get_config_n_secret
    def main(self, table, database_config, application_name):
        self._get_aws_secret(database_config)
        self.migrate_run(table, database_config, application_name)

if __name__ == "__main__":
    migrate = MigratePartition().main()