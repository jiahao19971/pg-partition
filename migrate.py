from dotenv import load_dotenv
import datetime, os, subprocess, time
import json, os
from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
import boto3, botocore, multiprocessing
from common.query import (
    get_order_by_limit_1,
    table_check
)
    
load_dotenv()

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
                
                cfg = botocore.config.Config(read_timeout=900, connect_timeout=900)
                session = boto3.Session(
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_access_key,
                            region_name=self.region
                        )
                
                ## Set Global AWS Variable in secrets
                self.s3_client = session.client('s3')
                self.lambda_client = session.client('lambda', config=cfg)
                self.inputParams = {
                    "key": "",
                    "bucket_name": self.bucket_name,
                    "aws_access_key_id": access_key,
                    "aws_secret_access_key": secret_access_key
                }
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

    def invoked_client(self, key):
        tic = time.perf_counter()
        self.inputParams['key'] = key

        self.logger.info(f"Invoking lambda function: {key}")
                
        response = self.lambda_client.invoke(
            FunctionName = self.lambda_arn,
            InvocationType = 'RequestResponse',
            Payload = json.dumps(self.inputParams)
        )
            
        responseFromChild = json.load(response['Payload'])

        toc = time.perf_counter()
        self.logger.debug(f"Lambda completed in {toc - tic:0.4f} seconds")
        return int(responseFromChild)

    def get_count_from_s3(self, path):
        self.logger.debug(f"Getting count for path: {path}")
        tic = time.perf_counter()
        data = self.s3_client.list_objects(
            Bucket=self.bucket_name,
            Prefix=path
        )

        get_key = [x['Key'] for x in data['Contents'] if ".sql" not in x['Key'] and path != x['Key']]

        process = 10
        
        self.logger.info(f"Initializing multiprocessing with {process} processors")
        pool = multiprocessing.Pool(processes = process)

        data = pool.map(self.invoked_client, get_key)
        
        data_count = sum(data)

        toc = time.perf_counter()

        self.logger.debug(f"Count completed in {toc - tic:0.4f} seconds")

        return data_count
    
    def _create_db_url(self, new_conn):
        db_name = new_conn["dbname"]
        db_user = new_conn["user"]
        db_host = new_conn["host"]
        db_port = new_conn["port"]

        users = f"{db_user}"
        if "db_pass" in new_conn:
            users = f"{db_user}:{new_conn['db_pass']}"

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


    def migrate_run(self, conn, table, database_config):
        
        conn = conn.connect()
        cur = conn.cursor()

        try:
            self._get_aws_secret(database_config)

            split_string_conn = conn.dsn.split(" ")

            new_conn = {}
            for conn in split_string_conn:
                splitter = conn.split("=")
                new_conn[splitter[0]] = splitter[1]

            db_url, db_name = self._create_db_url(new_conn)
                
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
                    self.logger.info(f"Migrating for: {table['schema']}.{table['name']}")
                    for looper_year in range(0, create_loop_year + 1):
                        new_year = min_year + looper_year 

                        self.logger.debug("Counting the amount of rows the table have")
                        cur.execute(f"SELECT count(*) FROM {table['name']}_{new_year};")

                        count_table = cur.fetchall()
                        now = datetime.datetime.now()
                        file_name = f"{table['schema']}_{table['name']}_{new_year}_{now.strftime('%Y%m%d%H%M%S')}"
                        
                        path = f"{db_name}/{table['schema']}/{table['name']}/{new_year}" 
                        file = f"{path}/{file_name}"

                        self.logger.info(f"Migrating data from table {new_year} to s3 {self.bucket_name}")

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
                        self.logger.info(f"Data migrated to s3 for year: {new_year}")

                        tables = f'"{table["schema"]}".{table["name"]}_{new_year}'

                        table_sql = '{a}_{b}_{c}.sql'.format(a=table["schema"], b=table["name"], c=new_year)

                        try:
                            self.migrate_ddl_from_table_to_s3(tables, table_sql, file_name, self.bucket_name, path, db_url)
                        except Exception as exp:
                            self.logger.error(exp)
                            exit(1)
                        finally:
                            count_tb = self.get_count_from_s3(path)
                            if int(count_table[0][0]) == int(count_tb):
                                self.logger.info(f"Data migrated check successfully {new_year}")

                                cur.execute(f'DROP TABLE "{table["schema"]}".{table["name"]}_{new_year};')

                                self.logger.info("Removing table from the partition and database")
                            else:
                                self.logger.error(f"Data migrated check failed {new_year}")

                        conn.commit()
                else:
                    self.logger.info(f"No migration needed for: {table['schema']}.{table['name']}")
                    conn.commit()
                    conn.close()
            else:
                self.logger.info(f"No migration needed for: {table['schema']}.{table['name']}")
                conn.close()
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Error occured while partitioning, rolling back")
            conn.rollback()
            conn.close()
        
    @get_config_n_secret
    def main(self, 
            conn,
            table, 
            logger,
            database_config,
            server,
            application_name
        ):
        self.logger = logger
        self.migrate_run(conn, table, database_config)

if __name__ == "__main__":
    migrate = MigratePartition()
    migrate.main()