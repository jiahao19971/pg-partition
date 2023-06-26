# PG Partition

## Current Partitioning only works on existing table and convert it to a Range Partition. 
## To use this Repository, make sure to install all the dependencies from requirements.txt
```
pip install - r requirements.txt
```

1. Create an .env file in the directory, either (.env.staging || .env.production)
Below show all the env options available: 
```diff
+ REMOTE_HOST can be added if you need to tunneling instead of SSL authentication
+ Include when required
#REMOTE_HOST=<Host name for Tunnelling>

+ REMOTE_PORT can be added if you need to tunneling instead of SSL authentication
+ Include when required
#REMOTE_PORT=<Port number for Tunnelling>

+ REMOTE_USERNAME can be added if you need to tunneling instead of SSL authentication
+ Include when required
#REMOTE_USERNAME=<Username for Tunnelling>

+ REMOTE_KEY can be added if you need to tunneling instead of SSL authentication
+ Include when required
+ If REMOTE_PASSWORD is used, please removed REMOTE_KEY
#REMOTE_KEY=<Root Key access for Tunnelling>

+ REMOTE_HOST can be added if you need to tunneling instead of SSL authentication
+ Include when required
+ If REMOTE_KEY is used, please removed REMOTE_PASSWORD
#REMOTE_PASSWORD=<Password for Tunnelling>

+ USERNAME refer to the username access to the database instance
+ If AWS is used, IAM user name can be insert here
+ Else Normal User to access to the database (postgres)
USERNAME=

+ PASSWORD refer to the password access to the database instance
PASSWORD=

+ DATABASE refer to the database name that you are connecting to 
DATABASE=

+ DB_HOST is the host name of the database instance, it can be IPv4 or DNS names 
DB_HOST=

+ DB_SSLMODE can be added if you need to use SSL authentication instead of tunneling
+ Include when required
# DB_SSLMODE=<SSL MODE IF required, include only if required>

+ DB_SSLROOTCERT can be added if you need to use SSL authentication instead of tunneling
+ Include when required
# DB_SSLROOTCERT=<ROOT CA location>

+ DB_SSLCERT can be added if you need to use SSL authentication instead of tunneling
+ Include when required
# DB_SSLCERT=<SSL Cert Location>

+ DB_SSLKEY can be added if you need to use SSL authentication instead of tunneling
+ Include when required
# DB_SSLKEY=<SSL Key Location>

+ AWS_ACCESS_KEY can be added if you want to run migrate.py
AWS_ACCESS_KEY=<AWS Access Key with access to s3 bucket mentioned>

+ AWS_SECRET_ACCESS_KEY can be added if you want to run migrate.py
AWS_SECRET_ACCESS_KEY=<AWS Secret Key with access to s3 bucket mentioned>

+ BUCKET_NAME can be added if you want to run migrate.py
BUCKET_NAME=<S3 bucket name>

+ LOGLEVEL can be added if you want to reduce the amount of logs
+ Params available is DEBUG | INFO | WARNING | ERROR
+ Default is set to DEBUG
# LOGLEVEL=DEBUG
```

2. Once we have the .env ready, we will need to create a `config.yaml`
- The main purpose of the config.yaml is to allow the script to known what table, and variables to perform the partitioning on

```diff
+ List of table you want to perform partition on
table:
+ Dict to fill in column information
 - column: 
+ The format of the column information
    <column name>: <column field for example see below>
+ Example of the format: 
    id: int NOT NULL DEFAULT nextval('public.test'::regclass)
+ How long do you want the data to be in your live db (n + 1) in years?
  interval: 3
+ What is the table name ?
  name: example
+ Which column are you planning to partition with ?
  partition: created_at
+ What is the primary key ?
  pkey: id
+ What is the schema of the db ?
  schema: test
+ Is there any additional index ? Will be added automatically if there is any, can be ignored
  additional_index_name: 
+ The format of the index information
    <index name>: <index field for example see below>
+ Example of the format: 
    index_test_type_id: USING  btree (item_type, item_id)
```

1. To run the script, run these export first:
```
export PATH=$PATH:./bin
export LANG='en_US.UTF-8'
export LC_ALL='en_US.UTF-8'
export PYTHONIOENCODING='UTF-8'
```

1. Once the export is done, 
```
staging python <*.py>

production python <*.py>
```

5. There are 3 main scripts to run
    a. partition.py - Used to perform partitioning
    b. migrate.py - Used to perform migration of partition data from db to s3 bucket and remove the partition
    c. reverse_partition.py - Used to reverse the partitioning

6. Other script to be used:
    a. check_blocker.py - Used to verified that during partitioning, no table are block

## To do
- Create a job schedular to check regularly on the max date and if the partition exist, if not then create a partition 