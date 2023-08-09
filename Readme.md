# PG Partition
#### Current Partitioning only works on existing table and convert it to a Range Partition.

#### To use this Repository, make sure to install all the dependencies from requirements.txt
```
pip install - r requirements.txt
```

Current repo uses pre-commit as the linter:
To use pre-commit
```diff
+ Update all the dependencies to the latest version
pre-commit autoupdate

+ To install the pre-commit script for github
pre-commit install
```

Example Output when you `git commit`
```
check yaml...........................................(no files to check)Skipped
fix end of files.....................................(no files to check)Skipped
trim trailing whitespace.............................(no files to check)Skipped
seed isort known_third_party.............................................Passed
isort................................................(no files to check)Skipped
cblack...............................................(no files to check)Skipped
pylint...............................................(no files to check)Skipped
```


---

## 1. Create an .env file in the directory
Below show all the env options available:
```diff
+ ENV can be added to set the environment
+ Params available is staging
+ Change naming for config.yaml to config.staging.yaml
ENV=staging

+ DEPLOYMENT can be added to set migration.py run
+ Params available is kubernetes
+ Run check base on min_date of the table instead
+ of the event u put in
DEPLOYMENT=kubernetes

+ LOGLEVEL can be added if you want to reduce the amount of logs
+ Params available is DEBUG | INFO | WARNING | ERROR
+ Default is set to DEBUG
# LOGLEVEL=DEBUG
```

## 2. Once we have the `.env` ready, we will need to create a `config.yaml`
- The main purpose of the `config.yaml` is to allow the script to known what table, and variables to perform the partitioning on

```diff
+ List of table you want to perform partition on
table:
+ Dict to fill in column information
 - column:
+ The format of the column information
    <column name>: <column field for example see below>
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
```

### Finalize example: `config.yaml`
```
table:
- column:
    id: int NOT NULL DEFAULT nextval('public.payments_id_seq'::regclass)
    created_at: timestamp NOT NULL
    updated: timestamp NOT NULL DEFAULT now()
    amount: float
    status: varchar
  interval: 3
  name: payments
  partition: created_at
  pkey: id
  schema: public
  additional_index_name:
    index_test_type_id: USING  btree (item_type, item_id)
```

## 3. Create a `secret.yaml`
- The main purpose of the `secret.yaml` is to allow the script have the required credentials to perform the partitioning
```
database:
  - db_identifier: <database instance name (aws rds: follow the identifier name)>
    db_host: <database host>
    db_name: <database name>
    db_username: <database user>
    db_port: <database port in str> (optional)
    db_password: <database password> (optional, required if db_ssl does not exist)
    remote_host: <tunnel host> (optional)
    remote_port: <tunnel port in str> (optional)
    remote_username: <tunnel username> (optional)
    remote_key: <tunnel public key> (optional, required if remote_password does not exist)
    remote_password: <tunnel password> (optional, required if remote key does not exist)
    db_ssl: (optional, required if db_password does not exist)
      db_sslmode: <database ssl mode> (required)
      db_sslrootcert: <database root certificate> (optional)
      db_sslcert: <database certificate> (optional)
      db_sslkey: <database ssl key> (optional)
    aws: (optional, required for migrate.py only)
      region: <which aws region is it ?> (optional)
      bucket_name: <s3 bucket name to upload the data to > (required)
      lambda_arn: <arn for lambda> (required)
      lambda_aws_access_key: arn:aws:lambda:{region}:{account_id}:function:{lambda name} (not give, used aws_access_key)
      lambda_aws_secret_access_key: <aws user access for lambda> (not give, used aws_secret_access_key)
      aws_access_key: <aws user access for postgres/s3 bucket> (required)
      aws_secret_access_key: <aws user secret access for postgres/s3 bucket> (required)
```

### Finalize example: `secret.yaml`
#### Local using Database Password
```
database:
  - db_identifier: example.com
    db_host: 0.0.0.0
    db_name: postgres
    db_username: postgres
    db_password: example.com
```
#### With Remote Host using Public Key
```
database:
  - db_identifier: example.com
    db_host: 0.0.0.0
    db_name: postgres
    db_username: postgres
    db_password: example.com
    remote_host: 192.168.1.1
    remote_port: "22"
    remote_username: ubuntu
    remote_key: example.pem
```

#### With AWS Cred
```
database:
  - db_identifier: example.com
    db_host: 0.0.0.0
    db_name: postgres
    db_username: postgres
    db_password: example.com
    remote_host: 192.168.1.1
    remote_port: "22"
    remote_username: ubuntu
    remote_key: example.pem
    aws:
      region: ap-southeast-1
      bucket_name: examplebucket
      lambda_arn: arn:aws:lambda:ap-southeast-1:123:function:example
      lambda_aws_access_key: example1
      lambda_aws_secret_access_key: test456
      aws_access_key: example
      aws_secret_access_key: test123
```

#### With AWS Cred and Database SSL
```
database:
  - db_identifier: example.com
    db_host: 0.0.0.0
    db_name: postgres
    db_username: postgres
    db_password: example.com
    remote_host: 192.168.1.1
    remote_port: "22"
    remote_username: ubuntu
    remote_key: example.pem
    db_ssl:
      db_sslmode: verify-full
      db_sslrootcert: example.pem
      db_sslcert: example-x509.pem
      db_sslkey: test.key
    aws:
      region: ap-southeast-1
      bucket_name: examplebucket
      lambda_arn: arn:aws:lambda:ap-southeast-1:123:function:example
      lambda_aws_access_key: example1
      lambda_aws_secret_access_key: test456
      aws_access_key: example
      aws_secret_access_key: test123
```
___
1. There are 4 main scripts to run
  - partition.py - Used to perform partitioning
  - yearly_partition.py - Used to perform partitioning based on the year provided
  - migrate.py - Used to perform migration of partition data from db to s3 bucket and remove the partition
  - reverse_partition.py - Used to reverse the partitioning

2. Other script to be used:
  - check_blocker.py - Used to verified that during partitioning, no table are block
