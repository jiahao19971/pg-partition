from dotenv import load_dotenv
import sys, json, yaml, os
from db.db import DBLoader
from cerberus import Validator

load_dotenv()

def print_psycopg2_exception(err):
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno

    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")

def _open_config(config_name):
    with open(config_name, "r", encoding="utf-8") as stream:
      try:
        data = yaml.safe_load(stream)
        with open("config.json", "r", encoding="utf-8") as validation_rules:
          schema = json.load(validation_rules)
          v = Validator(schema)
          if v.validate(data, schema):
            print("Validated config.yml and no issue has been found")
            return data
          else:
            raise ValueError(v.errors)
      except ValueError as e:
        raise e
      except yaml.YAMLError as yamlerr:
        if hasattr(yamlerr, "problem_mark"):
          pm = yamlerr.problem_mark
          message = "Your file {} has an issue on line {} at position {}"
          format_message = message.format(pm.name, pm.line, pm.column)
          raise ValueError(format_message) from yamlerr
        else:
          message = "Something went wrong while parsing config.yaml file"
          raise ValueError(message) from yamlerr
        
def check_table_partition(table, cur):
    checker = f"""
            SELECT n.nspname as "Schema",
                c.relname as "Name",
                CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
                pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size"
                FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r','p','v','m','S','f','')
                    AND n.nspname <> 'pg_catalog'
                    AND n.nspname <> 'information_schema'
                    AND n.nspname !~ '^pg_toast'
                    AND c.relname = '{table['name']}'
                    AND n.nspname = '{table['schema']}'
                AND pg_catalog.pg_table_is_visible(c.oid)
                ORDER BY 1,2;
        """
    cur.execute(checker)
    data = cur.fetchall()

    if "partitioned table" in list(data[0]):
        return False
    else:
        return True

def check_combine_table(table, cur):
    checker = f"""
            SELECT n.nspname as "Schema",
                c.relname as "Name",
                CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
                pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size"
                FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r','p','v','m','S','f','')
                    AND n.nspname <> 'pg_catalog'
                    AND n.nspname <> 'information_schema'
                    AND n.nspname !~ '^pg_toast'
                    AND c.relname like '{table['name']}_%'
                    AND n.nspname = '{table['schema']}'
                AND pg_catalog.pg_table_is_visible(c.oid)
                ORDER BY 1,2;
        """
    cur.execute(checker)
    data = cur.fetchall()

    table_to_be_combine = []
    for table in data:
        if "table" in list(table):
            table_to_be_combine.append(list(table)[1])

    return table_to_be_combine

    

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

        cur.execute(f"SET search_path TO '{table['schema']}';")

        partitioning = check_table_partition(table, cur)

        if not partitioning:
            print("Reverting index back to 1 table")
            collist = []
            colname = []
            for columnname in list(table['column'].keys()):
                newval = f"{columnname} {table['column'][columnname]}"
                collist.append(newval)
                colname.append(columnname)

            table_combine = check_combine_table(table, cur)

            create_table = f"""
                CREATE TABLE {table['name']}_new (
                    {", ".join(collist)},
                    primary key ({table['pkey']})    
                )
            """

            cur.execute(create_table)
            for tab in table_combine:
                insert = f"INSERT INTO {table['name']}_new SELECT {','.join(colname)} FROM {tab};"
                cur.execute(insert)

            
            drop_partitioning = f"""
                DROP TABLE {table['name']} CASCADE;
            """

            alter_name = f"ALTER TABLE {table['name']}_new RENAME TO {table['name']};"
            alter_idx = f"ALTER INDEX {table['name']}_new_pkey RENAME TO {table['name']}_pkey; "

            run_analyze = f"ANALYZE {table['name']};"

            cur.execute(drop_partitioning)
            cur.execute(alter_name)
            cur.execute(alter_idx)
            cur.execute(run_analyze)

            if 'additional_index_name' in table:
                idxColList = []
                for idx_col in list(table['additional_index_name'].keys()):
                    newval = f"CREATE INDEX {idx_col} ON {table['name']} {table['additional_index_name'][idx_col]};"
                    idxColList.append(newval)

                for idx in idxColList:
                    cur.execute(idx)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()