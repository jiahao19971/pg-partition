from dotenv import load_dotenv
import sys, json, yaml, re, os
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
        

def create_partitioning(collist, table, min_year, max_year, cur):
    alter_tablename = f"ALTER TABLE {table['name']} RENAME TO {table['name']}_old;"

    partitioning = f"""
        CREATE TABLE {table['name']} (
            {", ".join(collist)})
        PARTITION BY RANGE ({table['partition']});
    """

    alter_check_constraint = f"""
        ALTER TABLE {table['name']}_old ADD CONSTRAINT {table['name']}_old
        CHECK  ({table['partition']} >= '{min_year}-01-01 00:00:00' AND {table['partition']} < '{max_year + 1}-01-01 00:00:00');
    """

    alter_table_attach_partition = f"""
        ALTER TABLE {table['name']} ATTACH PARTITION {table['name']}_old
        FOR VALUES FROM ('{min_year}-01-01 00:00:00') TO ('{max_year + 1}-01-01 00:00:00');
    """

    new_year = max_year + 1

    create_new_year_partition = f"""
        CREATE TABLE {table['name']}_{new_year} PARTITION OF {table['name']}
        FOR VALUES FROM ('{new_year}-01-01 00:00:00') TO ('{new_year + 1}-01-01 00:00:00');
    """

    add_constraint_new_year_partition = f"""
        ALTER TABLE {table['name']}_{new_year} ADD CONSTRAINT {table['name']}_{new_year}
        CHECK  ({table['partition']} >= '{new_year}-01-01 00:00:00' AND {table['partition']} < '{new_year + 1}-01-01 00:00:00');
    """

    create_default_partition = f"""
        CREATE TABLE {table['name']}_default PARTITION OF {table['name']} DEFAULT;
    """
    
    print(f"Creating partition for {table['name']}")
    print("Altering table name")
    cur.execute(alter_tablename)
    print("Create partitioning")
    cur.execute(partitioning)
    print("Alter check constraint for partition table")
    cur.execute(alter_check_constraint)
    print("Attach table to partition table")
    cur.execute(alter_table_attach_partition)
    print("Create a new partition")
    cur.execute(create_new_year_partition)
    print("Add constraint to new partition")
    cur.execute(add_constraint_new_year_partition)
    print("Create default partition to store unpartition stuff")
    cur.execute(create_default_partition)

def split_partition(table, year, min_year, cur, colname):
    detach_partition = f"ALTER TABLE {table['name']} DETACH PARTITION {table['name']}_old;"
    table_name = f"{table['name']}_{year}"
    create_table = f"""
        CREATE TABLE {table_name} PARTITION OF {table['name']}
            FOR VALUES FROM ('{year}-01-01 00:00:00') TO ('{year + 1}-01-01 00:00:00');
    """

    add_constraint_table = f"""
        ALTER TABLE {table_name} ADD CONSTRAINT {table_name}
        CHECK  ({table['partition']} >= '{year}-01-01 00:00:00' AND {table['partition']} < '{year + 1}-01-01 00:00:00');
    """

    create_idx = f"CREATE INDEX idx_{table_name} ON {table_name}({table['partition']});"

    move_lines = f"""
        WITH moved_rows AS (
            DELETE FROM {table['name']}_old a
            WHERE {table['partition']} >= '{year}-01-01 00:00:00' AND {table['partition']} < '{year + 1}-01-01 00:00:00'
            RETURNING a.* 
        )
        INSERT INTO {table_name}
        SELECT {",".join(colname)} FROM moved_rows;
    """

    analyze = f"ANALYZE {table_name};"

    drop_existing_constraint = f"ALTER TABLE {table['name']}_old DROP CONSTRAINT {table['name']}_old;"

    change_constraint = f"""
        ALTER TABLE {table['name']}_old ADD constraint {table['name']}_old
        CHECK  ({table['partition']} >= '{min_year}-01-01 00:00:00' AND {table['partition']} < '{year}-01-01 00:00:00');
    """

    reattach_table = f"""
        ALTER TABLE {table['name']} ATTACH PARTITION {table['name']}_old
        FOR VALUES FROM ('{min_year}-01-01 00:00:00') TO ('{year}-01-01 00:00:00');
    """

    print(f"Splitting partition by year: {year}")
    print("Detach old partition table")
    cur.execute(detach_partition)
    print("Create new table for partition")
    cur.execute(create_table)
    print("Add table constraint for table")
    cur.execute(add_constraint_table)
    print("Create new table index")
    cur.execute(create_idx)
    print("Migrate old data to new table")
    cur.execute(move_lines)
    print("Run analyze")
    cur.execute(analyze)
    print("Drop existing constraint for partition table")
    cur.execute(drop_existing_constraint)
    print("Add new constraint for partition table")
    cur.execute(change_constraint)
    print("reattach new partition table")
    cur.execute(reattach_table)

def split_default_partition(table, year, cur, colname):
    detach_partition = f"ALTER TABLE {table['name']} DETACH PARTITION {table['name']}_default;"
    table_name = f"{table['name']}_{year}"

    create_table = f"""
        CREATE TABLE {table_name} PARTITION OF {table['name']}
            FOR VALUES FROM ('{year}-01-01 00:00:00') TO ('{year + 1}-01-01 00:00:00');
    """

    add_constraint_table = f"""
        ALTER TABLE {table_name} ADD CONSTRAINT {table_name}
        CHECK ({table['partition']} >= '{year}-01-01 00:00:00' AND {table['partition']} < '{year + 1}-01-01 00:00:00');
    """

    create_idx = f"CREATE INDEX idx_{table_name} ON {table_name}({table['partition']});"

    move_lines = f"""
        WITH moved_rows AS (
            DELETE FROM {table['name']}_default a
            WHERE {table['partition']} >= '{year}-01-01 00:00:00' AND {table['partition']} < '{year + 1}-01-01 00:00:00'
            RETURNING a.* 
        )
        INSERT INTO {table_name}
        SELECT {",".join(colname)} FROM moved_rows;
    """

    analyze = f"ANALYZE {table_name};"

    reattach_table = f"""
        ALTER TABLE {table['name']} ATTACH PARTITION {table['name']}_default DEFAULT
    """

    print(f"Splitting partition by year: {year}")
    print("Detach default partition table")
    cur.execute(detach_partition)
    print("Create new table for partition")
    cur.execute(create_table)
    print("Add table constraint for table")
    cur.execute(add_constraint_table)
    print("Create new table index")
    cur.execute(create_idx)
    print("Migrate old data to new table")
    cur.execute(move_lines)
    print("Run analyze")
    cur.execute(analyze)
    print("reattach new partition table")
    cur.execute(reattach_table)


def checker_table(table, cur):
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
    
    print("Checking table if it is partition")
    cur.execute(checker)
    data = cur.fetchall()

    return data

def check_table_partition(table, cur):
    data = checker_table(table, cur)

    if "partitioned table" in list(data[0]):
        return False
    else:
        return True
    
def additional_partitioning(table_data, cur, table, years, colname):

    data = checker_table(table_data, cur)

    if len(data) == 0:
        split_default_partition(table, years, cur, colname)
    else:
        print("Partition already exist")

def check_addon_partition_needed(cur, table):
    check_default_table = f"SELECT count(*) FROM {table['name']}_default;"

    cur.execute(check_default_table)

    default_table_count = cur.fetchall()
   
    return default_table_count

def additional_index_reverse_partitioning(index, table):
    if re.search(f"\\b{table['pkey']}", index[0]) == None:
        get_idx = index[0].split("USING")

        get_idx_name = get_idx[0].split("ON ")

        idx_name = get_idx_name[0].replace("CREATE INDEX ", "")
        idx_name = idx_name.replace(" ", "")

        get_tb_schema = get_idx_name[1].split(".")

        new_yaml_data_dict = {
            idx_name: f'USING {(get_idx[1])}'
        }
        with open('config.yaml','r') as yamlfile:
            cur_yaml = yaml.safe_load(yamlfile) # Note the safe_load

            for table in cur_yaml['table']:
                print(table['name'] in get_tb_schema[1])
                print(table['schema'] in get_tb_schema[0])
                if table['name'] in get_tb_schema[1] and table['schema'] in get_tb_schema[0]:
                    print("Adding additional index for reverse partitioning")
                    table['additional_index_name'] = {}
                    table['additional_index_name'].update(new_yaml_data_dict)

        with open('config.yaml','w') as yamlfile:
            yaml.safe_dump(cur_yaml, yamlfile)

def addon_partition(cur, table, colname):
    cur.execute(f"SELECT min({table['partition']}), max({table['partition']}) FROM {table['name']};")

    dates = cur.fetchall()

    for date in dates:
        min_check_date = date[0]
        max_check_date = date[1]

        min_check_year = min_check_date.year
        max_check_year = max_check_date.year

        update_loop_year = max_check_year - min_check_year

        for year in reversed(range(1, update_loop_year + 1)):
            years = min_check_year + year 

            new_table = {
                'name': f"{table['name']}_{years}",
                'schema': table['schema']
            }

            additional_partitioning(new_table, cur, table, years, colname)

        min_table = {
            'name': f"{table['name']}_{min_check_year}",
            'schema': table['schema']
        }

        additional_partitioning(min_table, cur, table, min_check_year, colname)

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

        cur.execute(f"select indexdef from pg_indexes where tablename = '{table['name']}' and schemaname = '{table['schema']}'")

        index_data = cur.fetchall()

        index_status = [index[0] for index in index_data if table['partition'] in index[0]]

        if len(index_status) == 0:
            cur.execute(f"CREATE UNIQUE INDEX temp_idx ON {table['name']} ({table['pkey']}, {table['partition']});")
            cur.execute(f'ALTER TABLE {table["name"]} DROP CONSTRAINT IF EXISTS {table["name"]}_pkey;')
            cur.execute(f'DROP INDEX IF EXISTS {table["name"]}_pkey;')
            cur.execute(f"ALTER INDEX temp_idx RENAME TO {table['name']}_pkey;")

        cur.execute(f"ALTER TABLE {table['name']} REPLICA IDENTITY FULL;")

        cur.execute(f"select indexdef from pg_indexes where tablename = '{table['name']}' and schemaname = '{table['schema']}'")

        index_data = cur.fetchall()
            
        cur.execute(f"SELECT min({table['partition']}), max({table['partition']}) FROM {table['name']};")

        data = cur.fetchall()

        for dates in data:
            min_date = dates[0]
            max_date = dates[1]

            min_year = min_date.year
            max_year = max_date.year

            partitioning = check_table_partition(table, cur)

            if partitioning:
                collist = []
                colname = []
                for columnname in list(table['column'].keys()):
                    newval = f"{columnname} {table['column'][columnname]}"
                    collist.append(newval)
                    colname.append(columnname)

                create_partitioning(collist, table, min_year, max_year, cur)

                create_loop_year = max_year - min_year

                for year in reversed(range(1, create_loop_year + 1)):
                    new_year = min_year + year 
                    split_partition(table, new_year, min_year, cur, colname)

                change_old_to_year = f"ALTER TABLE {table['name']}_old RENAME TO {table['name']}_{min_year};"

                cur.execute(change_old_to_year)

                get_index_on_old_table = f"select indexname from pg_indexes where tablename = '{table['name']}_2017' and schemaname = '{table['schema']}';"

                cur.execute(get_index_on_old_table)

                index_of_old_data = cur.fetchall()
                
                for indexes in index_of_old_data:
                    print("Dropping index from the old partition data")
                    cur.execute(f"DROP INDEX {indexes[0]};")
                
                for index in index_data:
                    print("Adding index to the new partition table")
                    cur.execute(f"{index[0]};")
                    
                additional_index_reverse_partitioning(index, table)
            
            else:
                print("No partitioning needed, as table already partition")
                ## Check if default have any data, if there is create a partition to store that data and remove it from default 

                default_table_count = check_addon_partition_needed(cur, table)

                if default_table_count[0][0] > 0:
                    ## Create partition for the table and move the data there (yearly basis)
                    addon_partition(cur, table, colname)

    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()