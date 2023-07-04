from dotenv import load_dotenv
import re, os, time
from db.db import DBLoader
from tunnel.tunnel import Tunneler
from common.common import _open_config, logger, logs, background
from ruamel.yaml import YAML
from common.query import (
    create_table_with_partitioning, 
    alter_table_constraint, 
    attach_table_partition, 
    create_partition_of_table, 
    create_default_table_for_partition, 
    move_rows_to_another_table, 
    detach_partition, 
    attach_default_partition, 
    table_check, 
    default_table_check, 
    rename_table, 
    alter_table_owner, 
    drop_table_constraint,
    drop_table_index,
    alter_table_index,
    create_unique_index,
    alter_sequence_owner,
    alter_sequence_owned_by,
    get_table_index,
    get_min_max_table,
    alter_replica_identity,
    set_search_path,
    get_order_by_limit_1
)
from multiprocessing import Process

yaml = YAML()
yaml.preserve_quotes = True

load_dotenv()

def split_partition(table, year, min_year, cur, colname):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name)
    
    detach_old_partition = detach_partition.format(a=table['name'], b='old')
    
    create_table = create_partition_of_table.format(a=table['name'], b=year, c=year + 1)

    change_table_owner = alter_table_owner.format(a=f"{table['name']}_{year}")

    add_constraint_table = alter_table_constraint.format(a=table['name'], b=year, c=table['partition'], d=year, e=year + 1)

    move_lines = move_rows_to_another_table.format(a=table['name'], b='old', c=table['partition'], d=year, e=year + 1, f=",".join(colname))

    drop_existing_constraint = drop_table_constraint.format(a=f"{table['name']}_old", b=f"{table['name']}_old")

    change_constraint = alter_table_constraint.format(a=table['name'], b='old', c=table['partition'], d=min_year, e=year)

    reattach_table = attach_table_partition.format(a=table['name'], b=min_year, c=year)
    
    logger.info(f"Splitting partition by year: {year}")
    logger.debug("Detach old partition table")
    cur.execute(detach_old_partition)
    logger.debug("Create new table for partition")
    cur.execute(create_table)
    logger.debug("Change table partition ownership")
    cur.execute(change_table_owner)
    logger.debug("Add table constraint for table")
    cur.execute(add_constraint_table)
    logger.debug("Migrate old data to new table")
    cur.execute(move_lines)
    logger.debug("Drop existing constraint for partition table")
    cur.execute(drop_existing_constraint)
    logger.debug("Add new constraint for partition table")
    cur.execute(change_constraint)
    logger.debug("reattach new partition table")
    cur.execute(reattach_table)

def create_partitioning(collist, table, min_year, max_year, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name)
    alter_tablename = rename_table.format(a=table['name'], b=f"{table['name']}_old")
    
    partitioning = create_table_with_partitioning.format(a=table['name'], b=", ".join(collist), c=table['partition'])

    alter_check_constraint = alter_table_constraint.format(a=table['name'], b='old', c=table['partition'], d=min_year, e=max_year + 1)

    alter_table_attach_partition = attach_table_partition.format(a=table['name'], b=min_year, c=max_year + 1)

    new_year = max_year + 1

    create_new_year_partition = create_partition_of_table.format(a=table['name'], b=new_year, c=new_year + 1)

    change_new_year_owner = alter_table_owner.format(a=f"{table['name']}_{new_year}")

    add_constraint_new_year_partition = alter_table_constraint.format(a=table['name'], b=new_year, c=table['partition'], d=new_year, e=new_year + 1)

    create_default_partition = create_default_table_for_partition.format(a=table['name'])

    change_default_owner = alter_table_owner.format(a=f"{table['name']}_default")
    
    logger.info(f"Creating partition for {table['schema']}.{table['name']}")
    logger.debug("Altering table name")
    cur.execute(alter_tablename)
    logger.debug("Create partitioning")
    cur.execute(partitioning)
    logger.debug("Alter check constraint for partition table")
    cur.execute(alter_check_constraint)
    logger.debug("Attach table to partition table")
    cur.execute(alter_table_attach_partition)
    logger.debug("Create a new partition")
    cur.execute(create_new_year_partition)
    logger.debug("Change new partition ownership")
    cur.execute(change_new_year_owner)
    logger.debug("Add constraint to new partition")
    cur.execute(add_constraint_new_year_partition)
    logger.debug("Create default partition to store unpartition stuff")
    cur.execute(create_default_partition)
    logger.debug("Change default partition ownership")
    cur.execute(change_default_owner)

def checker_table(table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name)
    checker = table_check.format(a=table['name'], b=table['schema'])
    
    logger.info(f"Checking table if it is partition: {table['schema']}.{table['name']}")
    cur.execute(checker)
    data = cur.fetchall()

    return data

def check_table_partition(table, cur):
    data = checker_table(table, cur)

    if "partitioned table" in list(data[0]):
        return False
    else:
        return True
    
def split_default_partition(table, year, cur, colname):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name)

    detach_default_partition = detach_partition.format(a=table['name'], b='default')

    create_table = create_partition_of_table.format(a=table['name'], b=year, c=year + 1)

    change_table_owner = alter_table_owner.format(a=f"{table['name']}_{year}")

    add_constraint_table = alter_table_constraint.format(a=table['name'], b=year, c=table['partition'], d=year, e=year + 1)

    move_lines = move_rows_to_another_table.format(a=table['name'], b='default', c=table['partition'], d=year, e=year + 1, f=",".join(colname))

    reattach_table = attach_default_partition.format(a=table['name'])

    logger.info(f"Splitting partition by year: {year}")
    logger.debug("Detach default partition table")
    cur.execute(detach_default_partition)
    logger.debug("Create new table for partition")
    cur.execute(create_table)
    logger.debug("Change table partition ownership")
    cur.execute(change_table_owner)
    logger.debug("Add table constraint for table")
    cur.execute(add_constraint_table)
    logger.debug("Migrate old data to new table")
    cur.execute(move_lines)
    logger.debug("reattach new partition table")
    cur.execute(reattach_table)
    
def additional_partitioning(table_data, cur, table, years, colname):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name)
    data = checker_table(table_data, cur)

    if len(data) == 0:
        split_default_partition(table, years, cur, colname)
    else:
        logger.info(f"Partition already exist for table: {table['schema']}.{table['name']}")

def check_addon_partition_needed(cur, table):
    check_default_table = default_table_check.format(a=table['name'])

    cur.execute(check_default_table)

    default_table_count = cur.fetchall()

    return default_table_count

def additional_index_reverse_partitioning(index, table):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name)
    if re.search(f"\\b{table['pkey']}", index) == None:
        get_idx = index.split("USING")

        get_idx_name = get_idx[0].split("ON ")

        idx_name = get_idx_name[0].replace("CREATE INDEX ", "")
        idx_name = idx_name.replace(" ", "")

        get_tb_schema = get_idx_name[1].split(".")

        new_yaml_data_dict = {
            idx_name: f'USING {(get_idx[1])}'
        }
        with open('config.yaml','r') as yamlfile:
            cur_yaml = yaml.load(yamlfile)

            for table in cur_yaml['table']:
                if table['name'] in get_tb_schema[1] and table['schema'] in get_tb_schema[0]:
                    logger.info("Adding additional index for reverse partitioning")
                    table['additional_index_name'] = {}
                    table['additional_index_name'].update(new_yaml_data_dict)

        with open('config.yaml','w') as yamlfile:
            yaml.dump(cur_yaml, yamlfile)

def addon_partition(cur, table, colname):
    min_max = get_min_max_table.format(a=table['partition'], b=table['name'])
    cur.execute(min_max)

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

def change_table_index(table, cur):
    unique_index = create_unique_index.format(a="temp_idx", b=table['name'], c=table['pkey'], d=table['partition'])
    cur.execute(unique_index)

    drop_existing_constraint = drop_table_constraint.format(a=table["name"], b=f"{table['name']}_pkey")
    cur.execute(drop_existing_constraint)

    drop_index = drop_table_index.format(a=f"{table['name']}_pkey")
    cur.execute(drop_index)

    alter_index = alter_table_index.format(a="temp_idx", b=f"{table['name']}_pkey")
    cur.execute(alter_index)

def change_owner_on_index_table(table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name)
    change_owner = alter_table_owner.format(a=table['name'])
    change_owner_sequence = alter_sequence_owner.format(a=table['name'])
    change_sequence_ownership = alter_sequence_owned_by.format(a=table['name'], b=f"{table['name']}.{table['pkey']}")

    logger.info("Change sequence ownership")
    cur.execute(change_owner)
    cur.execute(change_owner_sequence)
    cur.execute(change_sequence_ownership)

# @background
def perform_partition(server, table):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name)
    conn = DBLoader(server, os.environ['DATABASE'], application_name=application_name)
    conn = conn.connect()
    cur = conn.cursor()
    try:
        set_replica = set_search_path.format(a=table['schema'])
        logger.debug(set_replica)
        cur.execute(set_replica)

        get_index_def = get_table_index.format(a="indexdef", b=table['name'], c=table['schema'])
        cur.execute(get_index_def)

        index_data = cur.fetchall()

        index_status = [index[0] for index in index_data if table['partition'] in index[0]]

        new_index = []

        ## Keeping this as it is a bug from postgres 10 and 11 which partition index wont show in pg_indexes thus required to call from pg_class
        check_partition_table_index = f"""
            select i.relname as indexname
            from pg_class i
                join pg_index idx on idx.indexrelid = i.oid
                join pg_class t on t.oid = idx.indrelid
                join pg_namespace n ON n.oid = t.relnamespace
            where i.relkind = 'I'
                and t.relname = '{table['name']}' 
                and n.nspname = '{table['schema']}';
        """

        cur.execute(check_partition_table_index)

        partition_index = cur.fetchall()

        partition_index = [idx[0] for idx in partition_index]
        
        if len(index_status) == 0 and len(partition_index) == 0:
            index_data[0] = index_data[0][0].replace(table['pkey'], f"{table['pkey']}, {table['partition']}")

        for index in index_data:
            if type(index) is tuple:
                new_index.append(index[0])
            else:
                new_index.append(index)

        index_data = new_index

        change_replica_identity = alter_replica_identity.format(a=table['name'])
        cur.execute(change_replica_identity)

        partitioning = check_table_partition(table, cur)

        if partitioning:
            min = get_order_by_limit_1.format(a=table['partition'], b=table['name'], c=table['pkey'], d="ASC")
            max = get_order_by_limit_1.format(a=table['partition'], b=table['name'], c=table['pkey'], d="DESC")

            cur.execute(min)
            minumum_date = cur.fetchall()

            cur.execute(max)
            maximum_date = cur.fetchall()

            min_date = minumum_date[0][0]
            max_date = maximum_date[0][0]

            min_year = min_date.year
            max_year = max_date.year
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

            old_tb_name = f"{table['name']}_{min_year}"

            change_old_to_year = rename_table.format(a=f"{table['name']}_old", b=old_tb_name)

            cur.execute(change_old_to_year)

            change_owner_on_index_table(table, cur)

            get_index_on_old_table = get_table_index.format(a="indexname", b=old_tb_name, c=table['schema'])

            cur.execute(get_index_on_old_table)

            index_of_old_data = cur.fetchall()

            for indexes in index_of_old_data:
                logger.info("Dropping index from the old partition data")
                
                drop_existing_constraint = drop_table_constraint.format(a=old_tb_name, b=indexes[0])
                cur.execute(drop_existing_constraint)
                logger.debug(f"Dropped contraint from the {old_tb_name}")

                drop_index = drop_table_index.format(a=indexes[0])
                cur.execute(drop_index)
                logger.debug(f"Dropped index from the {old_tb_name}")

            for index in index_data:
                logger.info("Adding index to the new partition table")
                cur.execute(f"{index};")
                
                additional_index_reverse_partitioning(index, table)

        else:
            logger.info(f"No partitioning needed, as table already partition: {table['schema']}.{table['name']}")
            ## Check if default have any data, if there is create a partition to store that data and remove it from default 

            default_table_count = check_addon_partition_needed(cur, table)

            if default_table_count[0][0] > 0:
                ## Create partition for the table and move the data there (yearly basis)
                addon_partition(cur, table, colname)
        conn.commit()
    except Exception as e:
        logger.error(e)
        conn.rollback()
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
        perform_partition(server, table)
        toc = time.perf_counter()
        logger.debug(f"Script completed in {toc - tic:0.4f} seconds")

if __name__ == "__main__":
    main()