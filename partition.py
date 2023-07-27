from dotenv import load_dotenv
import re, os, time
from db.db import DBLoader
from tunnel.tunnel import Tunneler
from common.common import _open_config, logger, logs
from ruamel.yaml import YAML
from common.query import (
    create_table_with_partitioning, 
    alter_table_constraint, 
    attach_table_as_default_partition,
    create_partition_of_table, 
    move_rows_to_another_table, 
    detach_partition, 
    attach_default_partition, 
    table_check, 
    default_table_check, 
    rename_table, 
    alter_table_owner, 
    alter_sequence_owner,
    alter_sequence_owned_by,
    get_table_index,
    get_min_max_table,
    alter_replica_identity,
    set_search_path,
)
from multiprocessing import Process

yaml = YAML()
yaml.preserve_quotes = True

load_dotenv()

def checker_table(table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name=application_name)
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
    logger = logs(application_name=application_name)

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
    logger = logs(application_name=application_name)
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
    logger = logs(application_name=application_name)
    if re.search(f"\\b{table['pkey']}", index) == None:
        get_idx = index.split("USING")

        get_idx_name = get_idx[0].split("ON ")

        idx_name = get_idx_name[0].replace("CREATE INDEX ", "")
        idx_name = idx_name.replace(" ", "")

        get_tb_schema = get_idx_name[1].split(".")

        new_yaml_data_dict = {
            idx_name: f'USING {(get_idx[1])}'
        }
        configfile = "config.yaml"
        if os.environ['ENV'] == "staging":
            configfile = "config.staging.yaml"
        
        with open(configfile,'r') as yamlfile:
            cur_yaml = yaml.load(yamlfile)

            for table in cur_yaml['table']:
                if table['name'] in get_tb_schema[1] and table['schema'] in get_tb_schema[0]:
                    logger.info("Adding additional index for reverse partitioning")
                    table['additional_index_name'] = {}
                    table['additional_index_name'].update(new_yaml_data_dict)

        with open(configfile,'w') as yamlfile:
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

def change_owner_on_index_table(table, cur):
    change_owner = alter_table_owner.format(a=table['name'])
    change_owner_sequence = alter_sequence_owner.format(a=table['name'])
    change_sequence_ownership = alter_sequence_owned_by.format(a=table['name'], b=f"{table['name']}.{table['pkey']}")

    logger.info("Change sequence ownership")
    cur.execute(change_owner)
    cur.execute(change_owner_sequence)
    cur.execute(change_sequence_ownership)

def create_partitioning(collist, table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name=application_name)

    alter_tablename = rename_table.format(a=table['name'], b=f"{table['name']}_old")
    
    partitioning = create_table_with_partitioning.format(a=table['name'], b=", ".join(collist), c=table['partition'])

    add_as_default = attach_table_as_default_partition.format(a=table['name'], b=f"{table['name']}_old")
    
    logger.info(f"Creating partition for {table['schema']}.{table['name']}")
    logger.debug("Altering table name")
    cur.execute(alter_tablename)
    logger.debug("Create partitioning")
    cur.execute(partitioning)
    logger.debug("Attach table as default partition")
    cur.execute(add_as_default)

def get_column(table):
    collist = []
    colname = []

    for columnname in list(table['column'].keys()):
        newval = f"{columnname} {table['column'][columnname]}"
        collist.append(newval)
        colname.append(columnname)

    return collist, colname

def _get_tunnel():
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

    return server

def _get_config():
    if os.environ['ENV'] == "staging":
        configfile = "config.staging.yaml"
    else:
        configfile = "config.yaml"
    config = _open_config(configfile)

    return config

def get_index_required(table, cur):
    get_index_def = get_table_index.format(a="indexdef, indexname", b=table['name'], c=table['schema'])
    cur.execute(get_index_def)

    index_data = cur.fetchall()

    if len(index_data) > 0:
        index_status = [index[0] for index in index_data if table['partition'] in index[0]]

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

        if len(partition_index) == 0:
            partition_index = []
        else:
            partition_index = [idx[0] for idx in partition_index]
        
        if len(index_status) == 0 and len(partition_index) == 0:
            index_data[0] = (index_data[0][0].replace(table['pkey'], f"{table['pkey']}, {table['partition']}"), index_data[0][1])

    return index_data

def perform_partitioning(table):
    server = _get_tunnel()
    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name=application_name)
    conn = DBLoader(server, os.environ['DATABASE'], application_name=application_name)
    conn = conn.connect()
    cur = conn.cursor()
    try:
        set_searchPath = set_search_path.format(a=table['schema'])
        logger.debug(set_searchPath)
        cur.execute(set_searchPath)

        index_data = get_index_required(table, cur)

        change_replica_identity = alter_replica_identity.format(a=table['name'])
        cur.execute(change_replica_identity)

        partitioning = check_table_partition(table, cur)

        collist, colname = get_column(table)

        if partitioning:
            create_partitioning(collist, table, cur)

            change_owner_on_index_table(table, cur)

            for index in index_data:
                idx_name = index[1]
                idx_query = index[0]

                logger.debug("Renaming index {a} to {a}_old".format(a=idx_name))
                alter_idx = f"ALTER INDEX {idx_name} RENAME TO {idx_name}_old;"
                cur.execute(alter_idx)

                if idx_name == f"{table['name']}_pkey":
                    partition_table_idx = f"CREATE UNIQUE INDEX {table['name']}_pkey ON ONLY {table['name']} USING btree ({table['pkey']}, {table['partition']});"
                else:
                    partition_table_idx = f"{idx_query}".replace("ON", "ON ONLY")

                cur.execute(partition_table_idx)

                additional_index_reverse_partitioning(idx_query, table)

            conn.commit()
            conn.close()

            new_conn = DBLoader(server, os.environ['DATABASE'], application_name=application_name)
            new_conn = new_conn.connect()
            new_conn.autocommit = True
            new_cur = new_conn.cursor()

            set_searchPath = set_search_path.format(a=table['schema'])
            logger.debug(set_searchPath)
            new_cur.execute(set_searchPath)

            new_idx_name = f'{table["name"]}_old_{table["pkey"]}_{table["partition"]}_idx'

            logger.debug(f"Creating new unique index concurrently for {new_idx_name}")
            new_cur.execute(f'CREATE UNIQUE INDEX CONCURRENTLY {new_idx_name} ON {table["name"]}_old USING btree ({table["pkey"]}, {table["partition"]});')

            logger.debug(f"Attaching index {new_idx_name} to partition index {table['name']}_pkey")
            new_cur.execute(f"ALTER INDEX {table['name']}_pkey ATTACH PARTITION {new_idx_name};")

            if 'additional_index_name' in table:
                idxColList = []
                for idx_col in list(table['additional_index_name'].keys()):
                    d = re.findall('(?<=\().+?(?=\))', table['additional_index_name'][idx_col])

                    d = d[0]

                    d = d.split(",")

                    d = [x.replace(" ", "") for x in d]

                    if len(d) > 1:
                        d - "_".join(d)
                    else:
                        d = d[0]
                
                    addon_new_idx_name = f"{table['name']}_old_{d}_idx"
                    
                    logger.debug(f"Creating new index concurrently for {addon_new_idx_name}")
                    newval = f"CREATE INDEX CONCURRENTLY {addon_new_idx_name} ON {table['name']}_old {table['additional_index_name'][idx_col]};"

                    logger.debug(f"Attaching index {addon_new_idx_name} to partition index {idx_col}")
                    newalter = f"ALTER INDEX {idx_col} ATTACH PARTITION {addon_new_idx_name};"

                    idxColList.append(newval)
                    idxColList.append(newalter)

                for idx in idxColList:
                    new_cur.execute(idx)
            new_conn.close()
        else:
            logger.info(f"No partitioning needed, as table already partition: {table['schema']}.{table['name']}")
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(e)
        logger.error("Error occured while partitioning, rolling back")
        conn.rollback()
    conn.close()

def main():
    tic = time.perf_counter()
    config = _get_config()

    partitions = []
    for table in config['table']:
        partition = Process(target=perform_partitioning, args=(table,))
        partition.daemon = True
        partitions.append(partition)

    for partition in partitions:
        partition.start()
    
    for partition in partitions:
        partition.join()

    toc = time.perf_counter()
    logger.debug(f"Script completed in {toc - tic:0.4f} seconds")

if __name__ == "__main__":
    main()