from common.query import (
    detach_partition,
    set_search_path,
    create_partition_of_table,
    alter_table_owner,
    alter_table_constraint,
    move_rows_to_another_table,
    attach_table_as_default_partition,
)
import os, logging
from common.common import _open_config, background
from tunnel.tunnel import Tunneler
from db.db import DBLoader

logging.basicConfig(format="%(asctime)s - %(levelname)s: %(APPNAME)s @ %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

def logs(application_name="PG_Partition"):
  logs = logging.LoggerAdapter(logging.getLogger("PGPartition"), {'APPNAME': application_name})

  return logs

def get_column(table):
    colname = []

    for columnname in list(table['column'].keys()):
        colname.append(columnname)

    return colname

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

@background
def yearly_partition(event):
    table     = event['table']
    year      = event['year']

    application_name = f"{table['schema']}.{table['name']}"
    logger = logs(application_name=application_name)
    
    try:
        server = _get_tunnel()
        colname = get_column(table)
        conn = DBLoader(server, os.environ['DATABASE'], application_name=application_name)
        conn = conn.connect()
        cur = conn.cursor()

        set_searchPath = set_search_path.format(a=table['schema'])
        logger.debug(set_searchPath)
        cur.execute(set_searchPath)

        detach_old_partition = detach_partition.format(a=table['name'], b='old')
        create_table = create_partition_of_table.format(a=table['name'], b=year, c=year + 1)
        change_table_owner = alter_table_owner.format(a=f"{table['name']}_{year}")
        add_constraint_table = alter_table_constraint.format(a=table['name'], b=year, c=table['partition'], d=year, e=year + 1)
        move_lines = move_rows_to_another_table.format(a=table['name'], b='old', c=table['partition'], d=year, e=year + 1, f=",".join(colname))
        attach_as_default = attach_table_as_default_partition.format(a=table['name'], b=f"{table['name']}_old")

        logger.debug("Detach old partition table")
        cur.execute(detach_old_partition)
        logger.debug(f"Create new table for partition: {year}")
        cur.execute(create_table)
        logger.debug(f"Change table partition ownership: {year}")
        cur.execute(change_table_owner)
        logger.debug(f"Add table constraint for table: {year}")
        cur.execute(add_constraint_table)
        logger.debug(f"Migrate old data to new table: {year}")
        cur.execute(move_lines)
        logger.debug("Attach table as default partition")
        cur.execute(attach_as_default)

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(e)
        conn.rollback()
        conn.close()
    
def _get_config():
    if os.environ['ENV'] == "staging":
        configfile = "config.staging.yaml"
    else:
        configfile = "config.yaml"
    config = _open_config(configfile)

    return config

if __name__ == "__main__":
    event = {
        "table": {},
        "year": 2017,
    }
    
    config = _get_config()
    partitions = []
    for table in config['table']:
        event['table'] = table
        yearly_partition(event)