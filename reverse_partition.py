from dotenv import load_dotenv
import os
from db.db import DBLoader
from tunnel.tunnel import Tunneler
from common.common import _open_config, logs
from common.query import table_check, table_check_like

load_dotenv()
logger = logs("PG_Reverse_Partition")
        
def check_table_partition(table, cur):
    checker = table_check.format(a=table['name'], b=table['schema'])
    cur.execute(checker)
    data = cur.fetchall()

    if "partitioned table" in list(data[0]):
        return True
    else:
        return False

def check_combine_table(table, cur):
    checker = table_check_like.format(a=f'{table["name"]}_%', b=table['schema'])
    cur.execute(checker)
    data = cur.fetchall()

    table_to_be_combine = []
    for table in data:
        table_to_be_combine.append(list(table)[1])

    return table_to_be_combine

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
        application_name = f"{table['schema']}.{table['name']}"

        conn = DBLoader(server, os.environ['DATABASE'], application_name=application_name)
        conn = conn.connect()

        logger = logs(application_name)

        cur = conn.cursor()
        qry = f"SET search_path TO '{table['schema']}'"
        logger.info(qry)
        cur.execute(f"{qry};")

        partitioning = check_table_partition(table, cur)

        if partitioning:
            logger.info(f"Reverting table from partition to normal table: {table['schema']}.{table['name']}")
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
                logger.info(f"moving data from {tab} to {table['name']}_new")
                insertation = f"INSERT INTO {table['name']}_new SELECT {','.join(colname)} FROM {tab};"
                cur.execute(insertation)

            if len(table_combine) > 0:
                get_sequence = f"SELECT sequencename, start_value, increment_by FROM pg_sequences WHERE sequencename like '{table['name']}%' and schemaname = '{table['schema']}';"

                cur.execute(get_sequence)

                seq = cur.fetchall()
                
                drop_partitioning = f"""
                    DROP TABLE {table['name']} CASCADE;
                """

                alter_name = f"ALTER TABLE {table['name']}_new RENAME TO {table['name']};"
                alter_idx = f"ALTER INDEX {table['name']}_new_pkey RENAME TO {table['name']}_pkey; "

                # run_analyze = f"ANALYZE {table['name']};"

                cur.execute(drop_partitioning)
                logger.debug("Rename table to original table name")
                cur.execute(alter_name)
                logger.debug("Rename index to original table")
                cur.execute(alter_idx)
                # logger.debug("Running analyze")
                # cur.execute(run_analyze)

                if 'additional_index_name' in table:
                    idxColList = []
                    for idx_col in list(table['additional_index_name'].keys()):
                        newval = f"CREATE INDEX {idx_col} ON {table['name']} {table['additional_index_name'][idx_col]};"
                        idxColList.append(newval)

                    for idx in idxColList:
                        cur.execute(idx)

                for sequence in seq:
                    create_sequence = f'CREATE SEQUENCE IF NOT EXISTS "{table["schema"]}".{sequence[0]} START WITH {sequence[1]} INCREMENT BY {sequence[2]};'

                    change_owner = f"""
                        ALTER TABLE IF EXISTS "{table["schema"]}".{table['name']} OWNER TO postgres;
                        ALTER SEQUENCE IF EXISTS "{table["schema"]}".{sequence[0]} OWNER TO postgres;
                    """
                    
                    change_sequence_ownership = f'ALTER SEQUENCE IF EXISTS "{table["schema"]}".{sequence[0]} OWNED BY {table["name"]}.{table["pkey"]}'

                    logger.debug("Create sequence for table")
                    cur.execute(create_sequence)
                    logger.debug("Change table and sequence owner to postgres")
                    cur.execute(change_owner)
                    logger.debug("Change sequence ownership back to original table")
                    cur.execute(change_sequence_ownership)

        else:
            logger.info(f"No reversing partition needed for table: {table['schema']}.{table['name']}")
        conn.commit()
    conn.close()

if __name__ == "__main__":
    main()