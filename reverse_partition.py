from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

from common.common import PartitionCommon
from common.query import table_check, table_check_like
from common.wrapper import background, get_config_n_secret
from db.db import _get_db
from tunnel.tunnel import _get_tunnel

load_dotenv()


class ReversePartition(PartitionCommon):
  def __init__(self) -> None:
    super().__init__()

  def check_table_partition(self, table, cur):
    checker = table_check.format(a=table["name"], b=table["schema"])
    cur.execute(checker)
    data = cur.fetchall()

    if "partitioned table" in list(data[0]):
      return True
    else:
      return False

  def check_combine_table(self, table, cur):
    checker = table_check_like.format(
      a=f'{table["name"]}__%', b=table["schema"]
    )
    cur.execute(checker)
    data = cur.fetchall()

    table_to_be_combine = []
    for table in data:
      table_to_be_combine.append(list(table)[1])

    return table_to_be_combine

  @background
  def reverse_partition(self, table, database_config, application_name):
    db_identifier = database_config["db_identifier"]
    logger = self.logging_func(application_name=application_name)

    server = _get_tunnel(database_config)
    conn = _get_db(server, database_config, application_name)
    logger.debug(f"Connected: {db_identifier}")

    conn = conn.connect()
    cur = conn.cursor()
    try:
      qry = f"SET search_path TO '{table['schema']}'"
      logger.info(qry)
      cur.execute(f"{qry};")

      partitioning = self.check_table_partition(table, cur)

      if partitioning:
        logger.info(
          f"Reverting table from partition to normal table: {table['schema']}.{table['name']}"
        )
        collist = []
        colname = []
        for columnname in list(table["column"].keys()):
          newval = f"{columnname} {table['column'][columnname]}"
          collist.append(newval)
          colname.append(columnname)

        table_combine = self.check_combine_table(table, cur)

        create_table = f"""
                    CREATE TABLE {table['name']}_new (
                        {", ".join(collist)},
                        primary key ({table['pkey']})
                    )
                """

        cur.execute(create_table)

        for tab in table_combine:
          logger.info(f"Moving data from {tab} to {table['name']}_new")
          insertation = f"INSERT INTO {table['name']}_new SELECT {','.join(colname)} FROM {tab};"
          cur.execute(insertation)

        if len(table_combine) > 0:
          get_sequence = f"SELECT sequencename, start_value, increment_by, last_value FROM pg_sequences WHERE sequencename like '{table['name']}%' and schemaname = '{table['schema']}';"

          cur.execute(get_sequence)

          seq = cur.fetchall()

          drop_partitioning = f"""
                        DROP TABLE {table['name']} CASCADE;
                    """

          alter_name = (
            f"ALTER TABLE {table['name']}_new RENAME TO {table['name']};"
          )
          alter_idx = f"ALTER INDEX {table['name']}_new_pkey RENAME TO {table['name']}_pkey; "

          run_analyze = f"ANALYZE {table['name']};"

          cur.execute(drop_partitioning)
          logger.debug("Rename table to original table name")
          cur.execute(alter_name)
          logger.debug("Rename index to original table")
          cur.execute(alter_idx)
          logger.debug("Running analyze")
          cur.execute(run_analyze)

          if "additional_index_name" in table:
            idxColList = []
            for idx_col in list(table["additional_index_name"].keys()):
              newval = f"CREATE INDEX {idx_col} ON {table['name']} {table['additional_index_name'][idx_col]};"
              idxColList.append(newval)

            for idx in idxColList:
              cur.execute(idx)

          for sequence in seq:
            create_sequence = f'CREATE SEQUENCE IF NOT EXISTS "{table["schema"]}".{sequence[0]} START WITH {sequence[1]} INCREMENT BY {sequence[2]};'

            update_sequence = f""" select setval('"{table["schema"]}".{sequence[0]}', {sequence[3]}, true); """

            change_owner = f"""
                            ALTER TABLE IF EXISTS "{table["schema"]}".{table['name']} OWNER TO postgres;
                            ALTER SEQUENCE IF EXISTS "{table["schema"]}".{sequence[0]} OWNER TO postgres;
                        """

            add_sequence_back = f"""
                            ALTER TABLE "{table["schema"]}".{table['name']} ALTER COLUMN {table['pkey']} SET DEFAULT nextval('"{table["schema"]}".{sequence[0]}'::regclass);
                        """

            change_sequence_ownership = f'ALTER SEQUENCE IF EXISTS "{table["schema"]}".{sequence[0]} OWNED BY {table["name"]}.{table["pkey"]}'

            logger.debug("Create sequence for table")
            cur.execute(create_sequence)
            logger.debug("Update sequence last value")
            cur.execute(update_sequence)
            logger.debug("Change table and sequence owner to postgres")
            cur.execute(change_owner)
            logger.debug("Change sequence ownership back to original table")
            cur.execute(change_sequence_ownership)
            logger.debug("Add sequence back to original table")
            cur.execute(add_sequence_back)
        logger.info("Completed reversing partitioning")
        conn.commit()
        conn.close()
      else:
        logger.info(
          f"No reversing partition needed for table: {table['schema']}.{table['name']}"
        )
        conn.close()
      if type(server) == SSHTunnelForwarder:
        server.stop()
    except Exception as e:
      logger.error(e)
      logger.error("Error occured while reverse partitioning, rolling back")
      conn.rollback()
      conn.close()

  @get_config_n_secret
  def main(self, table, database_config, application_name):
    self.reverse_partition(table, database_config, application_name)


if __name__ == "__main__":
  reverse = ReversePartition()
  reverse.main()
