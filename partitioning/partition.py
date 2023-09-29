"""
    Partition module to perform partitioning in postgres database
    It utilize asycio background task to perform partitioning
    with renameing existing table to table_old and create table with partition
    and attached the table_old to the new table as default partition

    Besides, the module also perform additional index for the primary key and
    insert as composite key for the partition table
"""

from multiprocessing import Process

from dotenv import load_dotenv
from psycopg2 import Error
from ruamel.yaml import YAML
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
from db.db import get_db
from tunnel.tunnel import get_tunnel

yaml = YAML()
yaml.preserve_quotes = True

load_dotenv()


class Partition(PartitionCommon):
  """
  Partition class is used to perform partitioning in postgres database
  The main function is the entry point of the class

  Args:
    No args needed

  Returns:
    No returns
  """

  def change_owner_on_index_table(self, table, cur):
    change_owner = self.alter_table_owner.format(a=table["name"])
    change_owner_sequence = self.alter_sequence_owner.format(
      a=f'{table["name"]}_id_seq'
    )
    change_sequence_ownership = self.alter_sequence_owned_by.format(
      a=f"{table['name']}_id_seq", b=f"{table['name']}.{table['pkey']}"
    )

    self.logger.info("Change sequence ownership")
    cur.execute(change_owner)
    cur.execute(change_owner_sequence)
    cur.execute(change_sequence_ownership)

  def create_partitioning(self, table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = self.logging_func(application_name=application_name)

    partitioning = f"""
      CREATE TABLE IF NOT EXISTS {table['name']}_temp
      (like {table['name']}_new_temp including all)
      partition by range ({table['partition']});
    """

    logger.info(f"Creating partition for {table['schema']}.{table['name']}")
    logger.debug("Create temp table")
    cur.execute(
      f"""
      CREATE TABLE IF NOT EXISTS {table["name"]}_new_temp
        (
          LIKE {table["name"]}
          INCLUDING CONSTRAINTS
          INCLUDING DEFAULTS
          INCLUDING INDEXES
        );
    """
    )
    logger.debug("Alter pkey to composite key")
    cur.execute(
      f"""
      ALTER TABLE {table["name"]}_new_temp
        DROP CONSTRAINT {table["name"]}_new_temp_pkey,
        ADD PRIMARY KEY ({table['pkey']}, {table['partition']});
    """
    )
    logger.debug("Create partitioning main table")
    cur.execute(partitioning)
    logger.debug("Removing new temp table")
    cur.execute(
      f"""
      DROP TABLE IF EXISTS {table["name"]}_new_temp;
    """
    )
    logger.debug("Create default table for partition table")
    cur.execute(
      f"""
      CREATE TABLE {table["name"]}_default
      PARTITION OF {table["name"]}_temp DEFAULT;
    """
    )

  # @background
  def perform_partitioning(self, table, database_config, application_name):
    try:
      db_identifier = database_config["db_identifier"]
      logger = self.logging_func(application_name=application_name)

      server = get_tunnel(database_config)
      conn = get_db(server, database_config, application_name)

      conn = conn.connect()
      logger.debug(f"Connected: {db_identifier}")
      cur = conn.cursor()

      search_path = self.set_search_path.format(a=table["schema"])
      logger.debug(search_path)
      cur.execute(search_path)

      change_replica_identity = self.alter_replica_identity.format(
        a=table["name"]
      )
      cur.execute(change_replica_identity)

      partitioning = self.check_table_partition(table, cur)

      if partitioning:

        self.create_partitioning(table, cur)

        conn.commit()
        conn.close()
      else:
        logger.info(
          f"""No partitioning needed, as table already partition: {
            table['schema']
            }.{
              table['name']
            }"""
        )
        conn.close()

    except BaseSSHTunnelForwarderError as e:
      self.logger.error(f"{db_identifier}: {e}")
      conn.rollback()
      conn.close()
    except Error as opte:
      self.print_psycopg2_exception(opte)
      conn.rollback()
      conn.close()
    finally:
      logger.info(
        f"""Successfully partition table: {
          table['schema']
        }.{
          table['name']
        }"""
      )
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    p = Process(
      target=self.perform_partitioning,
      args=(table, database_config, application_name),
    )
    return p
    # self.perform_partitioning(table, database_config, application_name)


if __name__ == "__main__":
  runner = Partition()
  runner.main()
