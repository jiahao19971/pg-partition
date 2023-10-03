"""
   CleanupMigration is extend the class of MicrobatchMigration
   The main purpose is to cleanup the old partition table and
   create a new default partition table
"""
from multiprocessing import Process

from psycopg2 import Error
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.wrapper import get_config_n_secret
from db.db import get_db
from microbatching import MicrobatchMigration
from tunnel.tunnel import get_tunnel


class CleanupMigration(MicrobatchMigration):
  """
  CleanupMigration class is used to perform cleanup once partition completed
  The main function is the entry point of the class

  Args:
      No args needed

  Returns:
      No returns
  """

  # @background
  def cleanup_partition(self, table, database_config, application_name):
    try:
      db_identifier = database_config["db_identifier"]
      logger = self.logging_func(application_name=application_name)

      server = get_tunnel(database_config)
      conn = get_db(server, database_config, application_name)

      conn = conn.connect()
      logger.debug(f"Connected: {db_identifier}")

      cur = conn.cursor()

      parent_table = f'{table["name"]}'
      default_table = f'{table["name"]}_old'
      new_default_table = f'{table["name"]}_default'

      search_path = self.set_search_path.format(a=table["schema"])
      check_table_existence = self.check_table_exists.format(
        a=default_table, b=table["schema"]
      )
      check_row_existence = self.check_table_row_exists.format(a=default_table)
      detach_partition_from_parent = self.detach_partition_new.format(
        a=parent_table, b=default_table
      )
      create_default_table = self.create_default_partition_table.format(
        a=new_default_table, b=parent_table
      )
      drop_old_table = self.drop_table_if_exists.format(a=default_table)

      logger.debug(search_path)
      cur.execute(search_path)

      logger.debug(f"Checking if table {default_table} exists")
      cur.execute(check_table_existence)

      table_exist = cur.fetchone()[0]

      if table_exist is False:
        logger.warning(f"Table {default_table} does not exist, skipping")
        return

      logger.debug(f"Table {default_table} exists")
      logger.debug(f"Checking if table {default_table} have data")
      cur.execute(check_row_existence)

      row_exist = cur.fetchone()[0]

      if row_exist is True:
        logger.warning(f"Table {default_table} is not empty, skipping")
        return

      logger.debug(f"Table {default_table} is empty, proceeding to cleanup")
      logger.debug(f"Detached partition {default_table} from {parent_table}")

      cur.execute(detach_partition_from_parent)

      logger.info(
        f"Creating default partition {new_default_table} for {parent_table}"
      )
      cur.execute(create_default_table)

      logger.info(f"Dropping old partition {default_table}")
      cur.execute(drop_old_table)

      conn.commit()
    except BaseSSHTunnelForwarderError as e:
      self.logger.error(f"{db_identifier}: {e}")
      conn.rollback()
      conn.close()
    except Error as opte:
      self.logger.error(f"psycopg2 error: {db_identifier}")
      self.print_psycopg2_exception(opte)
      conn.rollback()
      conn.close()
    finally:
      logger.info(f"""Cleanup migration for table {parent_table} completed""")
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    p = Process(
      target=self.cleanup_partition,
      args=(table, database_config, application_name),
    )
    return p


if __name__ == "__main__":
  cleanup_run = CleanupMigration()
  cleanup_run.main()
