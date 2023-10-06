"""
  CompletionMigration is extend the class of MicrobatchMigration
  The main purpose is to remove data from default partition table
  and attached the new partition table to the parent table.

  This class will not execute if the table count does not meet the requirement
  of the parent table.
"""
from multiprocessing import Process

from psycopg2 import Error
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.wrapper import get_config_n_secret
from db.db import get_db
from microbatching import MicrobatchMigration
from tunnel.tunnel import get_tunnel


class CompletionMigration(MicrobatchMigration):
  """
  CompletionMigration class is used to perform completion step for
  each year of partition in postgres database
  The main function is the entry point of the class

  Args:
      No args needed

  Returns:
      No returns
  """

  # @background
  def complete_partition(self, table, database_config, application_name):
    try:
      db_identifier = database_config["db_identifier"]
      logger = self.logging_func(application_name=application_name)

      server = get_tunnel(database_config)
      conn = get_db(server, database_config, application_name)

      (
        insert_col,
        value_col,
        update_col,
        update_val_col,
      ) = self.create_trigger_column(table["column"])

      conn = conn.connect()
      logger.debug(f"Connected: {db_identifier}")

      cur = conn.cursor()

      search_path = self.set_search_path.format(a=table["schema"])
      logger.debug(search_path)
      cur.execute(search_path)

      check_old_exist = self.check_table_exists.format(
        a=f"{table['name']}_old", b=f"{table['schema']}"
      )

      cur.execute(check_old_exist)

      old_exist = cur.fetchone()[0]

      if old_exist is False:
        logger.info("No data to migrate")
        return

      new_partition_parent = f'"{table["schema"]}".{table["name"]}_partitioned'
      parent_table = f'"{table["schema"]}".{table["name"]}_old'

      get_latest_id_from_table = self.get_max_table_new.format(
        a=table["pkey"], b=parent_table
      )

      logger.info(f"Get latest id from parent table: {parent_table}")
      cur.execute(get_latest_id_from_table)

      last_processed_id = cur.fetchone()
      if last_processed_id is None:
        last_processed_id = 0
      else:
        last_processed_id = last_processed_id[0]

      logger.debug(f"Last processed id for {parent_table}: {last_processed_id}")

      logger.info(
        f"Get latest max id from partitioned table: {new_partition_parent}"
      )
      get_latest_max_id = self.get_max_table_new.format(
        a=table["pkey"], b=new_partition_parent
      )
      cur.execute(get_latest_max_id)

      parent_max_id = cur.fetchone()
      if parent_max_id is None:
        parent_max_id = 0
      else:
        parent_max_id = parent_max_id[0]

      logger.debug(
        f"Last processed id for {new_partition_parent}: {parent_max_id}"
      )

      if last_processed_id != parent_max_id:
        logger.info("Parent table has new data")
        logger.info("Skipping table for completion step")
        return

      create_moving_data = self.create_partition_function_and_trigger.format(
        a=new_partition_parent,
        b=",".join(insert_col),
        c=",".join(value_col),
        d=table["pkey"],
        e=",".join(update_col),
        f=",".join(update_val_col),
        g=table["name"],
      )

      cur.execute(create_moving_data)

      cur.execute(
        f"""
        DROP VIEW IF EXISTS {table['name']};

        ALTER TABLE {new_partition_parent} RENAME TO {table['name']};
      """
      )

      conn.commit()

      cur.execute(
        f"""
        DROP TABLE IF EXISTS {parent_table};

        DROP FUNCTION IF EXISTS move_to_partitioned();
      """
      )

      conn.commit()
      conn.close()
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
      logger.info(f"Complete migration for table {table['name']}")
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):

    p = Process(
      target=self.complete_partition,
      args=(
        table,
        database_config,
        application_name,
      ),
    )

    return p


if __name__ == "__main__":
  batchrun = CompletionMigration()
  batchrun.main()
