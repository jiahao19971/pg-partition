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
from live_partitioning import MicrobatchMigration
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

      conn = conn.connect()
      logger.debug(f"Connected: {db_identifier}")

      year = 2020
      (
        wschema_temp_partition_table,
        wschema_parent_table,
        wschema_default_table,
        wschema_child_table,
        _,
        temp_partition_table,
        parent_table,
        child_table,
      ) = self.create_naming(table, year)

      cur = conn.cursor()

      search_path = self.set_search_path.format(a=table["schema"])
      logger.debug(search_path)
      cur.execute(search_path)

      check_old_exist = self.check_table_exists.format(
        a=wschema_parent_table, b=f"{table['schema']}"
      )

      cur.execute(check_old_exist)

      old_exist = cur.fetchone()[0]

      if old_exist is False:
        logger.info("Old table does not exist, no data to migrate")
        return

      logger.info(f"Check if data exist in old table: {parent_table}")
      check_old_data_exist = self.check_table_row_exists.format(a=parent_table)

      cur.execute(check_old_data_exist)

      row_exists = cur.fetchone()[0]

      if row_exists is False:
        logger.info("No data to migrate")

        rename_old_to_default = self.rename_table.format(
          a=parent_table, b=wschema_default_table
        )

        logger.info(
          "Since table have no data, renaming old table to default table"
        )
        cur.execute(rename_old_to_default)

        conn.commit()
        return

      (
        get_min_date,
        get_max_date,
      ) = self.get_min_max_data_from_parent_partition(
        logger, table, parent_table, cur
      )

      if get_min_date is None or get_max_date is None:
        logger.info("No data to migrate")
        return

      for i in range(get_min_date, get_max_date + 1):
        if i == get_max_date:
          year = i
          wschema_child_table = f"{table['name']}_{year}"
          child_table = f'"{table["schema"]}".{wschema_child_table}'

          self.create_child_table_alter_index_to_pkey(
            logger,
            cur,
            table,
            wschema_child_table,
            conn,
            child_table,
            year,
            temp_partition_table,
          )

          self.data_migration(
            logger,
            get_max_date,
            table,
            cur,
            conn,
            parent_table,
            child_table,
            wschema_parent_table,
            wschema_temp_partition_table,
          )

          cur.execute(f"LOCK TABLE {parent_table} IN ACCESS EXCLUSIVE MODE;")

        logger.info(f"Getting child table max id for year: {i}")
        get_max = self.get_max_table_new.format(
          a=table["pkey"], b=f"{table['name']}_{i}"
        )
        cur.execute(get_max)
        max_id = cur.fetchone()

        if max_id is None:
          max_id = 0
        else:
          max_id = max_id[0]

        logger.info(f"Getting parent table max id for year: {i}")
        try:
          max_id_old = self.get_max_parent_id(table, cur, parent_table, i)
        except StopIteration:
          max_id_old = self.batch_get_max_parent_id(
            logger, table, parent_table, i, cur
          )

        if max_id_old == max_id:
          logger.info(f"Table {table['name']}_{i} is up to date")
          continue
        else:
          break

      get_latest_id_from_table = self.get_max_table_new.format(
        a=table["pkey"], b=parent_table
      )

      logger.info(f"Get latest id from parent table: {parent_table}")
      cur.execute(get_latest_id_from_table)

      parent_latest_id = cur.fetchone()
      if parent_latest_id is None:
        parent_latest_id = 0
      else:
        parent_latest_id = parent_latest_id[0]

      logger.debug(f"Parent id for {parent_table}: {parent_latest_id}")

      logger.info(
        f"Get latest max id from partitioned table: {temp_partition_table}"
      )
      get_latest_max_id = self.get_max_table_new.format(
        a=table["pkey"], b=temp_partition_table
      )
      cur.execute(get_latest_max_id)

      last_processed_id = cur.fetchone()
      if last_processed_id is None:
        last_processed_id = 0
      else:
        last_processed_id = last_processed_id[0]

      logger.debug(
        f"Last processed id for {temp_partition_table}: {last_processed_id}"
      )

      if parent_latest_id != last_processed_id:
        logger.info("Data not aligned")
        logger.info("Skipping table for completion step")
        return

      logger.info(f"Revert changes from view to table: {table['name']}")
      revert_changes = self.drop_view_alter_name.format(
        a=table["name"], b=temp_partition_table
      )

      cur.execute(revert_changes)

      conn.commit()

      logger.info(
        f"Dropping old table and trigger function for: {parent_table}"
      )
      cleanup_changes = self.drop_table_and_function.format(
        a=parent_table, b=f"{table['name']}_move_to_partitioned()"
      )

      cur.execute(cleanup_changes)

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
  batchrun.cleanup_cronjob_lock()
