"""
  CompletionMigration is extend the class of MicrobatchMigration
  The main purpose is to remove data from default partition table
  and attached the new partition table to the parent table.

  This class will not execute if the table count does not meet the requirement
  of the parent table.
"""
import os
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

  def completion_step(self, logger, table, cur, year, parent_table, conn):
    search_path = self.set_search_path.format(a=table["schema"])
    logger.debug(search_path)
    cur.execute(search_path)

    logger.info(f"Delete data from parent table where year: {year}")

    delete_row = self.delete_row_from_table.format(
      a=parent_table,
      b=table["partition"],
      c=year,
      d=year + 1,
    )

    cur.execute(delete_row)

    self.attach_partition(logger, year, table, cur, conn)

  def check_table_count(self, cur, year, parent_table, child_table, logger):
    ## Query
    lock_parent_table = self.lock_table.format(a=parent_table)
    check_parent_count = self.check_sepecific_table_count.format(
      a=parent_table,
      b=year,
      c=year + 1,
    )
    count_child_table = self.count_table_from_db.format(a=child_table)

    logger.info(f"Lock table {parent_table} to prevent any write")
    cur.execute(lock_parent_table)

    logger.info("Checking table count of parent and children")
    logger.debug("Getting parent table count")
    cur.execute(check_parent_count)
    count = cur.fetchone()[0]

    logger.debug("Getting child table count")
    cur.execute(count_child_table)
    count_child = cur.fetchone()[0]

    count_result = count_child == count

    return count_result

  # @background
  def complete_partition(self, table, database_config, application_name, event):
    try:
      table = event["table"]
      year = event["year"]

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

      parent_table = f'"{table["schema"]}".{table["name"]}_old'
      child_table = f'"{table["schema"]}".{table["name"]}_{year}'

      check_table_default = self.check_table_exists.format(
        a=f"{table['name']}_default", b=table["schema"]
      )

      cur.execute(check_table_default)

      default_exist = cur.fetchone()[0]

      if default_exist is True:
        parent_table = f'"{table["schema"]}".{table["name"]}_default'

      if (
        "DEPLOYMENT" in os.environ and os.environ["DEPLOYMENT"] == "kubernetes"
      ):
        logger.debug("Running on kubernetes")
        logger.debug(f"Getting min from parent table: {parent_table}")
        get_min = self.get_min_table.format(a=table["pkey"], b=parent_table)
        cur.execute(get_min)

        min_table = cur.fetchone()[0]

        if min_table is None:
          logger.info("No data to migrate")
          return

        get_id = self.get_table_custom.format(
          a=table["partition"], b=parent_table, c=table["pkey"], d=min_table
        )

        cur.execute(get_id)

        year_to_partition = cur.fetchone()[0]

        year = year_to_partition.year
        child_table = f'"{table["schema"]}".{table["name"]}_{year}'

      logger.info(f"Checking if table {child_table} exists")
      get_table_existence = self.check_table_exists.format(
        a=f"{table['name']}_{year}", b=table["schema"]
      )

      cur.execute(get_table_existence)

      table_exist = cur.fetchone()[0]

      if table_exist is False:
        logger.warning(f"Table {child_table} does not exist yet")
        logger.warning(f"Wait for the microbatch to create {child_table}")
        return

      logger.info(f"Checking if table {child_table} is a partition table")
      check_child_is_partition = self.check_table_part_of_partition.format(
        a=table["name"], b=table["schema"], c=f"{table['name']}_{year}"
      )

      cur.execute(check_child_is_partition)

      is_table_partition = cur.fetchone()[0] == 0

      if is_table_partition is False:
        logger.info(f"Table {child_table} is not a partition table")
        logger.info(
          f"Proceeding to attach partition {child_table} to {table['name']}"
        )

        equal_row = self.check_table_count(
          cur, year, parent_table, child_table, logger
        )

        if equal_row is True:
          self.completion_step(logger, table, cur, year, parent_table, conn)
        else:
          logger.warning(f"Table count not equal, skipping table {child_table}")
          logger.warning(
            "Waiting for the microbatch to complete, will retry again later"
          )
      else:
        logger.info(
          f"Table {child_table} is a partition table of {parent_table}"
        )
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
      logger.info(f"""Complete migration for table {child_table}""")
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    event = {
      "table": table,
      "year": 2020,
    }

    p = Process(
      target=self.complete_partition,
      args=(
        table,
        database_config,
        application_name,
        event,
      ),
    )

    return p


if __name__ == "__main__":
  batchrun = CompletionMigration()
  batchrun.main()
