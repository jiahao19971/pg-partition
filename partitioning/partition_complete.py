"""
    MicroBatching module to perform yearly
    partitioning in postgres database.It utilize
    asycio background task to perform year
    partitioning by extracting the specify year
    from args and perform the partitioning

    It detaches the old partition table, create
    new table for the new partition base on year
    and migrate year data from the old partition
    to the new partition and add index for the
    new partition table. Once completed, attached
    the old partition back as default partition

    Besides, it run in microbatch with default of
    1000 (can be increased to higher value). It
    helps to reduce the load to database and
    the lock when migrating data.

    Additional:
    This module is supported for kubernetes
    deployment, it will check the minimum year
    from the default partition table and perform
    the partitioning base on the minimum year.
    If the minimum year is 2015, it will perform
    partitioning and create a new table for 2015
    and migrate the data from the old partition.

    By deploying as a kubernetes deployment,
    the event: <year> will be ignored until
    there is no data left in the default partition.
"""
import os
from multiprocessing import Process

from psycopg2 import Error
from psycopg2.errors import WrongObjectType
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.wrapper import get_config_n_secret
from db.db import get_db
from microbatching import MicrobatchMigration
from tunnel.tunnel import get_tunnel


class CompletionhMigration(MicrobatchMigration):
  """
  Partition class is used to perform partitioning in postgres database
  The main function is the entry point of the class

  Args:
      No args needed

  Returns:
      No returns
  """

  def attach_partition(self, logger, year, table, cur, conn):
    try:
      logger.info(f"Attach new table to partition: {year}")
      attach_table_as_partition = self.alter_table_add_partition.format(
        a=f'{table["name"]}_temp',
        b=f"{table['name']}_{year}",
        c=year,
        d=year + 1,
      )
      cur.execute(attach_table_as_partition)

      logger.info(f"Add constraint to new table: {year}")
      add_constraint_table = self.alter_table_constraint.format(
        a=f'{table["name"]}', b=year, c=table["partition"], d=year, e=year + 1
      )

      cur.execute(add_constraint_table)

      conn.commit()
    except WrongObjectType:
      logger.info("No partitioning needed as table already partition")
    except Error as opte:
      raise opte

  def completion_step(self, logger, table, cur, year, parent_table, conn):
    search_path = self.set_search_path.format(a=table["schema"])
    logger.debug(search_path)
    cur.execute(search_path)

    self.attach_partition(logger, year, table, cur, conn)

  def check_table_count(self, cur, year, parent_table, child_table, logger):
    logger.info("Lock table to prevent any write")
    cur.execute(f"LOCK TABLE {parent_table} IN ACCESS EXCLUSIVE MODE;")

    logger.info("Checking table count of parent and children")

    cur.execute(
      f"""
      SELECT count(*) FROM {parent_table}
      WHERE created_at >= '{year}-01-01 00:00:00'
      AND created_at < '{year + 1}-01-01 00:00:00';
    """
    )

    count = cur.fetchone()[0]

    cur.execute(
      f"""
      SELECT count(*) FROM {child_table};
    """
    )

    count_child = cur.fetchone()[0]

    count_result = count_child == count

    return count_result

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

      parent_table = f'"{table["schema"]}".{table["name"]}'
      child_table = f'"{table["schema"]}".{table["name"]}_{year}'

      cur.execute(
        f"""
          SELECT EXISTS (
              SELECT 1 FROM pg_tables
              WHERE tablename = '{table["name"]}_temp'
              AND schemaname = '{table["schema"]}'
          ) AS table_existence;
      """
      )

      table_exist = cur.fetchone()[0]

      if table_exist is False:
        logger.info(f"Table {table['name']}_temp does not exist, skipping")
        conn.close()
        return

      if (
        "DEPLOYMENT" in os.environ and os.environ["DEPLOYMENT"] == "kubernetes"
      ):
        cur.execute(f"SELECT min({table['pkey']}) FROM {parent_table}")

        min_table = cur.fetchone()[0]

        if min_table is None:
          logger.info("No data to migrate")
          return

        cur.execute(
          f"""
            SELECT {table['partition']}
            FROM {parent_table}
            WHERE {table['pkey']} = {min_table}
          """
        )

        year_to_partition = cur.fetchone()[0]
        min_year = year_to_partition.year

        cur.execute(f"SELECT max({table['pkey']}) FROM {parent_table}")
        max_table = cur.fetchone()[0]

        cur.execute(
          f"""
            SELECT {table['partition']}
            FROM {parent_table}
            WHERE {table['pkey']} = {max_table}
          """
        )

        max_year_to_partition = cur.fetchone()[0]
        max_year = max_year_to_partition.year + 1

        for i in range(min_year, max_year):
          year = i
          child_table = f'"{table["schema"]}".{table["name"]}_{year}'

          cur.execute(
            f"""
                SELECT EXISTS (
                    SELECT 1 FROM pg_tables
                    WHERE tablename = '{table["name"]}_{year}'
                    AND schemaname = '{table["schema"]}'
                ) AS table_existence;
            """
          )

          table_exist = cur.fetchone()[0]

          if table_exist is False:
            logger.warning(f"Table {child_table} does not exist yet")
            logger.warning(f"Wait for the microbatch to create {child_table}")
            return

          check_child_is_partition = self.check_table_part_of_partition.format(
            a=f"{table['name']}_temp",
            b=table["schema"],
            c=f"{table['name']}_{year}",
          )

          cur.execute(check_child_is_partition)

          is_table_partition = cur.fetchone()[0] > 0

          if is_table_partition is False:
            logger.info(f"Table {child_table} is not a partition table")
            logger.info(
              f"Proceeding to attach partition {child_table} to {table['name']}"
            )

            self.completion_step(logger, table, cur, year, parent_table, conn)
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
  batchrun = CompletionhMigration()
  batchrun.main()
