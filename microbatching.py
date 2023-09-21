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

from psycopg2 import Error
from psycopg2.errors import WrongObjectType
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.common import PartitionCommon
from common.wrapper import background, get_config_n_secret
from db.db import get_db
from tunnel.tunnel import get_tunnel


class MicrobatchMigration(PartitionCommon):
  """
  Partition class is used to perform partitioning in postgres database
  The main function is the entry point of the class

  Args:
      No args needed

  Returns:
      No returns
  """

  def data_migration(
    self, logger, year, table, cur, conn, batch, parent_table, child_table
  ):
    logger.info("Get latest id from child table")
    get_latest_id_from_child_table = f"""
            SELECT COALESCE(MAX({table['pkey']}), 0) FROM {child_table};
        """

    cur.execute(get_latest_id_from_child_table)

    last_processed_id = cur.fetchone()[0]

    while True:
      logger.info("Get latest max id from parent table")
      get_latest_max_id = f"""
                SELECT max({table['pkey']}) FROM {parent_table}
                WHERE {table['partition']} >= '{year}-01-01 00:00:00' AND
                {table['partition']} < '{year + 1}-01-01 00:00:00';
            """

      cur.execute(get_latest_max_id)
      parent_max_id = cur.fetchone()[0]

      if last_processed_id is None:
        break
      elif parent_max_id is None:
        break
      elif parent_max_id == last_processed_id:
        break

      logger.info(f"Inserting data into child table: {last_processed_id}")
      select_query = f"""
                INSERT INTO {child_table}
                SELECT * FROM {parent_table}
                WHERE id > {last_processed_id}
                AND {table['partition']} >= '{year}-01-01 00:00:00'
                AND {table['partition']} < '{year + 1}-01-01 00:00:00'
                LIMIT {batch};
            """

      cur.execute(select_query)
      conn.commit()

      get_last_id_from_child_table = f"""
                SELECT MAX({table['pkey']}) FROM {child_table};
            """

      logger.info("Get last inserted id from child table")
      cur.execute(get_last_id_from_child_table)

      last_inserted_id = cur.fetchone()[0]
      logger.info(f"Last inserted id from child table: {last_inserted_id}")
      logger.info(f"Parent max id from parent table: {parent_max_id}")

      if last_inserted_id is None:
        break
      elif parent_max_id is None:
        break
      elif parent_max_id == last_inserted_id:
        logger.info("Lock table to prevent any write")
        cur.execute(f"LOCK TABLE {table['name']} IN ACCESS EXCLUSIVE MODE;")
        break

      last_processed_id = last_inserted_id

  def attach_partition(self, logger, year, table, cur, conn):
    try:
      logger.info(f"Attach new table to partition: {year}")
      attach_table_as_partition = self.alter_table_add_partition.format(
        a=table["name"], b=f"{table['name']}_{year}", c=year, d=year + 1
      )
      cur.execute(attach_table_as_partition)

      logger.info(f"Add constraint to new table: {year}")
      add_constraint_table = self.alter_table_constraint.format(
        a=table["name"], b=year, c=table["partition"], d=year, e=year + 1
      )

      cur.execute(add_constraint_table)

      conn.commit()
    except WrongObjectType:
      logger.info("No partitioning needed as table already partition")
    except Error as opte:
      raise opte

  @background
  def microbatching(self, table, database_config, application_name, event):
    try:
      table = event["table"]
      year = event["year"]

      db_identifier = database_config["db_identifier"]
      logger = self.logging_func(application_name=application_name)

      server = get_tunnel(database_config)
      conn = get_db(server, database_config, application_name)

      n_of_batch_default = 1000
      try:
        batch = (
          int(os.environ["BATCH_SIZE"])
          if "BATCH_SIZE" in os.environ
          else n_of_batch_default
        )
      except ValueError:
        logger.debug(
          f"BATCH_SIZE is not an integer, defaulting to {n_of_batch_default}"
        )
        batch = n_of_batch_default

      conn = conn.connect()
      logger.debug(f"Connected: {db_identifier}")

      cur = conn.cursor()
      logger.info("SET transaction isolation level to serializable")
      cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

      search_path = self.set_search_path.format(a=table["schema"])
      logger.debug(search_path)
      cur.execute(search_path)

      parent_table = f'"{table["schema"]}".{table["name"]}_old'
      child_table = f'"{table["schema"]}".{table["name"]}_{year}'

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

        year = year_to_partition.year
        child_table = f'"{table["schema"]}".{table["name"]}_{year}'

      logger.info(f"Create table if not exist: {year}")
      create_child_table_if_not_exists = f"""
                CREATE TABLE IF NOT EXISTS {child_table}
                (LIKE {parent_table} INCLUDING CONSTRAINTS INCLUDING DEFAULTS);
            """

      cur.execute(create_child_table_if_not_exists)

      conn.commit()

      self.data_migration(
        logger, year, table, cur, conn, batch, parent_table, child_table
      )

      search_path = self.set_search_path.format(a=table["schema"])
      logger.debug(search_path)
      cur.execute(search_path)

      logger.info("Data migration completed successfully")

      logger.info(f"Delete data from parent table where year: {year}")
      cur.execute(
        f"""
          DELETE FROM {parent_table}
          WHERE {table['partition']} >= '{year}-01-01 00:00:00'
          AND {table['partition']} < '{year + 1}-01-01 00:00:00';
        """
      )

      self.attach_partition(logger, year, table, cur, conn)
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
      logger.info(
        f"""Microbathing migration for table {
                table['schema']
                }.{
                table['name']
                } completed"""
      )
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    event = {
      "table": table,
      "year": 2016,
    }
    self.microbatching(table, database_config, application_name, event)


if __name__ == "__main__":
  batchrun = MicrobatchMigration()
  batchrun.main()
