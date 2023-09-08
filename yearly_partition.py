"""
    Yearly_partition module to perform yearly
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
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.common import PartitionCommon
from common.wrapper import background, get_config_n_secret
from db.db import get_db
from tunnel.tunnel import get_tunnel


class YearlyPartition(PartitionCommon):
  """
  Partition class is used to perform partitioning in postgres database
  The main function is the entry point of the class

  Args:
      No args needed

  Returns:
      No returns
  """

  def perform_split_partition(
    self, logger, table, year, cur, table_name, colname=None
  ):
    if colname is None:
      raise ValueError("Column name is not provided")
    detach_old_partition = self.detach_partition.format(
      a=table["name"], b="old"
    )
    create_table = self.create_partition_of_table.format(
      a=table["name"], b=year, c=year + 1
    )
    change_table_owner = self.alter_table_owner.format(a=table_name)
    add_constraint_table = self.alter_table_constraint.format(
      a=table["name"], b=year, c=table["partition"], d=year, e=year + 1
    )
    move_lines = self.move_rows_to_another_table.format(
      a=table["name"],
      b="old",
      c=table["partition"],
      d=year,
      e=year + 1,
      f=",".join(colname),
    )
    attach_as_default = self.attach_table_as_default_partition.format(
      a=table["name"], b=f"{table['name']}_old"
    )

    logger.info("Detach old partition table")
    cur.execute(detach_old_partition)
    logger.info(f"Create new table for partition: {year}")
    cur.execute(create_table)
    logger.info(f"Change table partition ownership: {year}")
    cur.execute(change_table_owner)
    logger.info(f"Add table constraint for table: {year}")
    cur.execute(add_constraint_table)
    logger.info(f"Migrate old data to new table: {year}")
    cur.execute(move_lines)
    logger.info("Attach table as default partition")
    cur.execute(attach_as_default)

  def run_partition(self, logger, cur, table, table_name, year, colname):
    check_information_schema = self.get_table_existence.format(
      a=table["schema"], b=table_name
    )
    logger.debug("Checking table count from information_schema.tables")
    cur.execute(check_information_schema)

    data = cur.fetchall()

    if data[0][0] == 1:
      logger.warning(f"Table {table_name} already exist")
    else:
      self.perform_split_partition(
        logger, table, year, cur, table_name, colname
      )

  @background
  def yearly_partition(self, table, database_config, application_name, event):
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

      _, colname = self._get_column(table)

      partitioning = self.check_table_partition(table, cur)

      if partitioning is False:
        table_name = f"{table['name']}_{year}"

        search_path = self.set_search_path.format(a=table["schema"])
        logger.debug(search_path)
        cur.execute(search_path)

        if (
          "DEPLOYMENT" in os.environ
          and os.environ["DEPLOYMENT"] == "kubernetes"
        ):
          min_table = self.get_min_table.format(
            a=table["partition"], b=f"{table['name']}_old"
          )
          logger.debug("Checking minumum year from default partition")
          cur.execute(min_table)
          min_date = cur.fetchall()

          if min_date[0][0] is None:
            self.run_partition(logger, cur, table, table_name, year, colname)
          else:
            min_check_date = min_date[0][0].year

            year = min_check_date
            table_name = f"{table['name']}_{year}"

            self.perform_split_partition(
              logger, table, year, cur, table_name, colname
            )
        else:
          logger.debug("Skipping min migration checked")
          self.run_partition(logger, cur, table, table_name, year, colname)

        conn.commit()
      else:
        logger.info(
          f"Partitioning needed for table {table['schema']}.{table['name']}"
        )

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
      logger.info(
        f"""Yearly partition for table {
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
      "year": 2017,
    }
    self.yearly_partition(table, database_config, application_name, event)


if __name__ == "__main__":
  yearly = YearlyPartition()
  yearly.main()
