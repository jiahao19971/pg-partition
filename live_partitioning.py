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
import subprocess
from multiprocessing import Process

import timeout_decorator
from psycopg2 import Error
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
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
    self,
    logger,
    year,
    table,
    cur,
    conn,
    parent_table,
    child_table,
    wschema_parent_table,
    wschema_temp_partition_table,
  ):

    logger.info("Get latest id from child table")

    get_latest_id_from_child_table = self.get_max_table_new.format(
      a=table["pkey"], b=child_table
    )

    cur.execute(get_latest_id_from_child_table)

    last_processed_id = cur.fetchone()

    if last_processed_id is None:
      last_processed_id = 0
    else:
      last_processed_id = last_processed_id[0]

    while True:
      logger.info("Get latest max id from parent table")
      get_latest_max_id = self.get_max_conditional_table_new.format(
        a=table["pkey"],
        b=parent_table,
        c=table["partition"],
        d=year,
        e=year + 1,
      )
      cur.execute(get_latest_max_id)

      parent_max_id = cur.fetchone()[0]

      if last_processed_id is None:
        break
      elif parent_max_id is None:
        break
      elif parent_max_id == last_processed_id:
        break

      logger.info(f"Inserting data into child table: {last_processed_id}")

      microbatches = self.microbatch_insert.format(
        a=child_table,
        b=parent_table,
        c=last_processed_id,
        d=table["partition"],
        e=year,
        f=year + 1,
        g=table["pkey"],
        h=self.n_of_batch_default,
      )
      cur.execute(microbatches)

      conn.commit()

      get_last_id_from_child_table = self.get_max_table_new.format(
        a=table["pkey"], b=child_table
      )

      logger.info("Get last inserted id from child table")
      cur.execute(get_last_id_from_child_table)

      last_inserted_id = cur.fetchone()

      if last_inserted_id is None:
        last_inserted_id = 0
      else:
        last_inserted_id = last_inserted_id[0]

      logger.info(f"Last inserted id from child table: {last_inserted_id}")
      logger.info(f"Parent max id from parent table: {parent_max_id}")

      logger.info(
        f"""Update view {
          wschema_parent_table
        } {
          table['pkey']
        }: {
          last_inserted_id
        }"""
      )
      update_view = self.create_view_with_where.format(
        a=table["name"],
        b=wschema_parent_table,
        c=table["pkey"],
        d=last_inserted_id,
        e=wschema_temp_partition_table,
      )
      cur.execute(update_view)

      conn.commit()

      if last_inserted_id is None:
        break
      elif parent_max_id is None:
        break
      elif parent_max_id == last_inserted_id:
        break

      last_processed_id = last_inserted_id

  def create_naming(self, table, year):
    wschema_temp_partition_table = f"{table['name']}_partitioned"
    wschema_parent_table = f'{table["name"]}_old'
    wschema_child_table = f'{table["name"]}_{year}'
    wschema_default_table = f'{table["name"]}_default'
    default_table = f'"{table["schema"]}".{wschema_default_table}'
    temp_partition_table = f'"{table["schema"]}".{wschema_temp_partition_table}'
    parent_table = f'"{table["schema"]}".{wschema_parent_table}'
    child_table = f'"{table["schema"]}".{wschema_child_table}'

    return (
      wschema_temp_partition_table,
      wschema_parent_table,
      wschema_default_table,
      wschema_child_table,
      default_table,
      temp_partition_table,
      parent_table,
      child_table,
    )

  def check_old_table_if_exist(
    self, logger, parent_table, cur, wschema_parent_table, table
  ):
    logger.info(f"Check if old table exist: {parent_table}")
    check_old_exist = self.check_table_exists.format(
      a=f"{wschema_parent_table}", b=f"{table['schema']}"
    )

    cur.execute(check_old_exist)

    old_exist = cur.fetchone()[0]

    if old_exist:
      logger.info(f"Check if data exist in old table: {parent_table}")
      check_old_data_exist = self.check_table_row_exists.format(a=parent_table)

      cur.execute(check_old_data_exist)

      row_exists = cur.fetchone()[0]

      return row_exists
    else:
      return old_exist

  def check_if_old_is_partition(self, logger, cur, table, wschema_parent_table):
    logger.info("Check if old table is part the partition")
    cur.execute(
      self.check_table_part_of_partition.format(
        a=table["name"], b=table["schema"], c=f"{wschema_parent_table}"
      )
    )

    is_table_partition = cur.fetchone()[0] == 0

    if is_table_partition is False:
      logger.info(f"Detach old table from partition: {table['name']}")
      detach_old_partition = self.detach_partition_new.format(
        a=f"{table['name']}", b=wschema_parent_table
      )

      cur.execute(detach_old_partition)

  def check_if_partitioned_exists(
    self,
    logger,
    cur,
    table,
    wschema_temp_partition_table,
  ):
    check_table_existence = self.check_table_exists.format(
      a=f"{wschema_temp_partition_table}", b=table["schema"]
    )

    logger.info("Check for partitioned table existence")
    cur.execute(check_table_existence)

    check_new_table_exist = cur.fetchone()[0]

    if check_new_table_exist is False:
      logger.info(
        f"""Partitioned table does not exist yet, renaming {
          table['name']
        } to {
          wschema_temp_partition_table
        }"""
      )
      rename_table_to_temp = self.rename_table.format(
        a=f"{table['name']}", b=wschema_temp_partition_table
      )
      cur.execute(rename_table_to_temp)

  def check_if_default_exists(
    self,
    logger,
    cur,
    table,
    wschema_temp_partition_table,
    default_table,
    wschema_default_table,
  ):
    logger.info(
      f"Create new default table for table: {wschema_temp_partition_table}"
    )

    create_default_table = self.create_default_partition_table.format(
      a=default_table, b=wschema_temp_partition_table
    )

    cur.execute(create_default_table)

    check_index_existence = self.check_index_exists.format(
      a=table["schema"], b=f"{wschema_default_table}_id_created_at_idx"
    )
    cur.execute(check_index_existence)

    default_index_exists = cur.fetchone()[0]

    if default_index_exists:
      logger.debug("Alter default table index to pkey")
      change_idx_to_pkey = self.alter_index_to_pkey.format(
        a=default_table,
        b=f"{wschema_default_table}_pkey",
        c=f"{wschema_default_table}_id_created_at_idx",
      )
      cur.execute(change_idx_to_pkey)

  def check_child_create_view(
    self,
    logger,
    cur,
    table,
    wschema_child_table,
    wschema_parent_table,
    wschema_temp_partition_table,
    child_table,
    parent_table,
    conn,
  ):

    (
      insert_col,
      value_col,
      update_col,
      update_val_col,
    ) = self.create_trigger_column(table["column"])

    check_child_existence = self.check_table_exists.format(
      a=f"{wschema_child_table}", b=table["schema"]
    )

    logger.info(f"Check for child table existence: {wschema_child_table}")
    cur.execute(check_child_existence)

    check_child_exist = cur.fetchone()[0]

    if check_child_exist is False:
      last_inserted_id = 0
    else:
      get_last_id_from_child_table = self.get_max_table_new.format(
        a=table["pkey"], b=child_table
      )

      logger.info("Get last inserted id from child table")
      cur.execute(get_last_id_from_child_table)

      last_inserted_id = cur.fetchone()

      if last_inserted_id is None:
        last_inserted_id = 0
      else:
        last_inserted_id = last_inserted_id[0]

    logger.info(
      f"Create a view for {table['name']} with old and partitioned table"
    )
    create_view_with_id = self.create_view_with_where.format(
      a=table["name"],
      b=wschema_parent_table,
      c=table["pkey"],
      d=last_inserted_id,
      e=wschema_temp_partition_table,
    )
    cur.execute(create_view_with_id)

    logger.info("Create a function to move data to old table first")

    create_moving_data = self.create_partition_function_and_trigger.format(
      a=parent_table,
      b=",".join(insert_col),
      c=",".join(value_col),
      d=table["pkey"],
      e=",".join(update_col),
      f=",".join(update_val_col),
      g=table["name"],
      h=f"{table['name']}_move_to_partitioned",
    )
    cur.execute(create_moving_data)

    conn.commit()

  def get_min_max_data_from_parent_partition(
    self, logger, table, parent_table, cur
  ):
    logger.info("Get min and max of parent_table")

    get_min = self.get_min_table_new.format(a=table["pkey"], b=parent_table)

    cur.execute(get_min)

    minimum = cur.fetchone()

    if minimum is None:
      return None, None

    minimum = minimum[0]

    logger.info(f"Get date with min year id from {parent_table}")
    get_min_year_with_id = self.get_table_custom.format(
      a=table["partition"],
      b=parent_table,
      c=table["pkey"],
      d=minimum,
    )
    cur.execute(get_min_year_with_id)

    get_min_date = cur.fetchone()[0].year

    get_max = self.get_max_table_new.format(a=table["pkey"], b=parent_table)

    cur.execute(get_max)

    maximum = cur.fetchone()[0]

    logger.info(f"Get date with max year id from {parent_table}")
    get_max_year_with_id = self.get_table_custom.format(
      a=table["partition"], b=parent_table, c=table["pkey"], d=maximum
    )
    cur.execute(get_max_year_with_id)

    get_max_date = cur.fetchone()[0].year

    return get_min_date, get_max_date

  def create_child_table_alter_index_to_pkey(
    self,
    logger,
    cur,
    table,
    wschema_child_table,
    conn,
    child_table,
    year,
    temp_partition_table,
  ):
    logger.info(f"Create table if not exist: {year}")
    create_table = self.create_partition_of_table.format(
      a=child_table,
      b=temp_partition_table,
      c=year,
      d=year + 1,
    )
    cur.execute(create_table)

    check_child_index = self.check_index_exists.format(
      a=table["schema"], b=f"{wschema_child_table}_id_created_at_idx"
    )

    cur.execute(check_child_index)

    child_index_exists = cur.fetchone()[0]

    if child_index_exists:
      change_idx_to_pkey = self.alter_index_to_pkey.format(
        a=child_table,
        b=f"{wschema_child_table}_pkey",
        c=f"{wschema_child_table}_id_created_at_idx",
      )
      cur.execute(change_idx_to_pkey)

    conn.commit()

  @timeout_decorator.timeout(10, timeout_exception=StopIteration)
  def get_max_parent_id(self, table, cur, parent_table, year):
    get_max_condi = self.get_max_conditional_table_new.format(
      a=table["pkey"],
      b=parent_table,
      c=table["partition"],
      d=year,
      e=year + 1,
    )
    cur.execute(get_max_condi)

    max_id_old = cur.fetchone()[0]

    return max_id_old

  def batch_get_max_parent_id(self, logger, table, parent_table, year, cur):
    batch_size = 1000000  # Using 1 million as default
    offset = 0
    max_id = -1

    while True:
      query = f"""
        SELECT {table["pkey"]}
        FROM {parent_table}
        WHERE {table["partition"]} >= '{year}-01-01 00:00:00'
          AND {table["partition"]} < '{year + 1}-01-01 00:00:00'
        ORDER BY {table["pkey"]}
        LIMIT {batch_size}
        OFFSET {offset};
      """

      cur.execute(query)
      for row in cur.fetchall():
        id_value = row[0]
        if id_value > max_id:
          max_id = id_value

      logger.debug(f"Table Max id in batch: {max_id}")

      offset += batch_size

      if cur.rowcount < batch_size:
        break

    return max_id

  # @background
  def microbatching(self, table, database_config, application_name, event):
    try:
      table = event["table"]
      year = event["year"]

      db_identifier = database_config["db_identifier"]
      logger = self.logging_func(application_name=application_name)

      server = get_tunnel(database_config)
      conn = get_db(server, database_config, application_name)

      (
        wschema_temp_partition_table,
        wschema_parent_table,
        wschema_default_table,
        wschema_child_table,
        default_table,
        temp_partition_table,
        parent_table,
        child_table,
      ) = self.create_naming(table, year)

      conn = conn.connect()
      logger.debug(f"Connected: {db_identifier}")

      cur = conn.cursor()

      search_path = self.set_search_path.format(a=table["schema"])
      logger.debug(search_path)
      cur.execute(search_path)

      old_exist = self.check_old_table_if_exist(
        logger, parent_table, cur, wschema_parent_table, table
      )

      if old_exist is False:
        logger.info("No data to migrate")
        return

      self.check_if_old_is_partition(logger, cur, table, wschema_parent_table)

      self.check_if_partitioned_exists(
        logger,
        cur,
        table,
        wschema_temp_partition_table,
      )

      self.check_if_default_exists(
        logger,
        cur,
        table,
        wschema_temp_partition_table,
        default_table,
        wschema_default_table,
      )

      self.check_child_create_view(
        logger,
        cur,
        table,
        wschema_child_table,
        wschema_parent_table,
        wschema_temp_partition_table,
        child_table,
        parent_table,
        conn,
      )

      if (
        "DEPLOYMENT" in os.environ and os.environ["DEPLOYMENT"] == "kubernetes"
      ):
        logger.info("Kubernetes deployment detected")

        (
          get_min_date,
          get_max_date,
        ) = self.get_min_max_data_from_parent_partition(
          logger, table, parent_table, cur
        )

        new_year = 2020
        for i in range(get_min_date, get_max_date + 1):
          logger.info(f"Checking if table exist for year: {i}")

          check_tb_exist = self.check_table_exists.format(
            a=f"{table['name']}_{i}", b=table["schema"]
          )

          cur.execute(check_tb_exist)

          tb_exist = cur.fetchone()[0]

          if tb_exist:
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
              logger.info(f"Table {table['name']}_{i} already exist")
              continue
            else:
              new_year = i
              break
          else:
            new_year = i
            break
        year = new_year
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
        year,
        table,
        cur,
        conn,
        parent_table,
        child_table,
        wschema_parent_table,
        wschema_temp_partition_table,
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
        f"""Live Partitioning for table {
                table['schema']
                }.{
                table['name']
                } completed"""
      )
      if (
        "DEPLOYMENT" in os.environ and os.environ["DEPLOYMENT"] == "kubernetes"
      ):
        with subprocess.Popen(
          ["kubectl create configmap cronjob-lock -n partitioning"],
          stdout=subprocess.PIPE,
          stderr=subprocess.STDOUT,
        ) as process:
          for line in process.stdout:
            self.logger.debug(line.decode("utf-8").strip())

          output = process.communicate()[0]

          if process.returncode != 0:
            self.logger.error(
              f"""Command failed. Return code : {
              process.returncode
            }"""
            )
          else:
            self.logger.info(output)
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    event = {
      "table": table,
      "year": 2020,
    }

    p = Process(
      target=self.microbatching,
      args=(
        table,
        database_config,
        application_name,
        event,
      ),
    )

    return p


if __name__ == "__main__":
  batchrun = MicrobatchMigration()
  batchrun.main()
