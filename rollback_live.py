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


class RollbackMigration(MicrobatchMigration):
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
        logger.info("No table to rollback")
        return

      new_partition_parent = f'"{table["schema"]}".{table["name"]}_partitioned'
      parent_table = f'"{table["schema"]}".{table["name"]}_old'
      default_table = f'"{table["schema"]}".{table["name"]}_default'

      revert_change = self.drop_view_alter_name.format(
        a=table["name"], b=new_partition_parent
      )
      cur.execute(revert_change)

      conn.commit()

      logger.info("Get min and max id from table")
      get_min_max = self.get_min_max_table.format(
        a=table["pkey"], b=parent_table
      )

      cur.execute(get_min_max)

      min_max = cur.fetchone()

      logger.info(f"Get date with min year id from {parent_table}")
      get_min_year_with_id = self.get_table_custom.format(
        a=table["partition"],
        b=parent_table,
        c=table["pkey"],
        d=min_max[0],
      )
      cur.execute(get_min_year_with_id)

      get_min_date = cur.fetchone()[0].year

      logger.info(f"Get date with max year id from {parent_table}")
      get_max_year_with_id = self.get_table_custom.format(
        a=table["partition"],
        b=parent_table,
        c=table["pkey"],
        d=min_max[1],
      )
      cur.execute(get_max_year_with_id)

      get_max_date = cur.fetchone()[0].year

      years = []
      for i in range(get_min_date, get_max_date + 1):
        years.append(i)

        logger.info(f"Detach year {i} from {table['name']}")
        detach_partition = self.detach_partition_new.format(
          a=table["name"], b=f"{table['name']}_{i}"
        )
        cur.execute(detach_partition)

        conn.commit()

      move_rows = self.delete_move.format(
        a=default_table,
        b=parent_table,
      )

      cur.execute(move_rows)

      attach_old = self.attach_table_as_default_partition.format(
        a=table["name"], b=parent_table
      )

      logger.info(f"Drop table: {default_table}")
      drop_tb = self.drop_table_if_exists.format(a=default_table)
      cur.execute(drop_tb)

      logger.info(f"Attach old to {table['name']}")
      cur.execute(attach_old)

      conn.commit()

      for i in years:
        logger.debug(f"Dropping table for year: {i}")
        drop_tb_by_year = self.drop_table_if_exists.format(
          a=f"{table['name']}_{i}"
        )
        cur.execute(drop_tb_by_year)

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
  batchrun = RollbackMigration()
  batchrun.main()
