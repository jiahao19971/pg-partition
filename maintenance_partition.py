"""
  MaintenanceMigration is extend the class of MicrobatchMigration
  The main purpose is to remove data from default partition table,
  move the data to a new partition and attached it back to the parent table.

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


class MaintenanceMigration(MicrobatchMigration):
  """
  MaintenanceMigration class is used to perform routine check for
  the default table, and migrate table to a newly year partition
  The main function is the entry point of the class

  Args:
      No args needed

  Returns:
      No returns
  """

  def maintenance_partition(self, table, database_config, application_name):
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

      year = 2020
      (
        _,
        _,
        wschema_default_table,
        wschema_child_table,
        default_table,
        _,
        _,
        child_table,
      ) = self.create_naming(table, year)

      check_default_exist = self.check_table_exists.format(
        a=wschema_default_table, b=table["schema"]
      )

      cur.execute(check_default_exist)

      default_exist = cur.fetchone()[0]

      if default_exist is False:
        logger.info(f"Default table {default_table} does not exist")
        return

      check_default_data = self.check_table_row_exists.format(a=default_table)

      cur.execute(check_default_data)

      default_data = cur.fetchone()[0]

      if default_data is False:
        logger.info(f"Default table {default_table} does not have data")
        return

      (
        get_min_date,
        get_max_date,
      ) = self.get_min_max_data_from_parent_partition(
        logger, table, default_table, cur
      )

      for i in range(get_min_date, get_max_date + 1):
        year = i
        wschema_child_table = f"{table['name']}_{year}"
        child_table = f'"{table["schema"]}".{wschema_child_table}'

        logger.info(f"Checking if table exist for year: {i}")

        check_tb_exist = self.check_table_exists.format(
          a=f"{table['name']}_{i}", b=table["schema"]
        )

        cur.execute(check_tb_exist)

        tb_exist = cur.fetchone()[0]

        if tb_exist:
          logger.info(f"Table {child_table} already exist")
          continue
        else:
          logger.info(f"Creating table for: {child_table}")
          create_table = self.create_inherit_table.format(
            a=child_table, b=default_table
          )

          cur.execute(create_table)

          conn.commit()

          self.data_migration(
            logger,
            year,
            table,
            cur,
            conn,
            default_table,
            child_table,
            wschema_default_table,
            table["name"],
            False,
          )

          get_latest_id_from_child_table = self.get_max_table_new.format(
            a=table["pkey"], b=child_table
          )
          cur.execute(get_latest_id_from_child_table)

          last_processed_id = cur.fetchone()

          if last_processed_id is None:
            last_processed_id = 0
          else:
            last_processed_id = last_processed_id[0]

          logger.info(
            f"Last process id from {child_table}: {last_processed_id}"
          )

          logger.info("Get latest max id from parent table")
          get_latest_max_id = self.get_max_conditional_table_new.format(
            a=table["pkey"],
            b=default_table,
            c=table["partition"],
            d=year,
            e=year + 1,
          )
          cur.execute(get_latest_max_id)

          parent_max_id = cur.fetchone()[0]

          logger.info(f"Max id from {default_table}: {parent_max_id}")

          if parent_max_id != last_processed_id:
            logger.info(
              f"""Data not fully migrated yet for {
                child_table
              }, due to data mismatch"""
            )
            continue
          else:
            logger.info("Locking table to prevent any write")
            locking = self.lock_table.format(a=table["name"])
            cur.execute(locking)

            detach_default_table = self.detach_partition_new.format(
              a=table["name"],
              b=default_table,
            )

            cur.execute(detach_default_table)

            attached_child_table = self.alter_table_add_partition.format(
              a=table["name"],
              b=child_table,
              c=year,
              d=year + 1,
            )

            cur.execute(attached_child_table)

            logger.info("Removing copied data from default table")
            cleanup_default_table = self.delete_row_from_table.format(
              a=default_table, b=table["partition"], c=year, d=year + 1
            )

            cur.execute(cleanup_default_table)

            logger.info("Attached back default table")

            attach_default_table = (
              self.attach_table_as_default_partition.format(
                a=table["name"],
                b=default_table,
              )
            )

            cur.execute(attach_default_table)

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
      logger.info(f"""Complete migration for table {table['name']}""")
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    p = Process(
      target=self.maintenance_partition,
      args=(
        table,
        database_config,
        application_name,
      ),
    )

    return p


if __name__ == "__main__":
  batchrun = MaintenanceMigration()
  batchrun.main()
  batchrun.cleanup_cronjob_lock()
