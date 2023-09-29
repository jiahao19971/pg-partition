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
from multiprocessing import Process

from psycopg2 import Error
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.wrapper import get_config_n_secret
from db.db import get_db
from microbatching import MicrobatchMigration
from tunnel.tunnel import get_tunnel


class CombineMigration(MicrobatchMigration):
  """
  Partition class is used to perform partitioning in postgres database
  The main function is the entry point of the class

  Args:
      No args needed

  Returns:
      No returns
  """

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

      parent_table = f'"{table["schema"]}".{table["name"]}'

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

      cur.execute(f"SELECT count(*) FROM {parent_table};")

      parent_count = cur.fetchone()[0]

      cur.execute(f"SELECT count(*) FROM {parent_table}_temp;")

      parent_table_count = cur.fetchone()[0]

      if parent_count == parent_table_count:
        logger.info("Lock table to prevent any write")
        cur.execute(f"LOCK TABLE {parent_table} IN ACCESS EXCLUSIVE MODE;")

        logger.info(f"Drop parent table {parent_table}")
        cur.execute(f"DROP TABLE {parent_table}")

        logger.info(f"Rename temp table to {parent_table}")
        cur.execute(
          f"ALTER TABLE {parent_table}_temp RENAME TO {table['name']};"
        )

        logger.info(f"Rename constraint to {parent_table}_pkey")
        cur.execute(
          f"""
            ALTER TABLE {parent_table}
            RENAME CONSTRAINT {table['name']}_temp_pkey TO {table['name']}_pkey;
          """
        )

        conn.commit()
      else:
        logger.warning(f"Table count not equal, skipping table {parent_table}")
        logger.warning(
          "Waiting for the microbatch to complete, will retry again later"
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
      logger.info(f"""Complete migration for table {parent_table}""")
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
  batchrun = CombineMigration()
  batchrun.main()
