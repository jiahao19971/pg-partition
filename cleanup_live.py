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

      insert_col = []
      value_col = []
      for col in table["column"].keys():
        if "default" not in table["column"][col].lower():
          insert_col.append(col)
          value_col.append(f"NEW.{col}")

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

      get_latest_id_from_table = self.get_max_with_coalesce.format(
        a=table["pkey"], b=parent_table
      )

      logger.info(f"Get latest id from parent table: {parent_table}")
      cur.execute(get_latest_id_from_table)

      last_processed_id = cur.fetchone()[0]
      logger.debug(f"Last processed id for {parent_table}: {last_processed_id}")

      logger.info(
        f"Get latest max id from partitioned table: {new_partition_parent}"
      )
      get_latest_max_id = self.get_max_with_coalesce.format(
        a=table["pkey"], b=new_partition_parent
      )
      cur.execute(get_latest_max_id)

      parent_max_id = cur.fetchone()[0]
      logger.debug(
        f"Last processed id for {new_partition_parent}: {parent_max_id}"
      )

      if last_processed_id != parent_max_id:
        logger.info("Parent table has new data")
        logger.info("Skipping table for completion step")
        return

      cur.execute(
        f"""
        CREATE OR REPLACE FUNCTION move_to_partitioned()
          RETURNS trigger AS
          $$
          BEGIN
            IF TG_OP = 'INSERT' THEN
              INSERT INTO {new_partition_parent} ({','.join(insert_col)})
                VALUES ({','.join(value_col)});
            END IF;
          RETURN NEW;
          END;
          $$
          LANGUAGE 'plpgsql';

        DO
          $$BEGIN
            CREATE TRIGGER view_trigger
              INSTEAD OF INSERT OR UPDATE OR DELETE ON {table['name']}
              FOR EACH ROW
              EXECUTE FUNCTION move_to_partitioned();
          EXCEPTION
            WHEN duplicate_object THEN
                NULL;
          END;$$;
      """
      )

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
