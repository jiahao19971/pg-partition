"""
    Partition module to perform partitioning in postgres database
    It utilize asycio background task to perform partitioning
    with renameing existing table to table_old and create table with partition
    and attached the table_old to the new table as default partition

    Besides, the module also perform additional index for the primary key and
    insert as composite key for the partition table
"""
import os
import re
from multiprocessing import Process

from dotenv import load_dotenv
from psycopg2 import Error
from ruamel.yaml import YAML
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
from db.db import get_db
from tunnel.tunnel import get_tunnel

yaml = YAML()
yaml.preserve_quotes = True

load_dotenv()


class Partition(PartitionCommon):
  """
  Partition class is used to perform partitioning in postgres database
  The main function is the entry point of the class

  Args:
    No args needed

  Returns:
    No returns
  """

  def additional_index_reverse_partitioning(self, index, table):
    application_name = f"{table['schema']}.{table['name']}"
    logger = self.logging_func(application_name=application_name)
    if re.search(f"\\b{table['pkey']}", index) is None:
      get_idx = index.split("USING")

      get_idx_name = get_idx[0].split("ON ")

      idx_name = get_idx_name[0].replace("CREATE INDEX ", "")
      idx_name = idx_name.replace(" ", "")

      get_tb_schema = get_idx_name[1].split(".")

      new_yaml_data_dict = {idx_name: f"USING {(get_idx[1])}"}
      configfile = "config.yaml"
      if "ENV" in os.environ and os.environ["ENV"] == "staging":
        configfile = "config.staging.yaml"

      with open(configfile, "r", encoding="utf-8") as yamlfile:
        cur_yaml = yaml.load(yamlfile)

        for tabs in cur_yaml["table"]:
          if (
            tabs["name"] in get_tb_schema[1]
            and tabs["schema"] in get_tb_schema[0]
          ):
            logger.info("Adding additional index for reverse partitioning")
            tabs["additional_index_name"] = {}
            tabs["additional_index_name"].update(new_yaml_data_dict)

      with open(configfile, "w", encoding="utf-8") as yamlfile:
        yaml.dump(cur_yaml, yamlfile)

  def change_owner_on_index_table(self, table, cur):
    change_owner = self.alter_table_owner.format(a=table["name"])
    change_owner_sequence = self.alter_sequence_owner.format(
      a=f'{table["name"]}_id_seq'
    )
    change_sequence_ownership = self.alter_sequence_owned_by.format(
      a=f"{table['name']}_id_seq", b=f"{table['name']}.{table['pkey']}"
    )

    self.logger.info("Change sequence ownership")
    cur.execute(change_owner)
    cur.execute(change_owner_sequence)
    cur.execute(change_sequence_ownership)

  def create_partitioning(self, collist, table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = self.logging_func(application_name=application_name)

    alter_tablename = self.rename_table.format(
      a=table["name"], b=f"{table['name']}_old"
    )

    partitioning = self.create_table_with_partitioning.format(
      a=table["name"],
      b=", ".join(collist),
      c=table["partition"],
      d=f"{table['pkey']}, {table['partition']}",
    )

    add_as_default = self.attach_table_as_default_partition.format(
      a=table["name"], b=f"{table['name']}_old"
    )

    logger.info(f"Creating partition for {table['schema']}.{table['name']}")
    logger.debug("Altering table name")
    cur.execute(alter_tablename)
    logger.debug("Create partitioning")
    cur.execute(partitioning)
    logger.debug("Attach table as default partition")
    cur.execute(add_as_default)

  def get_index_required(self, table, cur):
    get_index_def = self.get_table_index.format(
      a="indexdef, indexname", b=table["name"], c=table["schema"]
    )
    cur.execute(get_index_def)

    index_data = cur.fetchall()

    if len(index_data) > 0:
      index_status = [
        index[0] for index in index_data if table["partition"] in index[0]
      ]

      ## Keeping this as it is a bug
      ## from postgres 10 and 11
      ## which partition index wont show
      ## in pg_indexes thus required to call from pg_class
      check_partition_table_index = self.get_index_from_pg_class.format(
        a=table["name"], b=table["schema"]
      )

      cur.execute(check_partition_table_index)

      partition_index = cur.fetchall()

      if len(partition_index) == 0:
        partition_index = []
      else:
        partition_index = [idx[0] for idx in partition_index]

      new_index = []
      if len(index_status) == 0 and len(partition_index) == 0:
        for index in index_data:
          if f"({table['pkey']})" in index[0]:
            new_ = index[0].replace(
              table["pkey"], f"{table['pkey']}, {table['partition']}"
            )
            new_index.append((new_, index[1]))
          else:
            new_index.append(index)
      return new_index

    return index_data

  # @background
  def perform_partitioning(self, table, database_config, application_name):
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

      index_data = self.get_index_required(table, cur)

      change_replica_identity = self.alter_replica_identity.format(
        a=table["name"]
      )
      cur.execute(change_replica_identity)

      partitioning = self.check_table_partition(table, cur)

      collist, _ = self._get_column(table)

      if partitioning:
        # alter_table_partition_key_to_not_null = (
        #   self.alter_column_not_null.format(
        #     a=table["schema"], b=table["name"], c=table["partition"]
        #   )
        # )

        # self.logger.debug("Alter table partition column with not null")
        # cur.execute(alter_table_partition_key_to_not_null)

        # alter_old_table_pkey = (
        #   self.alter_table_drop_constraint_add_primary.format(
        #     a=table["schema"],
        #     b=table["name"],
        #     c=table["pkey"],
        #     d=table["partition"],
        #   )
        # )

        # self.logger.debug("Added table primary key with partition column")
        # cur.execute(alter_old_table_pkey)

        self.create_partitioning(collist, table, cur)

        self.change_owner_on_index_table(table, cur)

        for index in index_data:
          idx_query = index[0]
          idx_name = index[1]

          logger.debug(f"Renaming index {idx_name} to {idx_name}_old")
          alter_idx = self.alter_index_rename.format(
            a=idx_name, b=f"{idx_name}_old"
          )
          cur.execute(alter_idx)

          if idx_name == f"{table['name']}_pkey":
            partition_table_idx = self.create_unique_index.format(
              a=f"{table['name']}_pkey",
              b=table["name"],
              c=table["pkey"],
              d=table["partition"],
            )
          else:
            partition_table_idx = f"{idx_query}".replace("ON", "ON ONLY")

          cur.execute(partition_table_idx)

          self.additional_index_reverse_partitioning(idx_query, table)

        conn.commit()
        conn.close()

        new_conn = get_db(server, database_config, application_name)
        new_conn = new_conn.connect()
        new_conn.autocommit = True
        new_cur = new_conn.cursor()

        logger.debug(search_path)
        new_cur.execute(search_path)

        new_idx_name = (
          f'{table["name"]}_old_{table["pkey"]}_{table["partition"]}_idx'
        )

        logger.debug(
          f"Creating new unique index concurrently for {new_idx_name}"
        )

        concurrent_unique_index = self.create_unique_index_concurrently.format(
          a=new_idx_name,
          b=f"{table['name']}_old",
          c=table["pkey"],
          d=table["partition"],
        )

        new_cur.execute(concurrent_unique_index)

        logger.debug(
          f"""Attaching index {
            new_idx_name
          } to partition index {
            table['name']
          }_pkey"""
        )

        reattach_partition = self.alter_index_attach_partition.format(
          a=f"{table['name']}_pkey", b=new_idx_name
        )

        new_cur.execute(reattach_partition)

        if "additional_index_name" in table:
          idx_col_list = []
          for idx_col in list(table["additional_index_name"].keys()):
            d = re.findall(
              r"(?<=\().+?(?=\))", table["additional_index_name"][idx_col]
            )

            d = d[0]

            d = d.split(",")

            d = [x.replace(" ", "") for x in d]

            if len(d) > 1:
              d = "_".join(d)
            else:
              d = d[0]

            addon_new_idx_name = f"{table['name']}_old_{d}_idx"

            logger.debug(
              f"Creating new index concurrently for {addon_new_idx_name}"
            )
            newval = self.create_index_concurrently.format(
              a=addon_new_idx_name,
              b=f"{table['name']}_old",
              c=table["additional_index_name"][idx_col],
            )

            logger.debug(
              f"""Attaching index {
                  addon_new_idx_name
                } to partition index {
                  idx_col
                }"""
            )
            newalter = self.alter_index_attach_partition.format(
              a=idx_col, b=addon_new_idx_name
            )

            idx_col_list.append(newval)
            idx_col_list.append(newalter)

          for idx in idx_col_list:
            new_cur.execute(idx)
        new_conn.close()
      else:
        logger.info(
          f"""No partitioning needed, as table already partition: {
            table['schema']
            }.{
              table['name']
            }"""
        )
        conn.close()

    except BaseSSHTunnelForwarderError as e:
      self.logger.error(f"{db_identifier}: {e}")
      conn.rollback()
      conn.close()
    except Error as opte:
      self.print_psycopg2_exception(opte)
      conn.rollback()
      conn.close()
    finally:
      logger.info(
        f"""Successfully partition table: {
          table['schema']
        }.{
          table['name']
        }"""
      )
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    p = Process(
      target=self.perform_partitioning,
      args=(
        table,
        database_config,
        application_name,
      ),
    )

    return p


if __name__ == "__main__":
  runner = Partition()
  runner.main()
