import os
import re

from dotenv import load_dotenv
from ruamel.yaml import YAML
from sshtunnel import SSHTunnelForwarder

from common.common import PartitionCommon
from common.query import (
  alter_replica_identity,
  alter_sequence_owned_by,
  alter_sequence_owner,
  alter_table_owner,
  attach_table_as_default_partition,
  create_table_with_partitioning,
  get_table_index,
  rename_table,
  set_search_path,
)
from common.wrapper import background, get_config_n_secret
from db.db import _get_db
from tunnel.tunnel import _get_tunnel

yaml = YAML()
yaml.preserve_quotes = True

load_dotenv()


class Partition(PartitionCommon):
  def __init__(self) -> None:
    super().__init__()

  def additional_index_reverse_partitioning(self, index, table):
    application_name = f"{table['schema']}.{table['name']}"
    logger = self.logging_func(application_name=application_name)
    if re.search(f"\\b{table['pkey']}", index) == None:
      get_idx = index.split("USING")

      get_idx_name = get_idx[0].split("ON ")

      idx_name = get_idx_name[0].replace("CREATE INDEX ", "")
      idx_name = idx_name.replace(" ", "")

      get_tb_schema = get_idx_name[1].split(".")

      new_yaml_data_dict = {idx_name: f"USING {(get_idx[1])}"}
      configfile = "config.yaml"
      if os.environ["ENV"] == "staging":
        configfile = "config.staging.yaml"

      with open(configfile, "r") as yamlfile:
        cur_yaml = yaml.load(yamlfile)

        for table in cur_yaml["table"]:
          if (
            table["name"] in get_tb_schema[1]
            and table["schema"] in get_tb_schema[0]
          ):
            logger.info("Adding additional index for reverse partitioning")
            table["additional_index_name"] = {}
            table["additional_index_name"].update(new_yaml_data_dict)

      with open(configfile, "w") as yamlfile:
        yaml.dump(cur_yaml, yamlfile)

  def change_owner_on_index_table(self, table, cur):
    change_owner = alter_table_owner.format(a=table["name"])
    change_owner_sequence = alter_sequence_owner.format(a=table["name"])
    change_sequence_ownership = alter_sequence_owned_by.format(
      a=table["name"], b=f"{table['name']}.{table['pkey']}"
    )

    self.logger.info("Change sequence ownership")
    cur.execute(change_owner)
    cur.execute(change_owner_sequence)
    cur.execute(change_sequence_ownership)

  def create_partitioning(self, collist, table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = self.logging_func(application_name=application_name)

    alter_tablename = rename_table.format(
      a=table["name"], b=f"{table['name']}_old"
    )

    partitioning = create_table_with_partitioning.format(
      a=table["name"],
      b=", ".join(collist),
      c=table["partition"],
      d=f"{table['pkey']}, {table['partition']}",
    )

    add_as_default = attach_table_as_default_partition.format(
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
    get_index_def = get_table_index.format(
      a="indexdef, indexname", b=table["name"], c=table["schema"]
    )
    cur.execute(get_index_def)

    index_data = cur.fetchall()

    if len(index_data) > 0:
      index_status = [
        index[0] for index in index_data if table["partition"] in index[0]
      ]

      ## Keeping this as it is a bug from postgres 10 and 11 which partition index wont show in pg_indexes thus required to call from pg_class
      check_partition_table_index = f"""
                select i.relname as indexname
                from pg_class i
                    join pg_index idx on idx.indexrelid = i.oid
                    join pg_class t on t.oid = idx.indrelid
                    join pg_namespace n ON n.oid = t.relnamespace
                where i.relkind = 'I'
                    and t.relname = '{table['name']}'
                    and n.nspname = '{table['schema']}';
            """

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

  @background
  def perform_partitioning(self, table, database_config, application_name):
    db_identifier = database_config["db_identifier"]
    logger = self.logging_func(application_name=application_name)

    server = _get_tunnel(database_config)
    conn = _get_db(server, database_config, application_name)
    logger.debug(f"Connected: {db_identifier}")

    conn = conn.connect()
    cur = conn.cursor()
    try:
      set_searchPath = set_search_path.format(a=table["schema"])
      logger.debug(set_searchPath)
      cur.execute(set_searchPath)

      index_data = self.get_index_required(table, cur)

      change_replica_identity = alter_replica_identity.format(a=table["name"])
      cur.execute(change_replica_identity)

      partitioning = self.check_table_partition(table, cur)

      collist, _ = self._get_column(table)

      if partitioning:
        alter_table_partition_key_to_not_null = f"""
                    ALTER TABLE "{table['schema']}".{table['name']} ALTER COLUMN {table['partition']} SET NOT NULL;
                """
        self.logger.debug("Alter table partition column with not null")
        cur.execute(alter_table_partition_key_to_not_null)

        alter_old_table_pkey = f"""
                    ALTER TABLE "{table['schema']}".{table['name']}
                    DROP CONSTRAINT {table['name']}_pkey,
                    ADD PRIMARY KEY ({table['pkey']}, {table['partition']})
                """

        self.logger.debug("Added table primary key with partition column")
        cur.execute(alter_old_table_pkey)

        self.create_partitioning(collist, table, cur)

        self.change_owner_on_index_table(table, cur)

        for index in index_data:
          idx_query = index[0]
          idx_name = index[1]

          logger.debug("Renaming index {a} to {a}_old".format(a=idx_name))
          alter_idx = f"ALTER INDEX {idx_name} RENAME TO {idx_name}_old;"
          cur.execute(alter_idx)

          if idx_name == f"{table['name']}_pkey":
            partition_table_idx = f"CREATE UNIQUE INDEX {table['name']}_pkey ON ONLY {table['name']} USING btree ({table['pkey']}, {table['partition']});"
          else:
            partition_table_idx = f"{idx_query}".replace("ON", "ON ONLY")

          cur.execute(partition_table_idx)

          self.additional_index_reverse_partitioning(idx_query, table)

        conn.commit()
        conn.close()

        new_conn = _get_db(server, database_config, application_name)
        new_conn = new_conn.connect()
        new_conn.autocommit = True
        new_cur = new_conn.cursor()

        set_searchPath = set_search_path.format(a=table["schema"])
        logger.debug(set_searchPath)
        new_cur.execute(set_searchPath)

        new_idx_name = (
          f'{table["name"]}_old_{table["pkey"]}_{table["partition"]}_idx'
        )

        logger.debug(
          f"Creating new unique index concurrently for {new_idx_name}"
        )
        new_cur.execute(
          f'CREATE UNIQUE INDEX CONCURRENTLY {new_idx_name} ON {table["name"]}_old USING btree ({table["pkey"]}, {table["partition"]});'
        )

        logger.debug(
          f"Attaching index {new_idx_name} to partition index {table['name']}_pkey"
        )
        new_cur.execute(
          f"ALTER INDEX {table['name']}_pkey ATTACH PARTITION {new_idx_name};"
        )

        if "additional_index_name" in table:
          idxColList = []
          for idx_col in list(table["additional_index_name"].keys()):
            d = re.findall(
              "(?<=\().+?(?=\))", table["additional_index_name"][idx_col]
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
            newval = f"CREATE INDEX CONCURRENTLY {addon_new_idx_name} ON {table['name']}_old {table['additional_index_name'][idx_col]};"

            logger.debug(
              f"Attaching index {addon_new_idx_name} to partition index {idx_col}"
            )
            newalter = (
              f"ALTER INDEX {idx_col} ATTACH PARTITION {addon_new_idx_name};"
            )

            idxColList.append(newval)
            idxColList.append(newalter)

          for idx in idxColList:
            new_cur.execute(idx)
        new_conn.close()
      else:
        logger.info(
          f"No partitioning needed, as table already partition: {table['schema']}.{table['name']}"
        )
        conn.close()
      if type(server) == SSHTunnelForwarder:
        server.stop()
    except Exception as e:
      logger.error(e)
      logger.error("Error occured while partitioning, rolling back")
      conn.rollback()
      conn.close()

  @get_config_n_secret
  def main(self, table, database_config, application_name):
    self.perform_partitioning(table, database_config, application_name)


if __name__ == "__main__":
  runner = Partition()
  runner.main()
