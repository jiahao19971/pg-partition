"""
    Reverse_partition module is used to reverse partition table to normal table
    It utilize psycopy2 to connect to the database and execute the query
    Additional feature: it is able to perform tunneling to the database
"""
from dotenv import load_dotenv
from psycopg2 import Error
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
from db.db import get_db
from tunnel.tunnel import get_tunnel

load_dotenv()


class ReversePartition(PartitionCommon):
  """
  ReversePartition class is used to undo the partitioning process
  The main function is the entry point of the class

  Args:
    No args needed

  Returns:
    No returns
  """

  def check_combine_table(self, table, cur):
    checker = self.table_check_like.format(
      a=rf'{table["name"]}\_%', b=table["schema"]
    )
    cur.execute(checker)
    data = cur.fetchall()

    table_to_be_combine = []
    for tabs in data:
      table_to_be_combine.append(list(tabs)[1])

    return table_to_be_combine

  def reverse_partition(self, table, database_config, application_name):
    try:
      db_identifier = database_config["db_identifier"]
      logger = self.logging_func(application_name=application_name)

      server = get_tunnel(database_config)
      conn = get_db(server, database_config, application_name)

      conn = conn.connect()
      logger.debug(f"Connected: {db_identifier}")
      cur = conn.cursor()

      qry = self.set_search_path.format(a=table["schema"])
      logger.info(qry)
      cur.execute(f"{qry};")

      partitioning = self.reverse_check_table_partition(table, cur)

      if partitioning:
        logger.info(
          f"""Reverting table from partition to normal table: {
            table['schema']
          }.{
            table['name']
          }"""
        )

        collist, colname = self._get_column(table)

        table_combine = self.check_combine_table(table, cur)

        create_table = self.create_normal_table.format(
          a=f"{table['name']}_new",
          b=", ".join(collist),
          c=table["pkey"],
        )

        cur.execute(create_table)

        for tab in table_combine:
          logger.info(f"Moving data from {tab} to {table['name']}_new")
          insertation = self.insert_data_to_table.format(
            a=f"{table['name']}_new",
            b=",".join(colname),
            c=tab,
          )

          cur.execute(insertation)

        if len(table_combine) > 0:
          get_sequence = self.get_sequence_like_value.format(
            a=table["name"],
            b=table["schema"],
          )

          cur.execute(get_sequence)

          seq = cur.fetchall()

          drop_partitioning = self.drop_table_cascade.format(a=table["name"])

          alter_name = self.rename_table.format(
            a=f"{table['name']}_new", b=table["name"]
          )

          alter_idx = self.alter_index_rename.format(
            a=f"{table['name']}_new_pkey", b=f"{table['name']}_pkey"
          )

          run_analyze = self.analyze_table.format(a=table["name"])

          cur.execute(drop_partitioning)
          logger.debug("Rename table to original table name")
          cur.execute(alter_name)
          logger.debug("Rename index to original table")
          cur.execute(alter_idx)
          logger.debug("Running analyze")
          cur.execute(run_analyze)

          if "additional_index_name" in table:
            idx_col_list = []
            for idx_col in list(table["additional_index_name"].keys()):
              newval = self.create_normal_index.format(
                a=idx_col,
                b=table["name"],
                c=table["additional_index_name"][idx_col],
              )

              idx_col_list.append(newval)

            for idx in idx_col_list:
              cur.execute(idx)

          for sequence in seq:
            create_sequence = self.create_sequece_if_not_exists.format(
              a=table["schema"], b=sequence[0], c=sequence[1], d=sequence[2]
            )

            update_sequence = self.set_sequence_last_val.format(
              a=table["schema"], b=sequence[0], c=sequence[3]
            )

            change_owner = self.alter_table_owner.format(
              a=f'"{table["schema"]}".{table["name"]}'
            )

            change_sequence_owner = self.alter_sequence_owner.format(
              a=f'"{table["schema"]}".{sequence[0]}'
            )

            add_sequence_back = self.alter_table_set_default_val.format(
              a=table["schema"], b=table["name"], c=table["pkey"], d=sequence[0]
            )

            change_sequence_ownership = self.alter_sequence_owned_by.format(
              a=f'"{table["schema"]}".{sequence[0]}',
              b=f"{table['name']}.{table['pkey']}",
            )

            logger.debug("Create sequence for table")
            cur.execute(create_sequence)
            logger.debug("Update sequence last value")
            cur.execute(update_sequence)
            logger.debug("Change table owner to postgres")
            cur.execute(change_owner)
            logger.debug("Change sequence owner to postgres")
            cur.execute(change_sequence_owner)
            logger.debug("Change sequence ownership back to original table")
            cur.execute(change_sequence_ownership)
            logger.debug("Add sequence back to original table")
            cur.execute(add_sequence_back)
        logger.info("Completed reversing partitioning")
        conn.commit()
        conn.close()
      else:
        logger.info(
          f"""No reversing partition needed for table: {
            table['schema']
            }.{
            table['name']
            }"""
        )
        conn.close()
    except BaseSSHTunnelForwarderError as e:
      self.logger.error(e)
      conn.rollback()
      conn.close()
    except Error as opte:
      self.print_psycopg2_exception(opte)
      conn.rollback()
      conn.close()
    finally:
      logger.info(
        f"""Successfully revert partition changes on table: {
          table['schema']
        }.{
          table['name']
        }"""
      )
      if isinstance(server, SSHTunnelForwarder):
        server.stop()

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    self.reverse_partition(table, database_config, application_name)


if __name__ == "__main__":
  reverse = ReversePartition()
  reverse.main()
