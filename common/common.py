"""
    PartitionCommon module is used as
    the parent class for partitioning

    It is used to specify the common function
    required for the partitioning process
"""
import json
import os
import subprocess
import sys
from functools import lru_cache

from cerberus import Validator
from ruamel.yaml import YAML, YAMLError

from common.query import PartitionQuery
from common.validator import PartitioningValidator

yaml = YAML()

yaml.preserve_quotes = True


class PartitionCommon(PartitionQuery):
  """
  PartitionCommon class is used as
  the parent class for partitioning

  The main class specify the common function
  required for the partitioning process

  Args:
    No args needed

  Returns:
    No returns
  """

  def __init__(self):
    super().__init__()
    self.n_of_batch_default = self.batch_size()

  def reverse_check_table_partition(self, table, cur):
    checker = self.table_check.format(a=table["name"], b=table["schema"])
    cur.execute(checker)
    data = cur.fetchall()

    partition = bool("partitioned table" in list(data[0]))

    return partition

  def print_psycopg2_exception(self, err):
    err_type, _, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno

    self.logger.error(f"psycopg2 ERROR: {err} on line number: {line_num}")
    self.logger.error(f"psycopg2 traceback: {traceback} -- type: {err_type}")

    # psycopg2 extensions.Diagnostics object attribute
    self.logger.error(f"extensions.Diagnostics: {err.diag}")

    # print the pgcode and pgerror exceptions
    self.logger.error(f"pgerror: {err.pgerror}")
    self.logger.error(f"pgcode: {err.pgcode}")

  @lru_cache
  def _open_config(self, config_name="config.yaml"):
    with open(config_name, "r", encoding="utf-8") as stream:
      try:
        data = yaml.load(stream)
        with open("config.json", "r", encoding="utf-8") as validation_rules:
          schema = json.load(validation_rules)
          v = Validator(schema)
          if v.validate(data, schema):
            self.logger.debug(
              "Validated config.yml and no issue has been found"
            )
            return data
          else:
            raise ValueError(v.errors)
      except ValueError as e:
        raise e
      except YAMLError as yamlerr:
        if hasattr(yamlerr, "problem_mark"):
          pm = yamlerr.problem_mark
          message = "Your file {} has an issue on line {} at position {}"
          format_message = message.format(pm.name, pm.line, pm.column)
          raise ValueError(format_message) from yamlerr
        else:
          message = "Something went wrong while parsing config.yaml file"
        raise ValueError(message) from yamlerr

  @lru_cache
  def _open_secret(self, secret_name="secret.yaml"):
    with open(secret_name, "r", encoding="utf-8") as stream:
      try:
        data = yaml.load(stream)
        with open("secret.json", "r", encoding="utf-8") as validation_rules:
          schema = json.load(validation_rules)
          v = PartitioningValidator(schema)
          if v.validate(data, schema):
            self.logger.debug(
              "Validated secret.yml and no issue has been found"
            )
            return data
          else:
            raise ValueError(v.errors)
      except ValueError as e:
        raise e
      except YAMLError as yamlerr:
        if hasattr(yamlerr, "problem_mark"):
          pm = yamlerr.problem_mark
          message = "Your file {} has an issue on line {} at position {}"
          format_message = message.format(pm.name, pm.line, pm.column)
          raise ValueError(format_message) from yamlerr
        else:
          message = "Something went wrong while parsing config.yaml file"
          raise ValueError(message) from yamlerr

  def checker_table(self, table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = self.logging_func(application_name=application_name)
    checker = self.table_check.format(a=f'{table["name"]}', b=table["schema"])

    logger.debug("Checking table if it is partition")
    cur.execute(checker)
    data = cur.fetchall()

    return data

  def checker_temp_table(self, table, cur):
    application_name = f"{table['schema']}.{table['name']}"
    logger = self.logging_func(application_name=application_name)
    checker = self.table_check.format(
      a=f'{table["name"]}_temp', b=table["schema"]
    )

    logger.debug("Checking table if it is partition")
    cur.execute(checker)
    data = cur.fetchall()

    return data

  def check_table_partition(self, table, cur):
    data = self.checker_table(table, cur)
    partition = bool("partitioned table" not in list(data[0]))

    if partition is True:
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

      if table_exist is True:
        new_data = self.checker_temp_table(table, cur)
        partition = bool("partitioned table" not in list(new_data[0]))

    return partition

  def get_config(self):
    if "ENV" in os.environ and os.environ["ENV"] == "staging":
      configfile = "config.staging.yaml"
    else:
      configfile = "config.yaml"
    config = self._open_config(configfile)

    return config

  def get_secret(self):
    secret = self._open_secret("secret.yaml")

    return secret

  def _get_column(self, table):
    collist = []
    colname = []

    for columnname in list(table["column"].keys()):
      newval = f"{columnname} {table['column'][columnname]}"
      collist.append(newval)
      colname.append(columnname)

    return collist, colname

  def batch_size(self):
    n_of_batch_default = 1000
    try:
      batch = (
        int(os.environ["BATCH_SIZE"])
        if "BATCH_SIZE" in os.environ
        else n_of_batch_default
      )
    except ValueError:
      self.logger.debug.debug(
        f"BATCH_SIZE is not an integer, defaulting to {n_of_batch_default}"
      )
      batch = n_of_batch_default

    return batch

  def create_trigger_column(self, column):
    insert_col = []
    value_col = []
    update_col = []
    update_val_col = []
    for col in column.keys():
      update_col.append(col)
      if "default" not in column[col].lower():
        insert_col.append(col)
        value_col.append(f"NEW.{col}")
        update_val_col.append(f"NEW.{col}")
      else:
        update_val_col.append(f"OLD.{col}")

    return insert_col, value_col, update_col, update_val_col

  def cleanup_cronjob_lock(self):
    if "DEPLOYMENT" in os.environ and os.environ["DEPLOYMENT"] == "kubernetes":
      with subprocess.Popen(
        ["kubectl", "delete", "cm/cronjob-lock", "-n", "partitioning"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
      ) as process:
        for line in process.stdout:
          self.logger.info(line.decode("utf-8").strip())

        output = process.communicate()[0]

        if process.returncode != 0:
          self.logger.info(
            f"""Command failed. Return code : {
            process.returncode
          }"""
          )
        else:
          self.logger.info(output)
