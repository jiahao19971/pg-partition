"""
    Vacuumdb module is used to run analyze in stages to the
    database
    It utilize subprocess to run vacuumdb to the database.
    Additional feature: it is able to perform tunneling to the database
"""
import os
import subprocess
import sys

from dotenv import load_dotenv

from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
from db.db import get_db
from tunnel.tunnel import get_tunnel

load_dotenv()


class VacuumDB(PartitionCommon):
  """
  Vacuumdb run analyze in stages to the database

  Args:
    No args needed

  Returns:
    Result from vacuumdb
  """

  def _create_db_url(self, new_conn, database_config):
    db_name = new_conn["dbname"]
    db_user = new_conn["user"]
    db_host = new_conn["host"]
    db_port = new_conn["port"]

    user_config = {
      "name": db_name,
      "user": db_user,
      "host": db_host,
      "port": db_port,
    }

    if "password" in new_conn:
      user_config["password"] = database_config["db_password"]

    return user_config

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    db_identifier = database_config["db_identifier"]
    self.logger = self.logging_func(application_name=application_name)

    server = get_tunnel(database_config)
    db_conn = get_db(server, database_config, application_name)

    conn = db_conn.connect()
    self.logger.debug(f"Connected: {db_identifier}")

    split_string_conn = conn.dsn.split(" ")

    new_conn = {}
    for conn_part in split_string_conn:
      splitter = conn_part.split("=")
      new_conn[splitter[0]] = splitter[1]

    config = self._create_db_url(new_conn, database_config)

    with subprocess.Popen(
      [
        "vacuumdb",
        "-p",
        config["port"],
        "-d",
        config["name"],
        "-U",
        config["user"],
        "-h",
        config["host"],
        "-e",
        "-j",
        "100",
        "--analyze-in-stages",
      ],
      env=dict(os.environ, PGPASSWORD=config["password"]),
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
    ) as process:

      for line in process.stdout:
        self.logger.debug(line.decode("utf-8").strip())

      output = process.communicate()[0]

      if process.returncode != 0:
        self.logger.error(f"Command failed. Return code : {process.returncode}")
        sys.exit()
      else:
        self.logger.info(output)

    conn.close()


if __name__ == "__main__":
  VacuumDB().main()
