"""
    DB module is used to connect to postgres database
"""
import os

import psycopg2
from dotenv import load_dotenv
from psycopg2 import Error
from sshtunnel import SSHTunnelForwarder

from common.common import PartitionCommon

load_dotenv()


class DBLoader(PartitionCommon):
  """
  DBLoader class is used to connect to postgres database
  The connect function is the entry point of the class

  Args:
    [server]: dict[local_bind_host, local_bind_port] || SSHTunnelForwarder,
    [database]: str,
    [application_name]: str = "DB_Loader" (optional),
    [db_user]: str = False (optional),
    [db_pass]: str = False (optional),
    [db_sslmode]: str = False (optional),
    [db_sslrootcert]: str = False (optional),
    [db_sslcert]: str = False (optional),
    [db_sslkey]: str = False (optional),

  Returns:
    psycopg2.connect(**params)
  """

  env_string = (
    "Environment variable %s was not found/have issue, "
    "switching back to default value: %s"
  )

  def __init__(
    self,
    server,
    database,
    application_name="DB_Loader",
    db_user: str = False,
    db_pass: str = False,
    db_sslmode: str = False,
    db_sslrootcert: str = False,
    db_sslcert: str = False,
    db_sslkey: str = False,
  ):
    super().__init__()
    self.logger = self.logging_func(f"DB_{database}")
    self.logger.info(f"Database Initialize: {database}")

    if isinstance(server, SSHTunnelForwarder):
      self.logger.info(
        f"Database {database} Initialize: {server.local_bind_host}"
      )
    else:
      self.logger.info(
        f"Database {database} Initialize: {server['local_bind_host']}"
      )

    self.db_user = self._get_database_username(db_user)
    self.db_pass = self._get_database_password(db_pass)
    self.database = database
    self.server = server
    self.application_name = application_name
    self.db_sslmode = db_sslmode
    self.db_sslrootcert = db_sslrootcert
    self.db_sslcert = db_sslcert
    self.db_sslkey = db_sslkey

  def _get_database_username(self, db_user):
    try:
      if db_user is not False:
        username: str = db_user
      else:
        username: str = os.environ["USERNAME"]
      self.logger.debug("Environment variable USERNAME was found")
      return username
    except ValueError as e:
      self.logger.error("Environment variable USERNAME was not found")
      raise e

  def _get_database_password(self, db_pass):
    try:
      if db_pass is not False:
        password: str = db_pass
      else:
        password: str = os.environ["PASSWORD"]
      self.logger.debug("Environment variable PASSWORD was found")
      return password
    except ValueError as e:
      self.logger.error("Environment variable PASSWORD was not found")
      raise e

  def connect(self):
    if isinstance(self.server, SSHTunnelForwarder):
      host = self.server.local_bind_host
      port = self.server.local_bind_port
    else:
      host = self.server["local_bind_host"]
      port = self.server["local_bind_port"]

    db_connector = {
      "database": self.database,
      "user": self.db_user,
      "host": host,
      "port": port,
      "application_name": self.application_name,
    }

    if self.db_pass is not False:
      db_connector["password"] = self.db_pass

    try:
      if self.db_sslmode is not False and self.db_sslrootcert is not False:
        db_connector["sslmode"] = self.db_sslmode
        db_connector["sslrootcert"] = self.db_sslrootcert
        db_connector["sslcert"] = self.db_sslcert
        db_connector["sslkey"] = self.db_sslkey
        conn = psycopg2.connect(**db_connector)
      elif self.db_sslmode is not False and self.db_sslrootcert is False:
        db_connector["sslmode"] = self.db_sslmode
        conn = psycopg2.connect(**db_connector)
      else:
        conn = psycopg2.connect(**db_connector)
      return conn
    except Error as e:
      raise e


def get_db(server, database_config, application_name=False):
  db_name = database_config["db_name"]
  db_user = database_config["db_username"]
  db_identifier = database_config["db_identifier"]
  db_password = False
  db_sslmode = False
  db_sslrootcert = False
  db_sslcert = False
  db_sslkey = False

  if application_name is not False:
    db_identifier = application_name

  if "db_password" in database_config:
    db_password = database_config["db_password"]

  if "db_ssl" in database_config:
    db_ssl = database_config["db_ssl"]
    db_sslmode = db_ssl["db_sslmode"]
    if "db_sslrootcert" in db_ssl:
      db_sslrootcert = db_ssl["db_sslrootcert"]
      db_sslcert = db_ssl["db_sslcert"]
      db_sslkey = db_ssl["db_sslkey"]

  db_loader = DBLoader(
    server,
    db_name,
    db_identifier,
    db_user,
    db_password,
    db_sslmode,
    db_sslrootcert,
    db_sslcert,
    db_sslkey,
  )

  return db_loader
