"""
    Tunnel module is used to connect
    to another instance and perform
    the database connection from
    that instance
"""
import io
import os
import socket

import paramiko
from dotenv import load_dotenv
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

from common.common import PartitionCommon

load_dotenv()


class Tunneler(PartitionCommon):
  """
  DBLoader class is used to connect to postgres database
  The connect function is the entry point of the class

  Args:
    [instance]: str <database host>,
    [instance_port]: int <database port>,
    [remote_host]: str = False (optional),
    [remote_port]: str = False (optional),
    [remote_username]: str = False (optional),
    [remote_key]: str = False (optional),
    [remote_password]: str = False (optional),

  Returns:
    SSHTunnelForwarder()
  """

  def __init__(
    self,
    instance: str,
    instance_port: int,
    remote_host: str = False,
    remote_port: str = False,
    remote_username: str = False,
    remote_key: str = False,
    remote_password: str = False,
  ):
    super().__init__()
    self.logger = self.logging_func("DB_Tunneler")
    logs = self._check_logger()
    self.logger.setLevel(self._evaluate_logger(logs))
    self.logger.info(f"Tunnel Initialize: {instance}")
    self.remote_host = self._get_remote_host(remote_host)
    self.remote_port = self._get_remote_port(remote_port)
    self.remote_username = self._get_remote_username(remote_username)
    self.remote_auth, self.remote_auth_type = self._get_pass_or_key(
      remote_key, remote_password
    )
    self.instance = instance
    self.instance_port = instance_port

  def _get_remote_host(self, remote_host):
    try:
      if remote_host is not False:
        host: str = remote_host
      else:
        host: str = os.environ["REMOTE_HOST"]
      self.logger.debug("Environment variable REMOTE_HOST was found")
      return host
    except ValueError as e:
      self.logger.error("Environment variable REMOTE_HOST was not found")
      raise e

  def _get_remote_username(self, remote_username):
    try:
      if remote_username is not False:
        username: str = remote_username
      else:
        username: str = os.environ["REMOTE_USERNAME"]
      self.logger.debug("Environment variable REMOTE_USERNAME was found")
      return username
    except ValueError as e:
      self.logger.error("Environment variable REMOTE_USERNAME was not found")
      raise e

  def _get_remote_port(self, remote_port):
    try:
      if remote_port is not False:
        port: str = remote_port
      else:
        port: str = os.environ["REMOTE_PORT"]
      self.logger.debug("Environment variable REMOTE_PORT was found")
      return int(port)
    except ValueError as e:
      self.logger.error("Environment variable REMOTE_PORT was not found")
      raise e

  def _get_pass_or_key(self, remote_key, remote_password):
    try:
      if remote_password is not False:
        password: str = remote_password
      else:
        password: str = os.environ["REMOTE_PASSWORD"]
      return password, False
    except KeyError:
      self.logger.debug("Password does not exist, checking for key")
      try:
        if remote_key is not False:
          key: str = remote_key
        else:
          key: str = os.environ["REMOTE_KEY"]
        with open(key, "r", encoding="utf-8") as keys:
          ssh_key = keys.read()
          pkey = paramiko.RSAKey.from_private_key(io.StringIO(ssh_key))
          return pkey, True
      except FileNotFoundError as exc:
        self.logger.error("No Password or Key exist")
        raise FileNotFoundError("No Password or Key exist") from exc

  def connect(self):
    try:
      sock = socket.socket()
      sock.bind(("", 0))
      if self.remote_auth_type:
        self.logger.info("Authenticate with Private Key...")
        server = SSHTunnelForwarder(
          (self.remote_host, self.remote_port),
          ssh_username=self.remote_username,
          ssh_pkey=self.remote_auth,
          host_pkey_directories="./",
          allow_agent=False,
          remote_bind_address=(self.instance, self.instance_port),
          local_bind_address=("localhost", sock.getsockname()[1]),
        )
      else:
        self.logger.info("Authenticate with Password...")
        server = SSHTunnelForwarder(
          (self.remote_host, self.remote_port),
          ssh_username=self.remote_username,
          ssh_password=self.remote_auth,
          remote_bind_address=(self.instance, self.instance_port),
          local_bind_address=("localhost", sock.getsockname()[1]),
        )

      return server

    except BaseSSHTunnelForwarderError as e:
      raise e


def get_tunnel(database_config):
  db_port = 5432
  remote_host = False
  remote_port = False
  remote_username = False
  remote_key = False
  remote_password = False
  if "db_host" in database_config:
    db_host = database_config["db_host"]
  else:
    db_host = os.environ["DB_HOST"]
  if "db_port" in database_config:
    db_port = int(database_config["db_port"])

  if "remote_host" in database_config:
    remote_host = database_config["remote_host"]
    remote_port = database_config["remote_port"]
    remote_username = database_config["remote_username"]

  if "remote_key" in database_config:
    remote_key = database_config["remote_key"]

  if "remote_password" in database_config:
    remote_password = database_config["remote_password"]

  local = {
    "local_bind_host": db_host,
    "local_bind_port": db_port,
  }
  if remote_host is False:
    server = local
  else:
    server = Tunneler(
      db_host,
      db_port,
      remote_host,
      remote_port,
      remote_username,
      remote_key,
      remote_password,
    )

    server = server.connect()

    server.start()

  return server
