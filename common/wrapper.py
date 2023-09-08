"""
This wrapper module is used as
the wrapper for partitioning process

It consist of background function and
get_config_n_secret function

background function is used to run the function
in the background using asyncio

get_config_n_secret function is used to get the
config and secret from the config.yaml and secret.yaml
"""

import asyncio
import time


def background(f):
  def wrapped(*args, **kwargs):
    return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

  return wrapped


def get_config_n_secret(func):
  def wrapped(*args, **kwargs):
    self = args[0]

    secret = self.get_secret()
    db = secret["database"]

    n_of_chunks = 3
    for i in range(0, len(db), n_of_chunks):
      for database_config in db[i : i + n_of_chunks]:
        config = self.get_config()
        for table in config["table"]:
          db_identifier = database_config["db_identifier"]

          application_name = (
            f"{db_identifier}:{table['schema']}.{table['name']}"
          )

          func(*args, table, database_config, application_name)
        time.sleep(1)

  return wrapped
